"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.handler = void 0;
const AWS = __importStar(require("aws-sdk"));
const moment_1 = __importDefault(require("moment"));
const axios_1 = __importDefault(require("axios"));
const sync_1 = require("csv-parse/sync");
const s3 = new AWS.S3();
const TARGET_TABLE_NAME = 'robin_analytics';
const handler = async (request, context) => {
    const missingSecrets = [];
    if (!request.secrets.apiKey) {
        missingSecrets.push('apiKey');
    }
    if (!request.secrets.organizationId) {
        missingSecrets.push('organizationId');
    }
    if (missingSecrets.length) {
        return {
            errorMessage: `Your Fivetran configuration is missing these secrets: ${missingSecrets.join(', ')}`,
            errorType: 'SecretsError',
            stackTrace: new Error().stack,
        };
    }
    console.log(`Cursor: ${request.state.transactionsCursor}, Destination: s3://${request.bucket}/${request.file}`);
    let fetchResult;
    try {
        fetchResult = await fetchRecords(request);
    }
    catch (e) {
        const msg = e instanceof Error ? e.message : 'NONE';
        return {
            errorMessage: msg,
            errorType: 'FetchRecordsError',
            stackTrace: new Error().stack,
        };
    }
    console.log(`Received: Inserts = ${fetchResult.s3Data.insert.length}, Deletes =  ${fetchResult.s3Data.delete.length}`);
    const s3DataFile = {
        insert: { [TARGET_TABLE_NAME]: fetchResult.s3Data.insert },
        delete: { [TARGET_TABLE_NAME]: fetchResult.s3Data.delete }
    };
    const s3Result = await copyToS3(request.bucket, request.file, JSON.stringify(s3DataFile));
    if (!s3Result.success) {
        return {
            errorMessage: `${s3Result.error}`,
            errorType: 'CopyToS3Error',
            stackTrace: new Error().stack,
        };
    }
    const hasMore = fetchResult.nextCursor !== request.state.transactionsCursor;
    const result = {
        state: {
            transactionsCursor: fetchResult.nextCursor
        },
        schema: {
            [TARGET_TABLE_NAME]: {
                primary_key: ['Event ID']
            }
        },
        hasMore: hasMore
    };
    console.log(JSON.stringify(result, null, 4));
    return result;
};
exports.handler = handler;
async function doFetchRecords(request) {
    let startDate = '';
    let endDate = '';
    let cursorDate;
    const now = (0, moment_1.default)();
    if (request.state.transactionsCursor) {
        cursorDate = (0, moment_1.default)(request.state.transactionsCursor).startOf('day');
        startDate = request.state.transactionsCursor;
    }
    else {
        if (request.secrets.startDate) {
            cursorDate = (0, moment_1.default)(request.secrets.startDate).startOf('day');
        }
        else {
            // TODO: What is the first date we should try?
            cursorDate = (0, moment_1.default)('2008-01-01T00:00:00Z');
        }
        startDate = cursorDate.format();
    }
    if (now.diff(cursorDate, 'days') > 15) {
        const cursorEndDate = cursorDate.add(15, 'day').endOf('day');
        endDate = cursorEndDate.format();
    }
    else {
        endDate = now.format();
    }
    console.log(`startDate = ${startDate}, endDate = ${endDate}`);
    const url = `https://api.robinpowered.com/v1.0/insights/exports/organizations/${request.secrets.organizationId}/spaces`;
    const payload = { from: startDate, to: endDate };
    let createExportResult;
    try {
        const apiResult = await axios_1.default.post(url, payload, {
            headers: {
                Authorization: `Access-Token ${request.secrets.apiKey}`,
                'Content-Type': 'application/json'
            }
        });
        createExportResult = apiResult.data;
    }
    catch (e) {
        if (axios_1.default.isAxiosError(e)) {
            const axiosError = e;
            return {
                success: false,
                error: `Robin API Error: HTTP ${axiosError.code} ${JSON.stringify(axiosError.response?.data)}`
            };
        }
        else {
            return {
                success: false,
                error: `${JSON.stringify(e)}`
            };
        }
    }
    console.log(JSON.stringify(createExportResult, null, 4));
    if (createExportResult.meta.status !== 'ACCEPTED') {
        return {
            success: false,
            error: `${createExportResult.meta.status}: ${createExportResult.meta.message}`
        };
    }
    const exportId = createExportResult.data.export_id;
    let sleepTime = 10;
    let numAttempts = 0;
    do {
        await sleep(sleepTime);
        const url = `https://api.robinpowered.com/v1.0/insights/exports/${exportId}`;
        console.log(`Trying: ${url}`);
        try {
            const apiResult = await axios_1.default.get(url, {
                headers: {
                    Authorization: `Access-Token ${request.secrets.apiKey}`,
                    'Content-Type': 'application/json'
                }
            });
            const records = (0, sync_1.parse)(apiResult.data, { columns: true });
            return {
                success: true,
                data: records
            };
        }
        catch (e) {
            if (axios_1.default.isAxiosError(e)) {
                const axiosError = e;
                if (axiosError.code !== 'ERR_BAD_REQUEST') {
                    return {
                        success: false,
                        error: `Robin API Error: HTTP (${axiosError.status}) ${axiosError.code} ${JSON.stringify(axiosError.response?.data)}`
                    };
                }
            }
            else {
                return {
                    success: false,
                    error: `${JSON.stringify(e)}`
                };
            }
        }
        sleepTime = Math.ceil(sleepTime * 1.5);
        numAttempts += 1;
    } while (numAttempts < 5);
    return {
        success: false,
        error: `Robin API Error: Tried 5 times and gave up on ${url}`
    };
}
async function sleep(sleepTimeMs) {
    return new Promise((resolve, reject) => setTimeout(resolve, sleepTimeMs * 1000));
}
async function copyToS3(bucket, key, data) {
    return new Promise((resolve, reject) => {
        const params = {
            Bucket: bucket,
            Key: key,
            Body: data
        };
        s3.putObject(params, (err, data) => {
            if (err) {
                resolve({ success: false, error: err.message });
            }
            else {
                resolve({ success: true });
            }
        });
    });
}
async function fetchRecords(request) {
    const result = await doFetchRecords(request);
    if (!result.success) {
        throw new Error(result.error);
    }
    const data = result.data ?? [];
    // TODO: Is this the correct date field to use to move the "cursor" forward?
    const createdAtDates = data
        .map(item => (0, moment_1.default)(item['Created At (UTC)'], 'YYYY-MM-DD hh:mm:ss'))
        .sort((a, b) => a.diff(b));
    let nextCursor = '';
    if (createdAtDates.length) {
        let useLastCreatedAt = false;
        if (request.state.transactionsCursor) {
            const transactionsCursorMoment = (0, moment_1.default)(request.state.transactionsCursor);
            useLastCreatedAt = createdAtDates[createdAtDates.length - 1].isAfter(transactionsCursorMoment);
        }
        if (useLastCreatedAt) {
            nextCursor = createdAtDates[createdAtDates.length - 1].format();
        }
    }
    else {
        nextCursor = `${request.state.transactionsCursor}`;
    }
    return {
        nextCursor: `${nextCursor}`,
        s3Data: {
            delete: [],
            insert: data
        }
    };
}
