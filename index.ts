import { Context } from 'aws-lambda';
import * as AWS from 'aws-sdk';
import moment from 'moment';
import axios, { AxiosError } from 'axios';
import { parse } from 'csv-parse/sync';
import {
    IDoFetchResult,
    IFetchResponse,
    IFivetranError,
    IFivetranRequest,
    IFivetranResult,
    IRobinAnalyticsResult, IS3CopyResult
} from "./types";

const s3 = new AWS.S3();

const TARGET_TABLE_NAME = 'robin_analytics';



export const handler = async (request: IFivetranRequest, context: Context): Promise<IFivetranResult | IFivetranError> => {
    const missingSecrets: string[] = [];

    if(!request.secrets.apiKey) {
        missingSecrets.push('apiKey');
    }

    if(!request.secrets.organizationId) {
        missingSecrets.push('organizationId');
    }

    if(missingSecrets.length) {
        return {
            errorMessage: `Your Fivetran configuration is missing these secrets: ${missingSecrets.join(', ')}`,
            errorType: 'SecretsError',
            stackTrace: new Error().stack,
        }
    }

    console.log(`Cursor: ${request.state.transactionsCursor}, Destination: s3://${request.bucket}/${request.file}`);

    let fetchResult: IFetchResponse;

    try {
        fetchResult = await fetchRecords(request);
    }catch(e) {
        const msg = e instanceof Error ? e.message : 'NONE';
        return {
            errorMessage: msg,
            errorType: 'FetchRecordsError',
            stackTrace: new Error().stack,
        }
    }

    console.log(`Received: Inserts = ${fetchResult.s3Data.insert.length}, Deletes =  ${fetchResult.s3Data.delete.length}`);

    const s3DataFile = {
        insert: {[TARGET_TABLE_NAME]: fetchResult.s3Data.insert},
        delete: {[TARGET_TABLE_NAME]: fetchResult.s3Data.delete}
    };

    const s3Result = await copyToS3(request.bucket, request.file, JSON.stringify(s3DataFile));

    if(!s3Result.success) {
        return {
            errorMessage: `${s3Result.error}`,
            errorType: 'CopyToS3Error',
            stackTrace: new Error().stack,
        }
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

async function doFetchRecords(request: IFivetranRequest): Promise<IDoFetchResult> {
    let startDate = '';
    let endDate = '';
    let cursorDate: moment.Moment;
    const now = moment();

    if(request.state.transactionsCursor) {
        cursorDate = moment(request.state.transactionsCursor).startOf('day');
        startDate = request.state.transactionsCursor;
    }else{
        if(request.secrets.startDate) {
            cursorDate = moment(request.secrets.startDate).startOf('day');
        }else{
            // TODO: What is the first date we should try?
            cursorDate = moment('2008-01-01T00:00:00Z');
        }

        startDate = cursorDate.format();
    }

    if(now.diff(cursorDate, 'days') > 15) {
        const cursorEndDate = cursorDate.add(15, 'day').endOf('day');
        endDate = cursorEndDate.format();
    }else{
        endDate = now.format();
    }

    console.log(`startDate = ${startDate}, endDate = ${endDate}`);

    const url = `https://api.robinpowered.com/v1.0/insights/exports/organizations/${request.secrets.organizationId}/spaces`;
    const payload = {from: startDate, to: endDate};
    let createExportResult: IRobinAnalyticsResult;

    try {
        const apiResult = await axios.post<IRobinAnalyticsResult>(url, payload, {
            headers: {
                Authorization: `Access-Token ${request.secrets.apiKey}`,
                'Content-Type': 'application/json'
            }
        });
        createExportResult = apiResult.data;
    }catch(e) {
        if(axios.isAxiosError(e)) {
            const axiosError = (e as AxiosError);
            return {
                success: false,
                error: `Robin API Error: HTTP ${axiosError.code} ${JSON.stringify(axiosError.response?.data)}`
            };
        }else{
            return {
                success: false,
                error: `${JSON.stringify(e)}`
            };
        }
    }

    console.log(JSON.stringify(createExportResult, null, 4));

    if(createExportResult.meta.status !== 'ACCEPTED') {
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
            const apiResult = await axios.get<string>(url, {
                headers: {
                    Authorization: `Access-Token ${request.secrets.apiKey}`,
                    'Content-Type': 'application/json'
                }
            });

            const records = parse(apiResult.data, {columns: true});

            return {
                success: true,
                data: records
            };
        }catch(e){

            if(axios.isAxiosError(e)) {
                const axiosError = (e as AxiosError);
                if(axiosError.code !== 'ERR_BAD_REQUEST') {
                    return {
                        success: false,
                        error: `Robin API Error: HTTP (${axiosError.status}) ${axiosError.code} ${JSON.stringify(axiosError.response?.data)}`
                    };
                }
            }else{
                return {
                    success: false,
                    error: `${JSON.stringify(e)}`
                };
            }
        }

        sleepTime = Math.ceil(sleepTime * 1.5);
        numAttempts += 1;
    }while(numAttempts < 5);

    return {
        success: false,
        error: `Robin API Error: Tried 5 times and gave up on ${url}`
    };
}

async function sleep(sleepTimeMs: number) {
    return new Promise<void>((resolve, reject) => setTimeout(resolve, sleepTimeMs * 1000))
}

async function copyToS3(bucket: string, key: string, data: string): Promise<IS3CopyResult> {
    return new Promise<IS3CopyResult>((resolve, reject) => {
        const params = {
            Bucket: bucket,
            Key: key,
            Body: data
        };

        s3.putObject(params,  (err: AWS.AWSError, data) => {
            if (err){
                resolve({success: false, error: err.message});
            }else{
                resolve({success: true});
            }
        });
    });
}

async function fetchRecords(request: IFivetranRequest): Promise<IFetchResponse> {
    const result = await doFetchRecords(request);
    if(!result.success) {
        throw new Error(result.error);
    }

    const data = result.data ?? [];
    // TODO: Is this the correct date field to use to move the "cursor" forward?
    const createdAtDates = data
                          .map(item => moment(item['Created At (UTC)'], 'YYYY-MM-DD hh:mm:ss'))
                          .sort((a, b) => a.diff(b));

    const nextCursor = createdAtDates.length ? createdAtDates[ createdAtDates.length - 1 ].format() : request.state.transactionsCursor;

    return {
        nextCursor: `${nextCursor}`,
        s3Data: {
            delete: [],
            insert: data
        }
    }
}
