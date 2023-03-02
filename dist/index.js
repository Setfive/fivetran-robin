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
Object.defineProperty(exports, "__esModule", { value: true });
exports.handler = void 0;
const AWS = __importStar(require("aws-sdk"));
const s3 = new AWS.S3();
const handler = async (request, context) => {
    let fetchResult;
    try {
        fetchResult = await fetchRecords(request);
    }
    catch (e) {
        console.error(e);
        return {
            errorMessage: JSON.stringify(e),
            errorType: 'FetchRecordsError',
            stackTrace: new Error().stack,
        };
    }
    const s3Result = await copyToS3(request.bucket, request.file, JSON.stringify(fetchResult.s3Data));
    if (!s3Result.success) {
        return {
            errorMessage: `${s3Result.error}`,
            errorType: 'CopyToS3Error',
            stackTrace: new Error().stack,
        };
    }
    return {
        state: {
            transactionsCursor: fetchResult.nextCursor
        },
        schema: {
            transactions: {
                primary_key: ['Event ID']
            }
        },
        hasMore: false
    };
};
exports.handler = handler;
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
    return {
        nextCursor: '2018-01-01T00:00:00Z',
        s3Data: {
            delete: {
                transactions: [],
            },
            insert: {
                transactions: [
                    {
                        "Organization": "Robin",
                        "Building": "87 Summer Street",
                        "Floor": "",
                        "Space": "Conference Room",
                        "Space ID": 23253,
                        "Event ID": "207480119_20230101T043000Z",
                        "Event Status": "CONFIRMED",
                        "Created At (UTC)": "2022-09-18 04:08:41",
                        "Updated At (UTC)": "2022-09-18 04:08:41",
                        "All Day Event": "FALSE",
                        "Title": "Test Recurring",
                        "Is Recurring": "TRUE",
                        "Capacity": 8,
                        "Invited People": 1,
                        "Attendees": 1,
                        "Location": "Nicholas' Cage",
                        "Started At (UTC)": "2023-01-01 04:30:00",
                        "Ended At (UTC)": "2023-01-01 04:35:00",
                        "Minute Duration": 5,
                        "Checked In At (UTC)": "",
                        "Minutes Delayed": "",
                        "Automatically Unbooked At (UTC)": "",
                        "Creator Robin ID": "",
                        "Creator Robin Name": "",
                        "Creator Department": "",
                        "Creator Groups": "",
                        "Creator Robin Email": "",
                        "Created By Email": "conference-room@robinpowered.onmicrosoft.com",
                        "Host Robin ID": "",
                        "Host Robin Name": "",
                        "Host Department": "",
                        "Host Groups": "",
                        "Host Robin Email": "conference-room@robinpowered.onmicrosoft.com",
                        "Hosting Calendar Email": "conference-room@robinpowered.onmicrosoft.com",
                        "Local Creator ID": "",
                        "Local Creator Email": "",
                        "Cancellation Type": ""
                    },
                    {
                        "Organization": "Robin",
                        "Building": "87 Summer Street",
                        "Floor": "",
                        "Space": "Conference Room",
                        "Space ID": 23253,
                        "Event ID": "207480121_20230101T180000Z",
                        "Event Status": "CONFIRMED",
                        "Created At (UTC)": "2022-09-18 04:08:41",
                        "Updated At (UTC)": "2022-09-18 04:08:41",
                        "All Day Event": "FALSE",
                        "Title": "Zach Dunn in Nicholas' Cage",
                        "Is Recurring": "TRUE",
                        "Capacity": 8,
                        "Invited People": 1,
                        "Attendees": 1,
                        "Location": "Nicholas' Cage",
                        "Started At (UTC)": "2023-01-01 18:00:00",
                        "Ended At (UTC)": "2023-01-01 18:30:00",
                        "Minute Duration": 30,
                        "Checked In At (UTC)": "",
                        "Minutes Delayed": "",
                        "Automatically Unbooked At (UTC)": "",
                        "Creator Robin ID": "",
                        "Creator Robin Name": "",
                        "Creator Department": "",
                        "Creator Groups": "",
                        "Creator Robin Email": "",
                        "Created By Email": "conference-room@robinpowered.onmicrosoft.com",
                        "Host Robin ID": "",
                        "Host Robin Name": "",
                        "Host Department": "",
                        "Host Groups": "",
                        "Host Robin Email": "conference-room@robinpowered.onmicrosoft.com",
                        "Hosting Calendar Email": "conference-room@robinpowered.onmicrosoft.com",
                        "Local Creator ID": "",
                        "Local Creator Email": "",
                        "Cancellation Type": ""
                    }
                ],
            }
        }
    };
}
//# sourceMappingURL=index.js.map