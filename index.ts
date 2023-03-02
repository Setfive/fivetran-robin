import { Context, APIGatewayProxyResult, APIGatewayEvent } from 'aws-lambda';
import * as AWS from 'aws-sdk';

const s3 = new AWS.S3();

interface IFivetranRequest {
    state: {cursor: string};
    secrets: {apiKey: string};
    bucket: string;
    file: string;
}

// NOTE: The destination table will be named "transactions"
interface IFivetranResult {
    state: {
        transactionsCursor: string;
    };
    schema: {
        transactions: {
            primary_key: ['Event ID']
        }
    },
    hasMore: false
}

interface IFivetranError {
    errorMessage: string;
    errorType: string;
    stackTrace: any;
}

interface IFetchResponse {
    s3Data: {
        insert: {
            transactions: unknown[]
        },
        delete: {
            transactions: unknown[]
        }
    },
    nextCursor: string;
}

interface IS3CopyResult {
    success: boolean;
    error?: string;
}

export const handler = async (request: IFivetranRequest, context: Context): Promise<IFivetranResult | IFivetranError> => {

    let fetchResult: IFetchResponse;

    try {
        fetchResult = await fetchRecords(request);
    }catch(e) {
        console.error(e);
        return {
            errorMessage: JSON.stringify(e),
            errorType: 'FetchRecordsError',
            stackTrace: new Error().stack,
        }
    }

    const s3Result = await copyToS3(request.bucket, request.file, JSON.stringify(fetchResult.s3Data));

    if(!s3Result.success) {
        return {
            errorMessage: `${s3Result.error}`,
            errorType: 'CopyToS3Error',
            stackTrace: new Error().stack,
        }
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
    }
}
