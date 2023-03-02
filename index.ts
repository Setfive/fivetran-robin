import { Context, APIGatewayProxyResult, APIGatewayEvent } from 'aws-lambda';
import * as AWS from 'aws-sdk';
import moment from 'moment';
import axios from 'axios';

const s3 = new AWS.S3();

interface IFivetranRequest {
    state: {cursor: string | undefined};
    secrets: {apiKey: string, organizationId: string, startDate?: string};
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

interface IRobinAnalyticsResult {
    meta: {
        status_code: number,
        status: 'ACCEPTED' | 'BAD_REQUEST' | 'NOT_FOUND',
        message: string,
        more_info: unknown,
        errors: string[]
    },
    data: {
        export_id: string,
        report_id: string,
    }
}

interface IDoFetchResult {
    success: boolean;
    error?: string;
    data?: string;
}

async function doFetchRecords(request: IFivetranRequest): Promise<IDoFetchResult> {
    let startDate = '';
    let endDate = '';
    let cursorDate: moment.Moment;
    const now = moment();

    if(request.state.cursor) {
        cursorDate = moment(request.state.cursor).startOf('day');
        startDate = request.state.cursor;
    }else{
        if(request.secrets.startDate) {
            cursorDate = moment(request.secrets.startDate).startOf('day');
        }else{
            cursorDate = moment('2008-01-01T00:00:00Z');
        }

        startDate = cursorDate.format();
    }

    if(now.diff(cursorDate, 'days') > 185) {
        const cursorEndDate = cursorDate.add(185, 'day').endOf('day');
        endDate = cursorEndDate.format();
    }else{
        endDate = now.format();
    }

    console.log(`startDate = ${startDate}, endDate = ${endDate}`);

    const url = `https://api.robinpowered.com/v1.0/insights/exports/organizations/${request.secrets.organizationId}/spaces`;
    const payload = {from: startDate, to: endDate};
    const apiResult = await axios.post<IRobinAnalyticsResult>(url, payload, {headers: {Authorization: `Access-Token ${request.secrets.apiKey}`, 'Content-Type': 'application/json'}});

    if(apiResult.data.meta.status !== 'ACCEPTED') {
        return {
            success: false,
            error: `${apiResult.data.meta.status}: ${apiResult.data.meta.message}`
        };
    }

    const exportId = apiResult.data.data.export_id;
    let sleepTime = 1;
    let numAttempts = 0;
    let data = '';

    do {
        await sleep(sleepTime);
        const url = `https://api.robinpowered.com/v1.0/insights/exports/${exportId}`;
        const apiResult = await axios.get<IRobinAnalyticsResult>(url, {headers: {Authorization: `Access-Token ${request.secrets.apiKey}`, 'Content-Type': 'application/json'}});

        sleepTime = sleepTime * 2;
        numAttempts += 1;
    }while(numAttempts < 10);

}

async function sleep(sleepTimeMs: number) {
    return new Promise<void>((resolve, reject) => setTimeout(resolve, sleepTimeMs * 1000))
}

doFetchRecords({
    "state": {"cursor": "2020-01-01T00:00:00Z"},
    "secrets": {apiKey: `${process.env.ROBIN_KEY}`, organizationId: '34', startDate: "2020-01-01T00:00:00Z"},
    "bucket": "setfive-robin-sync",
    "file": "test2.csv",
})

export const handler = async (request: IFivetranRequest, context: Context): Promise<IFivetranResult | IFivetranError> => {

    // TODO: Verify the required secrets are present and error if not

    console.log(`Cursor: ${request.state.cursor}, Destination: s3://${request.bucket}/${request.file}`);

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

    console.log(`Received: Inserts = ${fetchResult.s3Data.insert.transactions.length}, Deletes =  ${fetchResult.s3Data.delete.transactions.length}`);

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
