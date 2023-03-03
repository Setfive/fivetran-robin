import { Context } from 'aws-lambda';
import * as AWS from 'aws-sdk';
import moment from 'moment';
import axios, { AxiosError } from 'axios';
import { parse } from 'csv-parse/sync';

const s3 = new AWS.S3();

const TARGET_TABLE_NAME = 'robin_analytics';

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
    hasMore: boolean
}

interface IFivetranError {
    errorMessage: string;
    errorType: string;
    stackTrace: any;
}

interface IFetchResponse {
    s3Data: {
        insert: unknown[]
        delete: unknown[]
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
    data?: Record<string, string>[];
}

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

    console.log(`Cursor: ${request.state.cursor}, Destination: s3://${request.bucket}/${request.file}`);

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

    const hasMore = fetchResult.nextCursor !== request.state.cursor;

    return {
        state: {
            transactionsCursor: fetchResult.nextCursor
        },
        schema: {
            transactions: {
                primary_key: ['Event ID']
            }
        },
        hasMore: hasMore
    };
};

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

    const nextCursor = createdAtDates.length ? createdAtDates[ createdAtDates.length - 1 ].format() : request.state.cursor;

    return {
        nextCursor: `${nextCursor}`,
        s3Data: {
            delete: [],
            insert: data
        }
    }
}
