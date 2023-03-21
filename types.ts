export interface IFivetranRequest {
  state: { transactionsCursor: string | undefined };
  secrets: { apiKey: string; organizationId: string; startDate?: string };
  bucket: string;
  file: string;
}

export interface IFivetranResult {
  state: {
    transactionsCursor: string;
  };
  schema: IFivetranSchema;
  hasMore: boolean;
}

export interface IFivetranSchemaDef {
  primary_key: string[];
}

export interface IFivetranSchema extends Record<string, IFivetranSchemaDef> {}

export interface IFivetranError {
  errorMessage: string;
  errorType: string;
  stackTrace: any;
}

export interface IFetchResponse {
  s3Data: {
    insert: unknown[];
    delete: unknown[];
  };
  nextCursor: string;
}

export interface IS3CopyResult {
  success: boolean;
  error?: string;
}

export interface IRobinAnalyticsResult {
  meta: {
    status_code: number;
    status: 'ACCEPTED' | 'BAD_REQUEST' | 'NOT_FOUND';
    message: string;
    more_info: unknown;
    errors: string[];
  };
  data: {
    export_id: string;
    report_id: string;
  };
}

export interface IDoFetchResult {
  success: boolean;
  error?: string;
  data?: Record<string, string>[];
}
