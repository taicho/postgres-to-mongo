export interface IConverterOptions {
    batchSize?: number;
    postgresConnectionString: string;
    mongoConnectionString: string;
    postgresSSL?: boolean;
}
