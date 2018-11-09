export interface ConverterOptions {
    batchSize?: number;
    postgresConnectionString: string;
    mongoConnectionString: string;
    postgresSSL?: boolean;
    baseCollectionSchema?: {
        [index: string]: any;
    };
    schemaDefaultValueConverter?: (schemaType: string, schemaFormat: string, valueString: string) => any;
    useMetadataCache?: boolean;
    createMetadataCache?: boolean;
    cacheDirectory?: string;
}
