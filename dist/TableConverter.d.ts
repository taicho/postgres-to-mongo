import { IConverterOptions } from './interfaces/IConverterOptions';
import { ITableTranslation } from './interfaces/ITableTranslation';
export declare function defaultValueConverter(schemaType: string, schemaFormat: string, valueString: string): any;
export declare function uuidToObjectIdString(sourceValue: string): string;
export declare class TableConverter {
    generatedSchemas: {
        [index: string]: any;
    };
    private client;
    private mongooseConnection;
    private options;
    constructor(options: IConverterOptions);
    convertTables(...options: ITableTranslation[]): Promise<void>;
    generateSchemas(...options: ITableTranslation[]): Promise<void>;
    private purgeClient();
    private convertTable(options);
    private convertTableInternal(options, prepare);
    private generateSchemasInternal(options, prepare);
    private prepareOptions(options);
    private gatherDepedencies(...translationOptions);
    private getDepedencyDefinition(translationOptions);
    private getDepedencyGraph(graph);
    private log(str);
    private hasColumns(options);
    private applyLegacyId(translationOptions, columns);
    private addMissingColumns(translationOptions, columns);
    private processColumnIndex(translationOptions, columnOptions);
    private resolveJsonPath(str, obj);
    private processDeletes(options, documents);
    private getSchemaForType(metadata);
    private generateSchema(translationOptions, columns);
    private processOptions(translationOptions, columns);
    private processRecords(translationOptions, columns, cursor, totalCount?, count?);
    private getConverter(metadata);
    private queryPostgres<T>(queryString);
    private getAllColumns(schema, tableName);
}
