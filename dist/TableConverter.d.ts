import { IConverterOptions } from './interfaces/IConverterOptions';
import { ITableTranslation } from './interfaces/ITableTranslation';
export declare function uuidToObjectIdString(sourceValue: string): string;
export declare class TableConverter {
    private client;
    private mongooseConnection;
    private options;
    constructor(options: IConverterOptions);
    convertTables(...options: ITableTranslation[]): Promise<void>;
    private purgeClient();
    private convertTable(options);
    private convertTableInternal(options, prepare);
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
    private processRecords(translationOptions, columns, cursor, totalCount?, count?);
    private getConverter(typeName, udtName);
    private queryPostgres<T>(queryString);
    private getAllColumns(schema, tableName);
}
