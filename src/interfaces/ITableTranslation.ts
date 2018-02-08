import { IColumnTranslation } from './IColumnTranslation';

export interface ITableTranslation {
    fromSchema: string;
    fromTable: string;
    toCollection?: string;
    embedIn?: string;
    embedSingle?: boolean;
    embedSourceIdColumn?: string;
    embedTargetIdColumn?: string;
    columns?: { [index: string]: IColumnTranslation };
    dynamicColumns?: { [index: string]: { jsonSchema?: any, value: (options: ITableTranslation, document: any) => any } };
    preserveEmbedSourceId?: boolean;
    mongifyColumnNames?: boolean;
    mongifyTableName?: boolean;
    includeMissingColumns?: boolean;
    includeTimestamps?: boolean;
    includeVersion?: boolean;
    includeId?: boolean;
    embedArrayField?: string;
    autoLegacyId?: boolean;
    legacyIdDestinationName?: string;
    filter?: (document: any) => boolean;
    addedDependencies?: string[];
    onCreated?: (options: ITableTranslation, document: any) => void;
    ignoreDependencies?: boolean;
    indexes?: { descriptor: any, options?: any }[];
    deleteFields?: string[];
    postProcess?: (options: ITableTranslation, documents: any[]) => Promise<void> | void;
    customWhere?: string;
}
