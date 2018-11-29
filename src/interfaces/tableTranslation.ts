import { ColumnTranslation } from './columnTranslation';

export interface TableTranslation {
    fromSchema: string;
    fromTable: string;
    toCollection?: string;
    embedIn?: string;
    embedSingle?: boolean;
    embedSourceIdColumn?: string;
    embedTargetIdColumn?: string;
    columns?: { [index: string]: ColumnTranslation };
    dynamicColumns?: { [index: string]: { jsonSchema?: any, value: (options: TableTranslation, document: any) => any } };
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
    onCreated?: (options: TableTranslation, document: any) => void;
    ignoreDependencies?: boolean;
    indexes?: { descriptor: any, options?: any }[];
    deleteFields?: string[];
    postProcess?: (options: TableTranslation, documents: any[]) => Promise<void> | void;
    customWhere?: string;
    onPersist?: (options: TableTranslation, documents: any[]) => Promise<void> | void;
}
