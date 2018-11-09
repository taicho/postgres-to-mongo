import * as mongoose from 'mongoose';
import { ColumnConverter } from './columnConverter';
import { TableTranslation } from './tableTranslation';

export interface DocumentGroups {
    [index: string]: { docs: any[], key: any };
}

export interface SchemaOptions {
    jsonSchema: any;
    mode?: 'inclusive' | 'exclusive';
}

export interface ColumnTranslation {
    to?: string;
    converter?: ColumnConverter;
    translator?: Translator;
    index?: boolean | any;
    isVirtual?: boolean;
    schemaOptions?: SchemaOptions;
}

export interface Translator {
    sourceCollection: string;
    sourceIdField?: string;
    desiredField?: string;
    sourceQuery?: (options: TableTranslation, document: any[], keyArray: any[]) => any;
    sourceProjection?: (options: TableTranslation, documents: any, keyArray: any[]) => any;
    processor?: (options: TableTranslation, groups: DocumentGroups, queryResultDocuments: any) => void;
}
