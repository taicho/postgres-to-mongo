import * as mongoose from 'mongoose';
import { IColumnConverter } from './IColumnConverter';
import { ITableTranslation } from './ITableTranslation';

export interface IDocumentGroups {
    [index: string]: { docs: any[], key: any };
}

export interface IColumnTranslation {
    to?: string;
    converter?: IColumnConverter;
    translator?: ITranslator;
    index?: boolean | any;
    isVirtual?: boolean;
}

export interface ITranslator {
    sourceCollection: string;
    sourceIdField?: string;
    desiredField?: string;
    sourceQuery?: (options: ITableTranslation, document: any[], keyArray: any[]) => any;
    sourceProjection?: (options: ITableTranslation, documents: any, keyArray: any[]) => any;
    processor?: (options: ITableTranslation, groups: IDocumentGroups, queryResultDocuments: any) => void;
}
