import * as pg from 'pg';
export interface IConversionResult {
    value?: any;
    documentModified?: boolean;
}
export type IColumnConverter = (sourceValue: any, sourceType: string, row?: any, document?: any, sourceName?: string, targetName?: string, sourceTypeDetails?: string) => IConversionResult | any;
