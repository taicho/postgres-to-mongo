export interface IConversionResult {
    value?: any;
    documentModified?: boolean;
}
export declare type IColumnConverter = (sourceValue: any, sourceType: string, row?: any, document?: any, sourceName?: string, targetName?: string, sourceTypeDetails?: string) => IConversionResult | any;
