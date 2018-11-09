export interface ConversionResult {
    value?: any;
    documentModified?: boolean;
}
export declare type ColumnConverter = (sourceValue: any, sourceType: string, row?: any, document?: any, sourceName?: string, targetName?: string, sourceTypeDetails?: string) => ConversionResult | any;
