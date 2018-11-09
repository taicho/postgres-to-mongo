
export interface TranslatorDefinition {
    collection: string;
    table: string;
    schema: string;
    columnMappings: { [index: string]: string };
}
