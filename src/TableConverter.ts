import * as debug from 'debug';
import * as deepmerge from 'deepmerge';
import * as fs from 'fs';
import * as mongoose from 'mongoose';
import { mongo } from 'mongoose';
import * as path from 'path';
import * as pg from 'pg';
import * as Cursor from 'pg-cursor';
import { start } from 'repl';
import { ColumnConverter } from 'src/interfaces/columnConverter';
import { ColumnTranslation } from 'src/interfaces/columnTranslation';
import { callbackify } from 'util';
import { toMongoName } from './Common';
import * as Database from './Database';
import { ConverterOptions } from './interfaces/converterOptions';
import { PostgresColumnInfo } from './interfaces/postgresColumnInfo';
import { TableTranslation } from './interfaces/tableTranslation';
import { TableTranslationInternal } from './interfaces/tableTranslationInternal';
import { TranslatorDefinition } from './interfaces/translatorDefinition';
const log = debug('postgres-to-mongo:TableConverter');

const defaultOptions: Partial<ConverterOptions> = {
    batchSize: 5000,
    postgresSSL: false,
    schemaDefaultValueConverter: defaultValueConverter,
    cacheDirectory: './ptmTemp',
};

const defaultTableTranslationOptions: Partial<TableTranslation> = {
    includeTimestamps: true,
    includeVersion: true,
    includeId: true,
    legacyIdDestinationName: '_legacyId',
};

export function defaultValueConverter(schemaType: string, schemaFormat: string, valueString: string): any {
    if (!valueString) {
        return null;
    }
    if (schemaType === 'boolean') {
        return valueString.toLowerCase() === 'true' ? true : false;
    }
    if (valueString.includes('::')) {
        const extractedValue = valueString.match(/(.*)::/)[1];
        if (schemaType === 'string') {
            return extractedValue;
        } else if (schemaType === 'number') {
            return parseFloat(extractedValue);
        }
    } else if (schemaType === 'number') {
        return parseFloat(valueString);
    } else if (schemaType === 'string') {
        if (schemaFormat === 'date') {
            return 'Date.now';
        } else if (valueString === 'uuid_generate_v4()') {
            return 'mongoose.Types.ObjectId';
        } else {
            return valueString;
        }
    } else {
        return valueString;
    }
}

export function uuidToObjectIdString(sourceValue: string): string {
    return (sourceValue).replace(/-/g, '').substring(0, 24);
}

export class TableConverter {
    public generatedSchemas: { [index: string]: any };
    private client: pg.Client;
    private mongooseConnection: mongoose.Connection;
    private options: ConverterOptions;

    constructor(options: ConverterOptions) {
        this.options = Object.assign({}, defaultOptions, options);
        this.generatedSchemas = options.baseCollectionSchema || {};
    }

    public async convertTables(...options: TableTranslation[]) {
        options = options.map((o) => this.prepareOptions(o));
        const dependencies = this.gatherDepedencies(...options as TableTranslationInternal[]);
        for (const tableOptions of dependencies.results) {
            try {
                await this.convertTableInternal(tableOptions, false);
            } catch (err) {
                this.log(err);
                if (this.client) {
                    this.client.release();
                    this.client = null;
                }
            }
        }
        this.purgeClient();
    }

    public async createTranslatorDefinitions(...options: TableTranslation[]): Promise<TranslatorDefinition[]> {
        const translatorDefinitions: TranslatorDefinition[] = [];
        options = options.map((o) => this.prepareOptions(o));
        const dependencies = this.gatherDepedencies(...options as TableTranslationInternal[]);
        for (const tableOptions of dependencies.results) {
            try {
                const definition = await this.createTranslatorDefinition(tableOptions, false);
                if (definition) {
                    translatorDefinitions.push(definition);
                }
            } catch (err) {
                this.log(err);
                if (this.client) {
                    this.client.release();
                    this.client = null;
                }
            }
        }
        this.purgeClient();
        return translatorDefinitions;
    }

    public async generateSchemas(...options: TableTranslation[]) {
        options = options.map((o) => this.prepareOptions(o));
        const dependencies = this.gatherDepedencies(...options as TableTranslationInternal[]);
        for (const tableOptions of dependencies.results) {
            try {
                await this.generateSchemasInternal(tableOptions, false);
            } catch (err) {
                this.log(err);
                if (this.client) {
                    this.client.release();
                    this.client = null;
                }
            }
        }
        this.purgeClient();
    }

    private purgeClient() {
        if (this.client) {
            this.client.release();
            this.client = null;
        }
    }

    private async createTranslatorDefinition(options: TableTranslationInternal, prepare: boolean): Promise<TranslatorDefinition> {
        if (!options.fromSchema) {
            throw new Error('options.fromSchema must be defined.');
        }
        if (!options.fromTable) {
            throw new Error('options.fromTable must be defined.');
        }
        if (prepare) {
            options = this.prepareOptions(options);
        }
        if (!this.client) {
            this.log('Connecting to databases.');
            this.client = await Database.getPostgresConnection(this.options.postgresConnectionString, this.options.postgresSSL);
            this.mongooseConnection = await Database.getMongoConnection(this.options.mongoConnectionString);
            this.log('Connections established.');
        } else {
            this.client.release();
            this.client = await Database.getPostgresConnection(this.options.postgresConnectionString, this.options.postgresSSL);
        }
        this.log(`Processing data for ${options.fromTable} => ${options.toCollection}.`);
        const columns = await this.getAllColumns(options.fromSchema, options.fromTable);
        this.log('Metadata Fetched.');
        this.processOptions(options, columns);
        const columnMappings: any = {};
        for (const columnKey of Object.keys(options.columns)) {
            const info = options.columns[columnKey];
            const postgresName = columnKey;
            const mongoName = info.to;
            columnMappings[mongoName] = postgresName;
        }
        const translatorDefinition: TranslatorDefinition = {
            collection: options.toCollection,
            table: options.fromTable,
            schema: options.fromSchema,
            columnMappings,
        };
        return translatorDefinition;
    }

    private async convertTableInternal(options: TableTranslationInternal, prepare: boolean) {
        if (!options.fromSchema) {
            throw new Error('options.fromSchema must be defined.');
        }
        if (!options.fromTable) {
            throw new Error('options.fromTable must be defined.');
        }
        if (prepare) {
            options = this.prepareOptions(options);
        }
        if (!this.client) {
            this.log('Connecting to databases.');
            this.client = await Database.getPostgresConnection(this.options.postgresConnectionString, this.options.postgresSSL);
            this.mongooseConnection = await Database.getMongoConnection(this.options.mongoConnectionString);
            this.log('Connections established.');
        } else {
            this.client.release();
            this.client = await Database.getPostgresConnection(this.options.postgresConnectionString, this.options.postgresSSL);
        }
        this.log(`Processing records for ${options.fromTable} => ${options.toCollection}.`);
        this.log('Metadata Fetched.');
        const columns = await this.getAllColumns(options.fromSchema, options.fromTable);
        this.log('Columns Fetched.');
        // if (options.fromSchema === 'project_02142917-c55e-4486-b8ac-02e274ffe1b0') {
        //     debugger;
        // }
        await this.processRecords(options, columns, null);
    }

    private async generateSchemasInternal(options: TableTranslationInternal, prepare: boolean) {
        if (!options.fromSchema) {
            throw new Error('options.fromSchema must be defined.');
        }
        if (!options.fromTable) {
            throw new Error('options.fromTable must be defined.');
        }
        if (prepare) {
            options = this.prepareOptions(options);
        }
        if (!this.client) {
            this.log('Connecting to databases.');
            this.client = await Database.getPostgresConnection(this.options.postgresConnectionString, this.options.postgresSSL);
            this.mongooseConnection = await Database.getMongoConnection(this.options.mongoConnectionString);
            this.log('Connections established.');
        } else {
            this.client.release();
            this.client = await Database.getPostgresConnection(this.options.postgresConnectionString, this.options.postgresSSL);
        }
        this.log('Metadata Fetched.');
        const columns = await this.getAllColumns(options.fromSchema, options.fromTable);
        return this.generateSchema(options, columns);
    }

    // tslint:disable-next-line:member-ordering
    public validateOptions(options: TableTranslationInternal) {
        if (options.embedInParsed && options.embedInParsed.length > 1) {
            if (!options.onPersist) {
                throw new Error('Deep sub document embeds not supported without onPersist.');
            }
        }
        if ((options.embedIn || options.embedInRoot) && !options.toCollection) {
            throw new Error('Embeds require toCollection.');
        }
    }

    private prepareOptions(options: TableTranslation): TableTranslationInternal {
        options = Object.assign({}, defaultTableTranslationOptions, options);
        options.columns = Object.assign({}, options.columns || {});
        options.toCollection = options.toCollection || (options.mongifyTableName ? toMongoName(options.fromTable) : options.fromTable);
        const internalOptions = options as TableTranslationInternal;
        internalOptions.embedInParsed = this.parseEmbedIn(options.embedIn);
        internalOptions.hasEmbed = !!(internalOptions.embedIn || internalOptions.embedInRoot);
        this.validateOptions(internalOptions);
        return options as TableTranslationInternal;
    }

    private gatherDepedencies(...translationOptions: TableTranslationInternal[]) {
        const graphDefinition: any = {};
        for (const option of translationOptions) {
            if (!option.ignoreDependencies) {
                graphDefinition[option.hasEmbed ? option.fromTable : option.toCollection] = this.getDepedencyDefinition(option, translationOptions);
            }
        }
        const collectionKeys = translationOptions.reduce((obj, curr) => {
            if (!curr.hasEmbed) {
                const arr = obj[curr.toCollection] = obj[curr.toCollection] || [];
                arr.push(curr);
            }
            return obj;
        }, {});
        const tableKeys = translationOptions.reduce((obj, curr) => {
            if (curr.hasEmbed) {
                const arr = obj[curr.fromTable] = obj[curr.fromTable] || [];
                arr.push(curr);
            }
            return obj;
        }, {});
        const graph = this.getDepedencyGraph(graphDefinition);
        let results = [];
        for (const g of graph) {
            const sub = collectionKeys[g] || tableKeys[g];
            if (sub instanceof Array) {
                results = results.concat(sub);
            } else {
                results.push(sub);
            }
        }
        return { graph, results };
    }

    private getDepedencyDefinition(currentOption: TableTranslationInternal, otherOptions: TableTranslationInternal[]) {
        let deps = [];
        if (currentOption.addedDependencies) {
            deps = deps.concat(currentOption.addedDependencies);
        }
        if (currentOption.hasEmbed) {
            if (currentOption.embedInRoot || currentOption.embedInParsed.length === 1) {
                deps.push(currentOption.toCollection);
            } else {
                const parentName = currentOption.embedInParsed.slice(0, -1).join('.');
                const parents = otherOptions.filter((o) => o.embedIn === parentName);
                deps = deps.concat(parents.map((p) => p.fromTable));
            }
        }
        if (currentOption.columns) {
            for (const columnKey of Object.keys(currentOption.columns)) {
                const column = currentOption.columns[columnKey];
                if (column.translator) {
                    deps.push(column.translator.sourceCollection);
                }
            }
        }
        return Array.from(new Set(deps));
    }

    private getDepedencyGraph(graph: { [index: string]: string[] }): string[] {
        const sorted = [];
        const visited = {};
        function visit(name, ancestors) {
            if (!Array.isArray(ancestors)) {
                ancestors = [];
            }
            ancestors.push(name);
            visited[name] = true;
            if (!(name in graph)) {
                return;
            }
            graph[name].forEach((dep) => {
                if (ancestors.indexOf(dep) >= 0) {
                    throw new Error('Circular dependency. "' + dep + '" is required by "' + name + '": ' + ancestors.join(' -> '));
                }
                if (visited[dep]) {
                    return;
                }
                visit(dep, ancestors.slice(0));
            });
            if (sorted.indexOf(name) < 0) {
                sorted.push(name);
            }
        }
        Object.keys(graph).forEach(visit);
        return sorted;
    }

    private log(str: string) {
        // tslint:disable-next-line:no-console
        log(str);
    }

    private hasColumns(options: TableTranslationInternal) {
        if (options.columns && Object.keys(options.columns).length) {
            return true;
        }
        return false;
    }

    private applyLegacyId(translationOptions: TableTranslationInternal, columns: { [index: string]: PostgresColumnInfo }) {
        if (translationOptions.autoLegacyId && !('id' in translationOptions.columns) && 'id' in columns) {
            translationOptions.columns.id = {
                to: translationOptions.legacyIdDestinationName,
                index: true,
            };
        }
    }

    private addMissingColumns(translationOptions: TableTranslationInternal, columns: { [index: string]: PostgresColumnInfo }) {
        translationOptions.columns = Object.assign({}, Object.keys(columns).reduce((obj, curr) => {
            if (!(curr in translationOptions.columns)) {
                obj[curr] = { to: translationOptions.mongifyColumnNames ? toMongoName(curr) : curr };
            } else {
                obj[curr] = translationOptions.columns[curr];
            }
            return obj;
        }, {}), translationOptions.columns);
    }

    private parseEmbedIn(str: string) {
        return str ? str.split('.') : null;
    }

    private async processColumnIndex(translationOptions: TableTranslationInternal, columnOptions: ColumnTranslation) {
        let indexOptions = null;
        if (typeof columnOptions.index === 'boolean') {
            indexOptions = { background: true };
        } else {
            indexOptions = Object.assign({}, columnOptions.index, { background: true });
        }
        if (translationOptions.hasEmbed && !translationOptions.embedInRoot) {
            indexOptions.sparse = true;
            const cleanEmbedName = `${translationOptions.embedIn.replace(/\.\$/g, '')}.${columnOptions.to}`;
            await this.mongooseConnection.db.collection(translationOptions.toCollection).createIndex({ [cleanEmbedName]: 1 }, indexOptions);
        } else {
            await this.mongooseConnection.db.collection(translationOptions.toCollection).createIndex({ [columnOptions.to]: 1 }, indexOptions);
        }
    }

    private resolveJsonPath(str: string, obj: any) {
        return str.split('.').reduce((o, i) => o[i], obj);
    }

    private processDeletes(options: TableTranslationInternal, documents: any[]) {
        if (options.deleteFields) {
            for (const doc of documents) {
                for (const field of options.deleteFields) {
                    delete doc[field];
                }
            }
        }
    }

    private getSchemaForType(metadata: PostgresColumnInfo): any {
        const typeName = metadata.data_type;
        const udtName = metadata.udt_name;
        switch (typeName) {
            case 'json':
                return {
                    type: 'object',
                };
            case 'uuid':
                return {
                    type: 'string',
                    format: 'ObjectId',
                };
            case 'text':
            case 'character varying':
                return {
                    type: 'string',
                };
            case 'date':
            case 'time':
            case 'timestamp with time zone':
            case 'timestamp without time zone':
            case 'timestamp':
                return {
                    type: 'string',
                    format: 'date',
                };
            case 'boolean':
                return {
                    type: 'boolean',
                };
            case 'integer':
            case 'smallint':
            case 'bigint':
            case 'smallserial':
            case 'serial':
            case 'bigserial':
                return {
                    type: 'integer',
                };
            case 'decimal':
            case 'numeric':
            case 'real':
            case 'double precision':
                return {
                    type: 'number',
                };
            case 'USER-DEFINED':
                return {
                    type: 'object',
                    format: 'geoJSON',
                    properties: {
                        type: {
                            type: 'string',
                        },
                        coordinates: {
                            type: 'array',
                        },
                    },
                };

            default:
                throw new Error('Found unsupported type!!! Create a converter or submit an issue!');
        }
    }

    private overwriteMergeArray(destinationArray: any[], sourceArray: any[]) {
        if (destinationArray.length
            && sourceArray.length
            && typeof destinationArray[0] === 'string'
            && typeof sourceArray[0] === 'string') {
            const keys: any = {};
            for (const val of destinationArray) {
                keys[val] = null;
            }
            for (const val of sourceArray) {
                keys[val] = null;
            }
            return Object.keys(keys);
        } else {
            return sourceArray;
        }
    }

    private generateSchema(translationOptions: TableTranslationInternal, columns: { [index: string]: PostgresColumnInfo }) {
        this.processOptions(translationOptions, columns);
        this.log(`Generating Schema '${translationOptions.hasEmbed ? `${translationOptions.fromTable} -> ${translationOptions.toCollection}` : translationOptions.toCollection}'`);
        const schema: any = {
            type: 'object',
            properties: {},
        };
        if (translationOptions.indexes) {
            schema.indexes = translationOptions.indexes;
        }
        const deletedFields = (translationOptions.deleteFields || []).reduce((obj, cur) => {
            obj.set(cur, null);
            return obj;
        }, new Map<string, any>());
        for (const columnName of Object.keys(translationOptions.columns)) {
            const columnOptions = translationOptions.columns[columnName];
            if (!deletedFields.has(columnOptions.to)) {
                const columnMetadata = columns[columnName];
                let schemaType = null;
                if (columnMetadata) {
                    if (columnOptions.to === 'id' && columnMetadata.udt_name === 'uuid') {
                        columnOptions.to = '_id';
                    }
                    if (columnOptions.to !== translationOptions.legacyIdDestinationName && columnOptions.to !== translationOptions.embedSourceIdColumn && !columnMetadata.column_default) {
                        const required = columnMetadata.is_nullable === 'YES' ? false : true;
                        if (required) {
                            (schema.required = schema.required || []).push(columnOptions.to);
                        }
                    }
                }
                if (columnOptions.translator) {
                    if (!columnOptions.translator.desiredField) {
                        schemaType = {
                            type: 'string',
                            format: 'ObjectId',
                        };
                    } else {
                        schemaType = {
                            comment: 'Unable to determine schema',
                        };
                    }
                    schemaType = this.processJsonSchemaOptions(columnOptions, schemaType);
                } else if (!columnOptions.isVirtual) {
                    schemaType = this.getSchemaForType(columnMetadata);
                    if (schemaType.format === 'geoJSON') {
                        schemaType.title = columnOptions.to;
                        schemaType.index = { type: '2dsphere' };
                    }
                    if (columnMetadata && columnMetadata.column_default) {
                        const defaultValue = this.options.schemaDefaultValueConverter(schemaType.type, schemaType.format, columnMetadata.column_default);
                        if (defaultValue) {
                            schemaType.default = defaultValue;
                        }
                    }
                    schemaType = this.processJsonSchemaOptions(columnOptions, schemaType);
                }
                if (columnOptions.to === 'id' && schemaType.type === 'string' && schemaType.format === 'ObjectId') {
                    schema.properties._id = schemaType;
                } else {
                    schema.properties[columnOptions.to] = schemaType;
                }
            }
        }
        if (!schema.properties._id && translationOptions.autoLegacyId) {
            const dynamicColumns: any = translationOptions.dynamicColumns || {};
            dynamicColumns._id = {
                jsonSchema: {
                    type: 'string',
                    format: 'ObjectId',
                },
            };
            translationOptions.dynamicColumns = dynamicColumns;
        }
        for (const columnName of Object.keys(translationOptions.dynamicColumns || {})) {
            const dynamicColumnInfo = translationOptions.dynamicColumns[columnName];
            if (dynamicColumnInfo.jsonSchema) {
                schema.properties[columnName] = dynamicColumnInfo.jsonSchema;
                if (dynamicColumnInfo.jsonSchema.type === 'object' || dynamicColumnInfo.jsonSchema.type === 'array') {
                    schema.properties[columnName].title = columnName;
                }
            }
        }
        if (!translationOptions.hasEmbed) {
            schema.title = translationOptions.toCollection;
            let currentSchema = this.generatedSchemas[translationOptions.toCollection];
            if (currentSchema) {

                currentSchema = deepmerge(currentSchema, schema, { arrayMerge: this.overwriteMergeArray });
            } else {
                currentSchema = schema;
            }
            this.generatedSchemas[translationOptions.toCollection] = currentSchema;
        } else {
            const parentSchema = this.generatedSchemas[translationOptions.toCollection];
            if (parentSchema) {
                let newSchema: any = { type: 'array' };
                for (const columnName of Object.keys(translationOptions.columns)) {
                    const columnInfo = translationOptions.columns[columnName];
                    if (columnInfo.translator && columnInfo.translator.sourceCollection) {
                        newSchema.relatedObjects = newSchema.relatedObjects || [];
                        const relatedField = columnInfo.translator.desiredField || '_id';
                        const relatedCollection = columnInfo.translator.sourceCollection;
                        newSchema.relatedObjects.push({ relatedField, relatedCollection, localField: columnInfo.to });
                    }
                }
                if (translationOptions.embedArrayField) {
                    newSchema.items = schema.properties[translationOptions.embedArrayField];
                } else {
                    delete schema.properties[translationOptions.embedSourceIdColumn];
                    if (translationOptions.embedSingle || translationOptions.embedInRoot) {
                        delete schema.properties._id;
                        newSchema = schema;
                    } else {
                        newSchema.items = schema;
                    }
                }
                if (translationOptions.embedInRoot || translationOptions.embedInParsed.length === 1) {
                    if (!translationOptions.embedInRoot) {
                        newSchema.title = translationOptions.embedIn;
                        parentSchema.properties[translationOptions.embedIn] = deepmerge(parentSchema.properties[translationOptions.embedIn] || {}, newSchema, { arrayMerge: this.overwriteMergeArray });
                    } else {
                        Object.assign(parentSchema.properties, newSchema.properties);
                    }
                } else {
                    const title = translationOptions.embedInParsed.slice().pop();
                    newSchema.title = title;
                    const slicedArray = translationOptions.embedInParsed.slice(0, translationOptions.embedInParsed.length - 1);
                    const parentSchemaProperty = this.getJsonPropertyFromPathArray(slicedArray, parentSchema);
                    if (parentSchemaProperty) {
                        if (parentSchemaProperty.properties) {
                            parentSchemaProperty.properties[title] = newSchema;
                        } else {
                            parentSchemaProperty.items.properties[title] = newSchema;
                        }
                    } else {
                        throw new Error(`Unable to find path ${slicedArray.join('.')} in Schema(${parentSchema.title})`);
                    }
                }
            }
        }
    }

    private getJsonPropertyFromPathArray(arr: string[], obj: any) {
        let currObject = obj;
        for (const key of arr) {
            if (currObject) {
                let container = currObject;
                let foundObject;
                while (container && !foundObject) {
                    container = currObject.properties || currObject.items;
                    if (container) {
                        foundObject = container[key];
                    }
                }
                currObject = foundObject;
            } else {
                return;
            }
        }
        return currObject;
    }

    private processJsonSchemaOptions(columnOptions: ColumnTranslation, schemaType: any) {
        if (columnOptions.schemaOptions) {
            const schemaMode = columnOptions.schemaOptions.mode || 'inclusive';
            if (schemaMode === 'inclusive') {
                schemaType = Object.assign({}, schemaType, columnOptions.schemaOptions.jsonSchema);
            } else {
                schemaType = columnOptions.schemaOptions.jsonSchema;
            }
        }
        return schemaType;
    }

    private processOptions(translationOptions: TableTranslationInternal, columns: { [index: string]: PostgresColumnInfo }) {
        if (translationOptions.embedSourceIdColumn) {
            translationOptions.embedSourceIdColumn = translationOptions.mongifyColumnNames ? toMongoName(translationOptions.embedSourceIdColumn) : translationOptions.embedSourceIdColumn;
        }
        this.applyLegacyId(translationOptions, columns);
        this.addMissingColumns(translationOptions, columns);
        const columnToKeys = Object.keys(translationOptions.columns).reduce((obj, curr) => { obj[translationOptions.columns[curr].to] = null; return obj; }, {});
        if (translationOptions.hasEmbed && !(translationOptions.embedSourceIdColumn in columnToKeys)) {
            if (!(translationOptions.embedSourceIdColumn in columns)) {
                throw new Error(`Embed Source Id Column (${translationOptions.embedSourceIdColumn}) not found in Source Dataset.`);
            }
            const column = translationOptions.columns[translationOptions.embedSourceIdColumn];
            translationOptions.columns[translationOptions.embedSourceIdColumn] = Object.assign({}, column, {
                to: translationOptions.mongifyColumnNames ?
                    toMongoName(translationOptions.embedSourceIdColumn) : translationOptions.embedSourceIdColumn,
            });
        }
    }

    private async insertDocuments(translationOptions: TableTranslation, documents: any[]) {
        try {
            await this.mongooseConnection.db.collection(translationOptions.toCollection).insertMany(documents);
        } catch (err) {
            err.data = documents;
            if (err.index > -1) {
                const msg = `Error inserting document at index ${err.index}, trying again without (attempting to recover). Error: ${err}`;
                log(msg);
                // tslint:disable-next-line:no-console
                console.log(msg);
                const newDocuments = documents.slice(err.index + 1);
                if (newDocuments.length) {
                    await this.insertDocuments(translationOptions, newDocuments);
                }
            } else {
                throw err;
            }
        }
    }

    private async processRecords(translationOptions: TableTranslationInternal, columns: { [index: string]: PostgresColumnInfo }, cursor: any, totalCount: number = 0, count = 0) {
        return new Promise(async (resolve) => {
            if (!cursor) {
                log('Getting cursor');
                const countRow = await this.queryPostgres(`SELECT COUNT(*) FROM "${translationOptions.fromSchema}"."${translationOptions.fromTable}" ${translationOptions.customWhere ? `WHERE ${translationOptions.customWhere}` : ''};`);
                totalCount = typeof countRow[0].count === 'string' ? parseInt(countRow[0].count) : countRow[0].count;
                this.processOptions(translationOptions, columns);
                if (totalCount > 0) {
                    log('Got count.');
                    const selectColumns = Object.keys(columns).reduce((obj, key: string) => {
                        const curr = columns[key];
                        if (curr.data_type.toLowerCase() === 'user-defined' && (curr.udt_name === 'geography' || curr.udt_name === 'geometry')) {
                            obj.push(`ST_AsGeoJSON("${curr.column_name}") as "${curr.column_name}"`);
                        } else {
                            obj.push(`"${curr.column_name}"`);
                        }
                        return obj;
                    }, []).join(',');
                    cursor = this.client.query(new Cursor(`SELECT ${selectColumns} FROM "${translationOptions.fromSchema}"."${translationOptions.fromTable}" ${translationOptions.customWhere ? `WHERE ${translationOptions.customWhere}` : ''};`)) as any;
                }
            }
            if (translationOptions.indexes) {
                for (const index of translationOptions.indexes) {
                    const indexOptions = Object.assign({}, index.options || {}, { background: true });
                    await this.mongooseConnection.db.collection(translationOptions.toCollection).createIndex(index.descriptor, indexOptions);
                    log('Applied indexes.');
                }
            }
            if (totalCount === 0) {
                this.log('No records found.');
                return resolve();
            }
            if (count >= totalCount) {
                cursor.close(() => {
                    resolve();
                });
            } else {
                log('Started cursor read.');
                cursor.read(this.options.batchSize, async (err, rows: any[]) => {
                    log('Reading rows.');
                    if (rows.length === 0) {
                        log('No rows.');
                        return resolve();
                    }
                    let documents = [];
                    for (const row of rows) {
                        const document: any = {};
                        for (const columnName of Object.keys(translationOptions.columns)) {
                            const columnValue = row[columnName];
                            const columnOptions = translationOptions.columns[columnName];
                            const columnMetadata = columns[columnName];
                            if (columnOptions.index) {
                                await this.processColumnIndex(translationOptions, columnOptions);
                            }
                            if (!columnOptions.isVirtual) {
                                const converter = columnOptions.converter || this.getConverter(columnMetadata);
                                const value = converter(columnValue, columnMetadata.data_type, row, document, columnName, columnOptions.to, columnMetadata.udt_name);
                                if (!value || (value && !value.documentModified)) {
                                    document[columnOptions.to] = value;
                                }
                            }
                        }
                        for (const dynamicColumnName of Object.keys(translationOptions.dynamicColumns || {})) {
                            const dynamicColumn = translationOptions.dynamicColumns[dynamicColumnName];
                            document[dynamicColumnName] = dynamicColumn.value(translationOptions, document);
                        }
                        if (translationOptions.includeId) {
                            if (!document._id) {
                                document._id = new mongoose.Types.ObjectId();
                            }
                            if (translationOptions.hasEmbed && !translationOptions.embedInRoot) {
                                await this.processColumnIndex(translationOptions, { to: '_id', index: { unique: true } });
                            }

                        }
                        if (translationOptions.includeTimestamps && !document.createdAt) {
                            document.createdAt = new Date();
                        }
                        if (translationOptions.includeTimestamps && !document.updatedAt) {
                            document.updatedAt = new Date();
                        }
                        if (translationOptions.includeVersion && !document.__v) {
                            document.__v = 0;
                        }
                        documents.push(document);
                    }
                    if (translationOptions.postProcess) {
                        log('Executing Post Process.');
                        const promise = translationOptions.postProcess(translationOptions, documents);
                        if (promise && promise.then) {
                            await promise;
                        }
                    }
                    log('Executing Translations.');
                    for (const columnKey of Object.keys(translationOptions.columns)) {
                        const column = translationOptions.columns[columnKey];
                        if (column.translator) {
                            const uniqueKeys = documents.reduce((obj, curr) => {
                                const arr = obj[curr[column.to]] = obj[curr[column.to]] || { key: curr[column.to], docs: [] };
                                arr.docs.push(curr);
                                return obj;
                            }, {});
                            const keyArray = Object.keys(uniqueKeys).reduce((obj, curr) => {
                                const item = uniqueKeys[curr];
                                obj.push(item.key);
                                return obj;
                            }, []);
                            const desiredField = column.translator.desiredField || '_id';
                            if (!column.translator.sourceIdField && (!column.translator.sourceQuery || !column.translator.sourceProjection)) {
                                throw new Error('sourceQuery or sourceProjection must be specified if no sourceIdField provided.');
                            }
                            let query = { [column.translator.sourceIdField]: { $in: keyArray } };
                            if (column.translator.sourceQuery) {
                                query = column.translator.sourceQuery(translationOptions, documents, keyArray);
                            }
                            let projection = { [desiredField]: 1, [column.translator.sourceIdField]: 1 };
                            if (column.translator.sourceProjection) {
                                if (!column.translator.processor) {
                                    throw new Error('If using a sourceProjection you must specify a processor');
                                }
                                projection = column.translator.sourceProjection(translationOptions, documents, keyArray);
                            }
                            const ids = await this.mongooseConnection.db.collection(column.translator.sourceCollection)
                                .find(query, projection).toArray();
                            if (column.translator.processor) {
                                column.translator.processor(translationOptions, uniqueKeys, ids);
                            } else {
                                for (const idObject of ids) {
                                    const lookupObject = { key: idObject[column.translator.sourceIdField], desiredValue: idObject[desiredField] };
                                    const lookupResult = uniqueKeys[lookupObject.key];
                                    for (const doc of lookupResult.docs) {
                                        doc[column.to] = lookupObject.desiredValue;
                                    }
                                }
                            }
                        }
                    }
                    log('Done Executing Translations.');
                    if (translationOptions.filter) {
                        const documentCount = documents.length;
                        log('Executing Filter.');
                        documents = documents.filter(translationOptions.filter);
                        const amountRemoved = documentCount - documents.length;
                        this.log(`Filtered - Removed ${amountRemoved} of ${documentCount}.`);
                    }
                    count += rows.length;
                    if (documents.length) {
                        if (!this.options.includeNulls && !translationOptions.includeNulls) {
                            this.purgeNulls(documents);
                        }
                        if (translationOptions.onPersist) {
                            log('Executing OnPersist.');
                            if (!translationOptions.ignoreDeletesOnPersist) {
                                this.processDeletes(translationOptions, documents);
                            }
                            await translationOptions.onPersist(translationOptions, documents);
                            log('Done Executing OnPersist.');
                        } else {
                            if (!translationOptions.hasEmbed) {
                                this.processDeletes(translationOptions, documents);
                                await this.insertDocuments(translationOptions, documents);
                            } else {
                                const sourceColumnName = translationOptions.embedSourceIdColumn;
                                const documentsBySourceColumnKey = documents.reduce((obj, curr) => {
                                    const group = obj[curr[sourceColumnName].toString()] = obj[curr[sourceColumnName].toString()] || { key: curr[sourceColumnName], docs: [] };
                                    if (!translationOptions.preserveEmbedSourceId) {
                                        delete curr[sourceColumnName];
                                    }
                                    group.docs.push(curr);
                                    return obj;
                                }, {});
                                for (const documentGroupKey of Object.keys(documentsBySourceColumnKey)) {
                                    const documentGroup = documentsBySourceColumnKey[documentGroupKey];
                                    let finalDocumentsOrValues = documentGroup.docs;
                                    if (translationOptions.embedArrayField) {
                                        finalDocumentsOrValues = finalDocumentsOrValues.map((d) => d[translationOptions.embedArrayField]);
                                    }
                                    const normalizedKeyValue = documentGroup.key;
                                    const filter: any = {};
                                    filter[translationOptions.embedTargetIdColumn] = typeof normalizedKeyValue === 'string' && normalizedKeyValue.length === 24 ? new mongoose.Types.ObjectId(normalizedKeyValue) : normalizedKeyValue;
                                    let update: any = { $push: { [translationOptions.embedIn]: { $each: finalDocumentsOrValues } } };
                                    if (translationOptions.embedInRoot) {
                                        update = { $set: finalDocumentsOrValues[0] };
                                    }
                                    if (translationOptions.embedSingle) {
                                        update = { $set: { [translationOptions.embedIn]: finalDocumentsOrValues[0] } };
                                    }
                                    this.processDeletes(translationOptions, documents);
                                    try {
                                        await this.mongooseConnection.db.collection(translationOptions.toCollection).updateMany(filter, update);
                                    } catch (err) {
                                        err.data = documents;
                                        throw err;
                                    }
                                }
                            }
                        }
                    }
                    this.log(`Processed ${count} rows of ${totalCount}`);
                    documents = null;
                    await this.processRecords(translationOptions, columns, cursor, totalCount, count);
                    resolve();
                });
            }
        });
    }

    private purgeNulls(documents: any[]) {
        for (const document of documents) {
            for (const key of Object.keys(document)) {
                const value = document[key];
                if (value === null) {
                    delete document[key];
                }
            }
        }
    }

    private getConverter(metadata: PostgresColumnInfo): ColumnConverter {
        const typeName = metadata.data_type;
        const udtName = metadata.udt_name;
        switch (typeName) {
            case 'json':
                return (sourceValue) => {
                    return sourceValue;
                };
            case 'uuid':
                return (sourceValue, sourceType, row, document, sourceName, toName) => {
                    let destinationValue = null;
                    if (sourceValue) {
                        destinationValue = new mongoose.Types.ObjectId(uuidToObjectIdString(sourceValue));
                    } else {
                        destinationValue = null;
                    }
                    if (sourceName === 'id' && toName === sourceName) {
                        document._id = destinationValue;
                        return {
                            documentModified: true,
                        };
                    }
                    return destinationValue;
                };
            case 'text':
            case 'character varying':
                return (sourceValue) => sourceValue;
            case 'date':
            case 'time':
            case 'timestamp with time zone':
            case 'timestamp without time zone':
            case 'timestamp':
                return (sourceValue) => {
                    if (sourceValue) {
                        if (sourceValue instanceof Date) {
                            return sourceValue;
                        }
                        return new Date(sourceValue);
                    }
                    return null;
                };
            case 'boolean':
                return (sourceValue) => {
                    return sourceValue;
                };
            case 'integer':
            case 'real':
            case 'numeric':
            case 'smallint':
            case 'double precision':
                return (sourceValue) => {
                    return sourceValue;
                };
            case 'USER-DEFINED':
                if (udtName === 'geography' || udtName === 'geometry') {
                    return (sourceValue) => {
                        if (sourceValue) {
                            return JSON.parse(sourceValue);
                        }
                        return null;
                    };
                } else {
                    return (sourceValue) => {
                        return null;
                    };
                }

            default:
                throw new Error('Found unsupported type!!! Create a converter or submit an issue!');
        }
    }

    private queryPostgres<T = any>(queryString: string): Promise<T[]> {
        return new Promise<T[]>((resolve) => {
            return this.client.query(queryString).then((result) => {
                resolve(result.rows && result.rows.length ? result.rows : []);
            }).catch((err) => {
                this.log(err);
            });
        });
    }

    private getCacheDirectory() {
        return path.resolve(this.options.cacheDirectory);
    }

    private getCacheFilePath(schema: string, tableName: string) {
        return path.join(this.getCacheDirectory(), `./${schema}.${tableName}.json`);
    }

    private cacheExists(schema: string, tableName: string) {
        const cacheDirectory = this.getCacheFilePath(schema, tableName);
        return fs.existsSync(cacheDirectory);
    }

    private saveMetadata(schema: string, tableName: string, metadata: { [index: string]: PostgresColumnInfo }) {
        const baseDirectory = this.getCacheDirectory();
        if (!fs.existsSync(baseDirectory)) {
            fs.mkdirSync(baseDirectory);
        }
        const filePath = this.getCacheFilePath(schema, tableName);
        fs.writeFileSync(filePath, JSON.stringify(metadata, null, 4), 'utf-8');
    }

    private async getAllColumns(schema: string, tableName: string) {
        if (this.options.useMetadataCache) {
            if (this.cacheExists(schema, tableName)) {
                const json = JSON.parse(fs.readFileSync(this.getCacheFilePath(schema, tableName), 'utf-8'));
                this.log(`Found cached metadata for ${schema}.${tableName}.`);
                return json as { [index: string]: PostgresColumnInfo };
            }
        }
        const results = await this.queryPostgres(`SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '${tableName}' AND table_schema = '${schema}';`);
        const columnResults = results.reduce((obj, currentRow: PostgresColumnInfo) => {
            obj[currentRow.column_name] = currentRow;
            return obj;
        }, {}) as { [index: string]: PostgresColumnInfo };
        if (this.options.createMetadataCache) {
            this.saveMetadata(schema, tableName, columnResults);
        }
        return columnResults;
    }
}
