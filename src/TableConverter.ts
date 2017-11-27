import * as mongoose from 'mongoose';
import { mongo } from 'mongoose';
import * as pg from 'pg';
import * as Cursor from 'pg-cursor';
import { start } from 'repl';
import { IColumnConverter } from 'src/interfaces/IColumnConverter';
import { IColumnTranslation } from 'src/interfaces/IColumnTranslation';
import { callbackify } from 'util';
import * as Database from './Database';
import { IConverterOptions } from './interfaces/IConverterOptions';
import { IPostgresColumnInfo } from './interfaces/IPostgresColumnInfo';
import { ITableTranslation } from './interfaces/ITableTranslation';

const defaultOptions: Partial<IConverterOptions> = {
    batchSize: 5000,
    postgresSSL: false,
};

const defaultTableTranslationOptions: Partial<ITableTranslation> = {
    includeTimestamps: true,
    includeVersion: true,
    includeId: true,
    legacyIdDestinationName: '_legacyId',
};

export function uuidToObjectIdString(sourceValue: string): string {
    return (sourceValue).replace(/-/g, '').substring(0, 24);
}

export class TableConverter {
    private client: pg.Client;
    private mongooseConnection: mongoose.Connection;
    private options: IConverterOptions;

    constructor(options: IConverterOptions) {
        this.options = Object.assign({}, defaultOptions, options);
    }

    public async convertTables(...options: ITableTranslation[]) {
        options = options.map((o) => this.prepareOptions(o));
        const dependencies = this.gatherDepedencies(...options);
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

    private purgeClient() {
        if (this.client) {
            this.client.release();
            this.client = null;
        }
    }

    private async convertTable(options: ITableTranslation) {
        this.convertTableInternal(options, true);
    }

    private async convertTableInternal(options: ITableTranslation, prepare: boolean) {
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
        await this.processRecords(options, columns, null);
    }

    private prepareOptions(options: ITableTranslation) {
        options = Object.assign({}, defaultTableTranslationOptions, options);
        options.columns = Object.assign({}, options.columns || {});
        options.toCollection = options.toCollection || (options.mongifyTableName ? this.toMongoName(options.fromTable) : options.fromTable);
        return options;
    }

    private gatherDepedencies(...translationOptions: ITableTranslation[]) {
        const graphDefinition: any = {};
        for (const options of translationOptions) {
            if (!options.ignoreDependencies) {
                graphDefinition[options.embedIn ? options.fromTable : options.toCollection] = this.getDepedencyDefinition(options);
            }
        }
        const collectionKeys = translationOptions.reduce((obj, curr) => {
            if (!curr.embedIn) {
                const arr = obj[curr.toCollection] = obj[curr.toCollection] || [];
                arr.push(curr);
            }
            return obj;
        }, {});
        const tableKeys = translationOptions.reduce((obj, curr) => {
            if (curr.embedIn) {
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

    private getDepedencyDefinition(translationOptions: ITableTranslation) {
        let deps = [];
        if (translationOptions.addedDependencies) {
            deps = deps.concat(translationOptions.addedDependencies);
        }
        if (translationOptions.embedIn) {
            deps.push(translationOptions.toCollection);
        }
        if (translationOptions.columns) {
            for (const columnKey of Object.keys(translationOptions.columns)) {
                const column = translationOptions.columns[columnKey];
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

    private camelize(str) {
        return str.replace(/(?:^\w|[A-Z]|\b\w|\s+)/g, (match, index) => {
            if (+match === 0) {
                return '';
            }
            return index === 0 ? match.toLowerCase() : match.toUpperCase();
        });
    }

    private toMongoName(name: string) {
        if (name.includes('_')) {
            return this.camelize(name.replace(/_/g, ' '));
        } else {
            return name;
        }
    }

    private log(str: string) {
        // tslint:disable-next-line:no-console
        console.log(str);
    }

    private hasColumns(options: ITableTranslation) {
        if (options.columns && Object.keys(options.columns).length) {
            return true;
        }
        return false;
    }

    private applyLegacyId(translationOptions: ITableTranslation, columns: { [index: string]: IPostgresColumnInfo }) {
        if (translationOptions.autoLegacyId && !('id' in translationOptions.columns) && 'id' in columns) {
            translationOptions.columns.id = {
                to: translationOptions.legacyIdDestinationName,
                index: true,
            };
        }
    }

    private addMissingColumns(translationOptions: ITableTranslation, columns: { [index: string]: IPostgresColumnInfo }) {
        translationOptions.columns = Object.assign({}, Object.keys(columns).reduce((obj, curr) => {
            if (!(curr in translationOptions.columns)) {
                obj[curr] = { to: translationOptions.mongifyColumnNames ? this.toMongoName(curr) : curr };
            } else {
                obj[curr] = translationOptions.columns[curr];
            }
            return obj;
        }, {}), translationOptions.columns);
    }

    private async processColumnIndex(translationOptions: ITableTranslation, columnOptions: IColumnTranslation) {
        let indexOptions = null;
        if (typeof columnOptions.index === 'boolean') {
            indexOptions = { background: true };
        } else {
            indexOptions = Object.assign({}, columnOptions.index, { background: true });
        }
        if (translationOptions.embedIn) {
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

    private processDeletes(options: ITableTranslation, documents: any[]) {
        if (options.deleteFields) {
            for (const doc of documents) {
                for (const field of options.deleteFields) {
                    delete doc[field];
                }
            }
        }
    }

    private async processRecords(translationOptions: ITableTranslation, columns: { [index: string]: IPostgresColumnInfo }, cursor: any, totalCount: number = 0, count = 0) {
        return new Promise(async (resolve) => {
            if (!cursor) {
                const countRow = await this.queryPostgres(`SELECT COUNT(*) FROM "${translationOptions.fromSchema}"."${translationOptions.fromTable}";`);
                totalCount = typeof countRow[0].count === 'string' ? parseInt(countRow[0].count) : countRow[0].count;
                this.applyLegacyId(translationOptions, columns);
                this.addMissingColumns(translationOptions, columns);
                const columnToKeys = Object.keys(translationOptions.columns).reduce((obj, curr) => { obj[translationOptions.columns[curr].to] = null; return obj; }, {});
                if (translationOptions.embedIn && !(translationOptions.embedSourceIdColumn in columnToKeys)) {
                    if (!(translationOptions.embedSourceIdColumn in columns)) {
                        throw new Error(`Embed Source Id Column (${translationOptions.embedSourceIdColumn}) not found in Source Dataset.`);
                    }
                    const column = translationOptions.columns[translationOptions.embedSourceIdColumn];
                    translationOptions.columns[translationOptions.embedSourceIdColumn] = Object.assign({}, column, {
                        to: translationOptions.mongifyColumnNames ?
                            this.toMongoName(translationOptions.embedSourceIdColumn) : translationOptions.embedSourceIdColumn,
                    });
                }
                if (totalCount > 0) {
                    const selectColumns = Object.keys(columns).reduce((obj, key: string) => {
                        const curr = columns[key];
                        if (curr.data_type.toLowerCase() === 'user-defined' && (curr.udt_name === 'geography' || curr.udt_name === 'geometry')) {
                            obj.push(`ST_AsGeoJSON("${curr.column_name}") as "${curr.column_name}"`);
                        } else {
                            obj.push(`"${curr.column_name}"`);
                        }
                        return obj;
                    }, []).join(',');
                    cursor = this.client.query(new Cursor(`SELECT ${selectColumns} FROM "${translationOptions.fromSchema}"."${translationOptions.fromTable}";`)) as any;
                }
            }
            if (translationOptions.indexes) {
                for (const index of translationOptions.indexes) {
                    const indexOptions = Object.assign({}, index.options || {}, { background: true });
                    await this.mongooseConnection.db.collection(translationOptions.toCollection).createIndex(index.descriptor, indexOptions);
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
                cursor.read(this.options.batchSize, async (err, rows: any[]) => {
                    if (rows.length === 0) {
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
                                const converter = columnOptions.converter || this.getConverter(columnMetadata.data_type, columnMetadata.udt_name);
                                const value = converter(columnValue, columnMetadata.data_type, row, document, columnName, columnOptions.to, columnMetadata.udt_name);
                                if (!value || (value && !value.documentModified)) {
                                    document[columnOptions.to] = value;
                                }
                            }
                        }
                        for (const dynamicColumnName of Object.keys(translationOptions.dynamicColumns || {})) {
                            const dynamicColumn = translationOptions.dynamicColumns[dynamicColumnName];
                            dynamicColumn(translationOptions, document);
                        }
                        if (translationOptions.includeId) {
                            if (!document._id) {
                                document._id = new mongoose.Types.ObjectId();
                            }
                            if (translationOptions.embedIn) {
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
                        const promise = translationOptions.postProcess(translationOptions, documents);
                        if (promise && promise.then) {
                            await promise;
                        }
                    }
                    for (const columnKey of Object.keys(translationOptions.columns)) {
                        const column = translationOptions.columns[columnKey];
                        if (column.translator) {
                            // {'fdsfdsf/fdsffsf'}
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
                                    // { _id:foo, maps: [{_legacyId:foo}]}
                                    const lookupObject = { key: idObject[column.translator.sourceIdField], desiredValue: idObject[desiredField] };
                                    const lookupResult = uniqueKeys[lookupObject.key];
                                    for (const doc of lookupResult.docs) {
                                        doc[column.to] = lookupObject.desiredValue;
                                    }
                                }
                            }
                        }
                    }
                    if (translationOptions.filter) {
                        const documentCount = documents.length;
                        documents = documents.filter(translationOptions.filter);
                        const amountRemoved = documentCount - documents.length;
                        this.log(`Filtered - Removed ${amountRemoved} of ${documentCount}.`);
                    }
                    count += rows.length;
                    if (documents.length) {
                        if (!translationOptions.embedIn) {
                            this.processDeletes(translationOptions, documents);
                            try {
                                await this.mongooseConnection.db.collection(translationOptions.toCollection).insertMany(documents);
                            } catch (err) {
                                err.data = documents;
                                throw err;
                            }
                        } else {
                            const sourceColumnName = translationOptions.mongifyColumnNames ? this.toMongoName(translationOptions.embedSourceIdColumn) : translationOptions.embedSourceIdColumn;
                            const groups = documents.reduce((obj, curr) => {
                                const group = obj[curr[sourceColumnName].toString()] = obj[curr[sourceColumnName].toString()] || { key: curr[sourceColumnName], docs: [] };
                                if (!translationOptions.preserveEmbedSourceId) {
                                    delete curr[sourceColumnName];
                                }
                                group.docs.push(curr);
                                return obj;
                            }, {});
                            for (const groupKey of Object.keys(groups)) {
                                const group = groups[groupKey];
                                let finalValues = group.docs;
                                if (translationOptions.embedArrayField) {
                                    finalValues = finalValues.map((d) => d[translationOptions.embedArrayField]);
                                }
                                const normalizedKeyValue = group.key;
                                const filter: any = {};
                                filter[translationOptions.embedTargetIdColumn] = typeof normalizedKeyValue === 'string' && normalizedKeyValue.length === 24 ? new mongoose.Types.ObjectId(normalizedKeyValue) : normalizedKeyValue;
                                let update: any = { $push: { [translationOptions.embedIn]: { $each: finalValues } } };
                                if (translationOptions.embedSingle) {
                                    update = { $set: { [translationOptions.embedIn]: finalValues[0] } };
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
                    this.log(`Processed ${count} rows of ${totalCount}`);
                    documents = null;
                    await this.processRecords(translationOptions, columns, cursor, totalCount, count);
                    resolve();
                });
            }
        });
    }

    private getConverter(typeName: string, udtName: string): IColumnConverter {
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

    private async getAllColumns(schema: string, tableName: string) {
        const results = await this.queryPostgres(`SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '${tableName}' AND table_schema = '${schema}';`);
        return results.reduce((obj, currentRow: IPostgresColumnInfo) => {
            obj[currentRow.column_name] = currentRow;
            return obj;
        }, {}) as { [index: string]: IPostgresColumnInfo };
    }
}
