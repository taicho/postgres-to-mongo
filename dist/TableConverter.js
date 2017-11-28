"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const mongoose = require("mongoose");
const Cursor = require("pg-cursor");
const Common_1 = require("./Common");
const Database = require("./Database");
const defaultOptions = {
    batchSize: 5000,
    postgresSSL: false,
};
const defaultTableTranslationOptions = {
    includeTimestamps: true,
    includeVersion: true,
    includeId: true,
    legacyIdDestinationName: '_legacyId',
};
function uuidToObjectIdString(sourceValue) {
    return (sourceValue).replace(/-/g, '').substring(0, 24);
}
exports.uuidToObjectIdString = uuidToObjectIdString;
class TableConverter {
    constructor(options) {
        this.options = Object.assign({}, defaultOptions, options);
    }
    convertTables(...options) {
        return __awaiter(this, void 0, void 0, function* () {
            options = options.map((o) => this.prepareOptions(o));
            const dependencies = this.gatherDepedencies(...options);
            for (const tableOptions of dependencies.results) {
                try {
                    yield this.convertTableInternal(tableOptions, false);
                }
                catch (err) {
                    this.log(err);
                    if (this.client) {
                        this.client.release();
                        this.client = null;
                    }
                }
            }
            this.purgeClient();
        });
    }
    purgeClient() {
        if (this.client) {
            this.client.release();
            this.client = null;
        }
    }
    convertTable(options) {
        return __awaiter(this, void 0, void 0, function* () {
            this.convertTableInternal(options, true);
        });
    }
    convertTableInternal(options, prepare) {
        return __awaiter(this, void 0, void 0, function* () {
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
                this.client = yield Database.getPostgresConnection(this.options.postgresConnectionString, this.options.postgresSSL);
                this.mongooseConnection = yield Database.getMongoConnection(this.options.mongoConnectionString);
                this.log('Connections established.');
            }
            else {
                this.client.release();
                this.client = yield Database.getPostgresConnection(this.options.postgresConnectionString, this.options.postgresSSL);
            }
            this.log(`Processing records for ${options.fromTable} => ${options.toCollection}.`);
            this.log('Metadata Fetched.');
            const columns = yield this.getAllColumns(options.fromSchema, options.fromTable);
            yield this.processRecords(options, columns, null);
        });
    }
    prepareOptions(options) {
        options = Object.assign({}, defaultTableTranslationOptions, options);
        options.columns = Object.assign({}, options.columns || {});
        options.toCollection = options.toCollection || (options.mongifyTableName ? Common_1.toMongoName(options.fromTable) : options.fromTable);
        return options;
    }
    gatherDepedencies(...translationOptions) {
        const graphDefinition = {};
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
            }
            else {
                results.push(sub);
            }
        }
        return { graph, results };
    }
    getDepedencyDefinition(translationOptions) {
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
    getDepedencyGraph(graph) {
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
    log(str) {
        console.log(str);
    }
    hasColumns(options) {
        if (options.columns && Object.keys(options.columns).length) {
            return true;
        }
        return false;
    }
    applyLegacyId(translationOptions, columns) {
        if (translationOptions.autoLegacyId && !('id' in translationOptions.columns) && 'id' in columns) {
            translationOptions.columns.id = {
                to: translationOptions.legacyIdDestinationName,
                index: true,
            };
        }
    }
    addMissingColumns(translationOptions, columns) {
        translationOptions.columns = Object.assign({}, Object.keys(columns).reduce((obj, curr) => {
            if (!(curr in translationOptions.columns)) {
                obj[curr] = { to: translationOptions.mongifyColumnNames ? Common_1.toMongoName(curr) : curr };
            }
            else {
                obj[curr] = translationOptions.columns[curr];
            }
            return obj;
        }, {}), translationOptions.columns);
    }
    processColumnIndex(translationOptions, columnOptions) {
        return __awaiter(this, void 0, void 0, function* () {
            let indexOptions = null;
            if (typeof columnOptions.index === 'boolean') {
                indexOptions = { background: true };
            }
            else {
                indexOptions = Object.assign({}, columnOptions.index, { background: true });
            }
            if (translationOptions.embedIn) {
                indexOptions.sparse = true;
                const cleanEmbedName = `${translationOptions.embedIn.replace(/\.\$/g, '')}.${columnOptions.to}`;
                yield this.mongooseConnection.db.collection(translationOptions.toCollection).createIndex({ [cleanEmbedName]: 1 }, indexOptions);
            }
            else {
                yield this.mongooseConnection.db.collection(translationOptions.toCollection).createIndex({ [columnOptions.to]: 1 }, indexOptions);
            }
        });
    }
    resolveJsonPath(str, obj) {
        return str.split('.').reduce((o, i) => o[i], obj);
    }
    processDeletes(options, documents) {
        if (options.deleteFields) {
            for (const doc of documents) {
                for (const field of options.deleteFields) {
                    delete doc[field];
                }
            }
        }
    }
    processRecords(translationOptions, columns, cursor, totalCount = 0, count = 0) {
        return __awaiter(this, void 0, void 0, function* () {
            return new Promise((resolve) => __awaiter(this, void 0, void 0, function* () {
                if (!cursor) {
                    const countRow = yield this.queryPostgres(`SELECT COUNT(*) FROM "${translationOptions.fromSchema}"."${translationOptions.fromTable}";`);
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
                                Common_1.toMongoName(translationOptions.embedSourceIdColumn) : translationOptions.embedSourceIdColumn,
                        });
                    }
                    if (totalCount > 0) {
                        const selectColumns = Object.keys(columns).reduce((obj, key) => {
                            const curr = columns[key];
                            if (curr.data_type.toLowerCase() === 'user-defined' && (curr.udt_name === 'geography' || curr.udt_name === 'geometry')) {
                                obj.push(`ST_AsGeoJSON("${curr.column_name}") as "${curr.column_name}"`);
                            }
                            else {
                                obj.push(`"${curr.column_name}"`);
                            }
                            return obj;
                        }, []).join(',');
                        cursor = this.client.query(new Cursor(`SELECT ${selectColumns} FROM "${translationOptions.fromSchema}"."${translationOptions.fromTable}";`));
                    }
                }
                if (translationOptions.indexes) {
                    for (const index of translationOptions.indexes) {
                        const indexOptions = Object.assign({}, index.options || {}, { background: true });
                        yield this.mongooseConnection.db.collection(translationOptions.toCollection).createIndex(index.descriptor, indexOptions);
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
                }
                else {
                    cursor.read(this.options.batchSize, (err, rows) => __awaiter(this, void 0, void 0, function* () {
                        if (rows.length === 0) {
                            return resolve();
                        }
                        let documents = [];
                        for (const row of rows) {
                            const document = {};
                            for (const columnName of Object.keys(translationOptions.columns)) {
                                const columnValue = row[columnName];
                                const columnOptions = translationOptions.columns[columnName];
                                const columnMetadata = columns[columnName];
                                if (columnOptions.index) {
                                    yield this.processColumnIndex(translationOptions, columnOptions);
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
                                    yield this.processColumnIndex(translationOptions, { to: '_id', index: { unique: true } });
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
                                yield promise;
                            }
                        }
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
                                const ids = yield this.mongooseConnection.db.collection(column.translator.sourceCollection)
                                    .find(query, projection).toArray();
                                if (column.translator.processor) {
                                    column.translator.processor(translationOptions, uniqueKeys, ids);
                                }
                                else {
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
                                    yield this.mongooseConnection.db.collection(translationOptions.toCollection).insertMany(documents);
                                }
                                catch (err) {
                                    err.data = documents;
                                    throw err;
                                }
                            }
                            else {
                                const sourceColumnName = translationOptions.mongifyColumnNames ? Common_1.toMongoName(translationOptions.embedSourceIdColumn) : translationOptions.embedSourceIdColumn;
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
                                    const filter = {};
                                    filter[translationOptions.embedTargetIdColumn] = typeof normalizedKeyValue === 'string' && normalizedKeyValue.length === 24 ? new mongoose.Types.ObjectId(normalizedKeyValue) : normalizedKeyValue;
                                    let update = { $push: { [translationOptions.embedIn]: { $each: finalValues } } };
                                    if (translationOptions.embedSingle) {
                                        update = { $set: { [translationOptions.embedIn]: finalValues[0] } };
                                    }
                                    this.processDeletes(translationOptions, documents);
                                    try {
                                        yield this.mongooseConnection.db.collection(translationOptions.toCollection).updateMany(filter, update);
                                    }
                                    catch (err) {
                                        err.data = documents;
                                        throw err;
                                    }
                                }
                            }
                        }
                        this.log(`Processed ${count} rows of ${totalCount}`);
                        documents = null;
                        yield this.processRecords(translationOptions, columns, cursor, totalCount, count);
                        resolve();
                    }));
                }
            }));
        });
    }
    getConverter(typeName, udtName) {
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
                    }
                    else {
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
                }
                else {
                    return (sourceValue) => {
                        return null;
                    };
                }
            default:
                throw new Error('Found unsupported type!!! Create a converter or submit an issue!');
        }
    }
    queryPostgres(queryString) {
        return new Promise((resolve) => {
            return this.client.query(queryString).then((result) => {
                resolve(result.rows && result.rows.length ? result.rows : []);
            }).catch((err) => {
                this.log(err);
            });
        });
    }
    getAllColumns(schema, tableName) {
        return __awaiter(this, void 0, void 0, function* () {
            const results = yield this.queryPostgres(`SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '${tableName}' AND table_schema = '${schema}';`);
            return results.reduce((obj, currentRow) => {
                obj[currentRow.column_name] = currentRow;
                return obj;
            }, {});
        });
    }
}
exports.TableConverter = TableConverter;
//# sourceMappingURL=TableConverter.js.map