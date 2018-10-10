import { TableService, TableQuery, TableUtilities, createTableService, TableBatch } from "azure-storage";
import { fromBase64, toBase64 } from "../common";
import { isString, isArray } from "util";
import "reflect-metadata";

const azureEdmMetadataKey = Symbol("Azure.Edm");
const azureIgnoreMetadataKey = Symbol("Azure.Ignore");
const int64EdmMetadataKey = "Edm.Int64";
const int32EdmMetadataKey = "Edm.Int32";
const doubleEdmMetadataKey = "Edm.Double";

export function Ignore() {
    return (target: Object, propertyKey: string | symbol) => Reflect.defineMetadata(azureIgnoreMetadataKey, true, target, propertyKey);
}

export function Int64() {
    return (target: Object, propertyKey: string | symbol) => Reflect.defineMetadata(azureEdmMetadataKey, int64EdmMetadataKey, target, propertyKey);
}

export function Int32() {
    return (target: Object, propertyKey: string | symbol) => Reflect.defineMetadata(azureEdmMetadataKey, int32EdmMetadataKey, target, propertyKey);
}

export function Double() {
    return (target: Object, propertyKey: string | symbol) => Reflect.defineMetadata(azureEdmMetadataKey, doubleEdmMetadataKey, target, propertyKey);
}

export function fromAzure<T extends AzureEntity>(entity: any, t: new () => T): T;
export function fromAzure(continuationToken: TableService.TableContinuationToken): string;
export function fromAzure<T extends AzureEntity>(entityOrContinuationToken: any | TableService.TableContinuationToken, t?: new () => T): T | string {
    if (!entityOrContinuationToken) {
        return null;
    }
    if (!t) {
        return toBase64(entityOrContinuationToken);
    } else {
        const result = new t() as any; // cast to "any" type to be able to set properties by name
        for (const key in entityOrContinuationToken) {
            if (entityOrContinuationToken.hasOwnProperty(key)) {
                if (!!entityOrContinuationToken[key] && entityOrContinuationToken[key].hasOwnProperty("_")) {
                    switch (entityOrContinuationToken[key].$) {
                        case "Edm.DateTime":
                            result[key] = new Date(entityOrContinuationToken[key]._)
                            break;
                        case "Edm.Int32":
                        case "Edm.Int64":
                            result[key] = parseInt(entityOrContinuationToken[key]._)
                            break;
                        case "Edm.Double":
                            result[key] = parseFloat(entityOrContinuationToken[key]._)
                            break;
                        default:
                            result[key] = entityOrContinuationToken[key]._;
                            break;
                    }
                } else {
                    result[key] = entityOrContinuationToken[key];
                }
            }
        }
        return result;
    }
}

export function toAzure<T extends AzureEntity>(entity: T): any;
export function toAzure(continuation: string): TableService.TableContinuationToken;
export function toAzure<T extends AzureEntity>(entityOrContinuation: T | string): any | TableService.TableContinuationToken {
    if (!entityOrContinuation) {
        return null;
    }
    if (isString(entityOrContinuation)) {
        return fromBase64<TableService.TableContinuationToken>(entityOrContinuation);
    } else {
        const entity: any = {
            ".metadata": (entityOrContinuation as any)[".metadata"] // cast to "any" type to be able to get properties by name
        };
        for (const key in entityOrContinuation) {
            if (key != ".metadata" && !Reflect.getMetadata(azureIgnoreMetadataKey, entityOrContinuation, key)) {
                entity[key] = {
                    _: entityOrContinuation[key],
                    $: Reflect.getMetadata(azureEdmMetadataKey, entityOrContinuation, key)
                };
            }
        }
        return entity;
    }
}

export class AzureEntity {
    PartitionKey: string;
    RowKey: string;
}

export class AzureQueryResult<T extends AzureEntity> {

    constructor(azureQueryResult: TableService.QueryEntitiesResult<any>, toT: (e: any) => T) {
        this.items = azureQueryResult.entries.map(toT);
        this.continuation = fromAzure(azureQueryResult.continuationToken);
    }

    items: T[];
    continuation: string;
}

export class AzureRepository {

    protected table: TableService;

    constructor(connectionString: string) {
        this.table = createTableService(connectionString);
    }

    protected ensureTable(tableName: string): Promise<void> {
        return new Promise<void>((res, rej) => {
            this.table.createTableIfNotExists(tableName, err => {
                if (err) {
                    rej(err);
                } else {
                    res();
                }
            });
        });
    }

    protected delete(tableName: string, partitionKey: string, rowKey: string): Promise<void> {
        return this.ensureTable(tableName)
            .then(() => {
                return new Promise<void>((res, rej) => {
                    const entity = {
                        PartitionKey: TableUtilities.entityGenerator.String(partitionKey),
                        RowKey: TableUtilities.entityGenerator.String(rowKey)
                    };
                    this.table.deleteEntity(tableName, entity, (err, response) => {
                        if (err && response.statusCode != 404) {
                            rej(err);
                        } else {
                            res();
                        }
                    })
                });
            });
    }

    deleteAll<T extends AzureEntity>(t: new () => T, tableName: string, query: TableQuery): Promise<void[]> {
        return this.selectAll(async (c) => await this.select(t, tableName, query, c))
            .then(list => {
                const batches: Promise<void>[] = [];
                while (list.length) {
                    const batch = new TableBatch();
                    list.splice(0, 100)
                        .forEach(e => batch.deleteEntity(toAzure(e)));
                    batches.push(new Promise<void>((res, rej) => {
                        this.table.executeBatch(tableName, batch, err => {
                            if (err) {
                                rej(err);
                            } else {
                                res();
                            }
                        });
                    }));
                }
                return Promise.all(batches);
            });
    }

    protected select<T extends AzureEntity>(t: new () => T, tableName: string, partitionKey: string, rowKey: string, throwIfNotFound?: boolean): Promise<T>;
    protected select<T extends AzureEntity>(t: new () => T, tableName: string, query: TableQuery, continuation: string): Promise<AzureQueryResult<T>>;
    protected select<T extends AzureEntity>(t: new () => T, tableName: string, partitionKeyOrQuery: string | TableQuery, rowKeyOrContinuation: string, throwIfNotFound = false): Promise<T | AzureQueryResult<T>> {
        return this.ensureTable(tableName)
            .then(() => {
                return new Promise<any | AzureQueryResult<any>>((res, rej) => {
                    if (isString(partitionKeyOrQuery)) {
                        this.table.retrieveEntity(tableName, partitionKeyOrQuery, rowKeyOrContinuation, (err, result, response) => {
                            if (err && (response.statusCode != 404 || !!throwIfNotFound)) {
                                rej(err);
                            } else {
                                res(fromAzure(result, t));
                            }
                        });
                    } else {
                        this.table.queryEntities(tableName, partitionKeyOrQuery, toAzure(rowKeyOrContinuation), (err, result) => {
                            if (err) {
                                rej(err);
                            } else {
                                res(new AzureQueryResult(result, e => fromAzure<T>(e, t)));
                            }
                        });
                    }
                });
            });
    }

    protected insertOrMerge<T extends AzureEntity>(tableName: string, entities: T[]): Promise<void[]>;
    protected insertOrMerge<T extends AzureEntity>(tableName: string, entity: T): Promise<void>;
    protected insertOrMerge<T extends AzureEntity>(tableName: string, entityOrArray: T[] | T): Promise<void[] | void> {
        return this.ensureTable(tableName)
            .then(() => {
                if (isArray(entityOrArray)) {
                    const batches: Promise<void>[] = [];
                    while (entityOrArray.length) {
                        const batch = new TableBatch();
                        entityOrArray.splice(0, 100)
                            .forEach(e => batch.insertOrMergeEntity(toAzure(e)));
                        batches.push(new Promise<void>((res, rej) => {
                            this.table.executeBatch(tableName, batch, err => {
                                if (err) {
                                    rej(err);
                                } else {
                                    res();
                                }
                            });
                        }));
                    }
                    return Promise.all(batches).then(() => { });
                } else {
                    return new Promise<void>((res, rej) => {
                        this.table.insertOrMergeEntity(tableName, toAzure(entityOrArray), err => {
                            if (err) {
                                rej(err);
                            } else {
                                res();
                            }
                        });
                    });
                }
            });
    }

    /**
     * Fetches all entities chunk by chunk.
     * @param query Performs actual query, must accept continuation
     */
    protected async selectAll<T extends AzureEntity>(query: (c: string) => Promise<AzureQueryResult<T>>): Promise<T[]> {
        let continuation: string = null;
        let items: T[] = [];

        do {
            const res = await query(continuation);
            continuation = res.continuation;
            items = items.concat(res.items);
        } while (!!continuation)

        return items;
    }

    validateContinuation(continuation: string) {
        try {
            return !continuation || toAzure(continuation) != null;
        } catch (e) {
            return false;
        }
    }
}