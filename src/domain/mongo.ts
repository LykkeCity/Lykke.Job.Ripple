import { MongoClient, Db, MongoClientOptions } from "mongodb";

export abstract class MongoEntity<ID> {
    _id: ID;
}

export abstract class MongoRepository {

    private _db: Db;

    constructor(private connectionString: string, private user: string, private password: string, private database: string) {
    }

    protected async db(): Promise<Db> {
        if (this._db == null) {
            const options: MongoClientOptions = {
                useNewUrlParser: true,
                auth: !!this.user
                    ? { user: this.user, password: this.password }
                    : undefined
            };
            this._db = (await MongoClient.connect(this.connectionString, options)).db(this.database);
        }

        return this._db;
    }
}

export class MongoQueryResult<T> {
    constructor(public items: T[], public continuation: string) {
    }
}