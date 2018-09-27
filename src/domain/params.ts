import { Settings } from "../common";
import { AzureEntity, Int64, AzureRepository } from "./azure";

export class ParamsEntity extends AzureEntity {
    @Int64()
    LastProcessedLedger: number;
}

export class ParamsRepository extends AzureRepository {

    private tableName: string = "RippleParams";
    private partitionKey = "Params";
    private rowKey = "";

    constructor(private settings: Settings) {
        super(settings.RippleJob.Azure.ConnectionString);
    }

    async get(): Promise<ParamsEntity> {
        return await this.select(ParamsEntity, this.tableName, this.partitionKey, this.rowKey);
    }

    async upsert(lastProcessedLedger: number) {
        const entity = new ParamsEntity();
        entity.PartitionKey = this.partitionKey;
        entity.RowKey = this.rowKey;
        entity.LastProcessedLedger = lastProcessedLedger;

        await this.insertOrMerge(this.tableName, entity);
    }
}