import { Settings } from "../common";
import { AzureEntity, AzureRepository, Ignore, Int64, Double } from "./azure";
import { TableQuery } from "azure-storage";

export class HistoryEntity extends AzureEntity {
    From: string;
    To: string;
    AssetId: string;

    @Double()
    Amount: number;

    @Int64()
    AmountInBaseUnit: number;

    Block: number;
    BlockTime: Date;
    TxId: string;
    ActionId: string;
    OperationId: string;
}

export class HistoryByTxIdEntity extends AzureEntity {

    @Ignore()
    get TxId(): string {
        return this.PartitionKey;
    }

    @Int64()
    Block: number;
}

export enum HistoryAddressCategory {
    From = "From",
    To = "To"
}

export class HistoryRepository extends AzureRepository {

    private historyTableName: string = "RippleHistory";
    private historyByTxIdTableName: string = "RippleHistoryByTxId";

    constructor(private settings: Settings) {
        super(settings.RippleJob.Azure.ConnectionString);
    }

    async upsert(from: string, to: string, assetId: string, amount: number, amountInBaseUnit: number,
        block: number, blockTime: Date, txId: string, actionId: string, operationId?: string) {

        const historyByTxIdEntity = new HistoryByTxIdEntity();
        historyByTxIdEntity.PartitionKey = txId;
        historyByTxIdEntity.RowKey = "";
        historyByTxIdEntity.Block = block;

        await this.insertOrMerge(this.historyByTxIdTableName, historyByTxIdEntity);

        const historyEntity = new HistoryEntity();
        historyEntity.PartitionKey = `${HistoryAddressCategory.From}_${from}`;
        historyEntity.RowKey = `${block}_${txId}_${actionId}`;
        historyEntity.From = from;
        historyEntity.To = to;
        historyEntity.Amount = amount;
        historyEntity.AmountInBaseUnit = amountInBaseUnit;
        historyEntity.AssetId = assetId;
        historyEntity.Block = block;
        historyEntity.BlockTime = blockTime;
        historyEntity.TxId = txId;
        historyEntity.ActionId = actionId;
        historyEntity.OperationId = operationId;

        await this.insertOrMerge(this.historyTableName, historyEntity);

        historyEntity.PartitionKey = `${HistoryAddressCategory.To}_${to}`;

        await this.insertOrMerge(this.historyTableName, historyEntity);
    }

    async get(category: HistoryAddressCategory, address: string, take = 100, afterHash: string = null): Promise<HistoryEntity[]> {
        let query = new TableQuery()
            .where("PartitionKey == ?", `${category}_${address}`)
            .top(take);

        if (!!afterHash) {
            const index = await this.select(HistoryByTxIdEntity, this.historyByTxIdTableName, afterHash, "");
            if (!!index) {
                query = query.and("RowKey > ?", index.Block);
            }
        }

        return await this.selectAll(async (c) => await this.select(HistoryEntity, this.historyTableName, query, c));
    }
}