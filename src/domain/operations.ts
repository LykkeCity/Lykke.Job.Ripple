import { TableQuery } from "azure-storage";
import { Settings } from "../common";
import { AzureRepository, AzureEntity, Ignore, Int64, Double } from "./azure";

export enum ErrorCode {
    unknown = "unknown",
    amountIsTooSmall = "amountIsTooSmall",
    notEnoughBalance = "notEnoughBalance",
    buildingShouldBeRepeated = "buildingShouldBeRepeated"
}

export class OperationEntity extends AzureEntity {
    @Ignore()
    get OperationId(): string {
        return this.PartitionKey;
    }

    FromAddress: string;
    ToAddress: string;
    AssetId: string;

    @Double()
    Amount: number;

    @Int64()
    AmountInBaseUnit: number;

    BuildTime: Date;
    SendTime: Date;
    TxId: string;
    CompletionTime: Date;
    BlockTime: Date;

    @Int64()
    Block: number;

    @Int64()
    Expiration: number;

    FailTime: Date;
    Error: string;
    ErrorCode: ErrorCode;
    DeleteTime: Date;

    isCompleted(): boolean {
        return !!this.CompletionTime;
    }

    isFailed(): boolean {
        return !!this.FailTime;
    }

    isSent(): boolean {
        return !!this.SendTime;
    }
}

export class OperationByExpirationEntity extends AzureEntity {
    @Ignore()
    get Expiration(): number {
        return parseInt(this.PartitionKey);
    }

    @Ignore()
    get OperationId(): string {
        return this.RowKey;
    }
}

export class OperationByTxIdEntity extends AzureEntity {
    @Ignore()
    get TxId(): string {
        return this.PartitionKey;
    }

    OperationId: string;
}

export class OperationRepository extends AzureRepository {

    private operationTableName: string = "RippleOperations";
    private operationByExpirationTableName: string = "RippleOperationsByExpiration";
    private operationByTxIdTableName: string = "RippleOperationsByTxId";

    constructor(private settings: Settings) {
        super(settings.RippleJob.Azure.ConnectionString);
    }

    async upsert(operationId: string, assetId: string, fromAddress: string, toAddress: string, amount: number, amountInBaseUnit: number, expiration?: number) {

        const operationEntity = new OperationEntity();
        operationEntity.PartitionKey = operationId;
        operationEntity.RowKey = "";
        operationEntity.AssetId = assetId;
        operationEntity.FromAddress = fromAddress;
        operationEntity.ToAddress = toAddress;
        operationEntity.Amount = amount;
        operationEntity.AmountInBaseUnit = amountInBaseUnit;
        operationEntity.BuildTime = new Date();
        operationEntity.Expiration = expiration;

        await this.insertOrMerge(this.operationTableName, operationEntity);

        if (!!expiration) {
            const operationByExpiryTimeEntity = new OperationByExpirationEntity();
            operationByExpiryTimeEntity.PartitionKey = expiration.toFixed();
            operationByExpiryTimeEntity.RowKey = operationId;
            await this.insertOrMerge(this.operationByExpirationTableName, operationByExpiryTimeEntity);
        }
    }

    async update(operationId: string,
        operation: { sendTime?: Date, completionTime?: Date, failTime?: Date, deleteTime?: Date, txId?: string, blockTime?: Date, block?: number, error?: string, errorCode?: ErrorCode }) {

        // update transaction index
        if (!!operation.txId) {
            const operationByTxIdEntity = new OperationByTxIdEntity();
            operationByTxIdEntity.PartitionKey = operation.txId;
            operationByTxIdEntity.RowKey = "";
            operationByTxIdEntity.OperationId = operationId;

            await this.insertOrMerge(this.operationByTxIdTableName, operationByTxIdEntity);
        }

        // update transaction
        const operationEntity = new OperationEntity();
        operationEntity.PartitionKey = operationId;
        operationEntity.RowKey = "";
        operationEntity.SendTime = operation.sendTime;
        operationEntity.CompletionTime = operation.completionTime;
        operationEntity.FailTime = operation.failTime;
        operationEntity.DeleteTime = operation.deleteTime;
        operationEntity.TxId = operation.txId;
        operationEntity.BlockTime = operation.blockTime;
        operationEntity.Block = operation.block;
        operationEntity.Error = operation.error;
        operationEntity.ErrorCode = operation.errorCode;

        await this.insertOrMerge(this.operationTableName, operationEntity);
    }

    async get(operationId: string): Promise<OperationEntity> {
        return await this.select(OperationEntity, this.operationTableName, operationId, "");
    }

    async getOperationIdByTxId(txId: string) {
        const operationByTxIdEntity = await this.select(OperationByTxIdEntity, this.operationByTxIdTableName, txId, "");
        if (!!operationByTxIdEntity) {
            return operationByTxIdEntity.OperationId;
        } else {
            return null;
        }
    }

    async geOperationIdByExpiration(from: number, to: number): Promise<string[]> {
        const query = new TableQuery()
            .where("PartitionKey > ? and PartitionKey <= ?", from.toFixed(), to.toFixed());

        const entities = await this.selectAll(async (c) => this.select(OperationByExpirationEntity, this.operationByExpirationTableName, query, c));

        return entities.map(e => e.OperationId);
    }
}