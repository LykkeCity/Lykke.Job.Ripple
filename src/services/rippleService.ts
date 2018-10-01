import { Settings, ADDRESS_SEPARATOR, isoUTC } from "../common";
import { LogService, LogLevel } from "./logService";
import { AssetRepository } from "../domain/assets";
import { OperationRepository, ErrorCode } from "../domain/operations";
import { ParamsRepository } from "../domain/params";
import { BalanceRepository } from "../domain/balances";
import { HistoryRepository } from "../domain/history";
import { RippleAPI } from "ripple-lib";
import { FormattedPaymentTransaction } from "ripple-lib/dist/npm/transaction/types";
import { Amount } from "ripple-lib/dist/npm/common/types/objects";

export interface ProcessedInterval {
    lastProcessedLedger: number,
    lastValidatedLedger: number
}

export class RippleService {

    private paramsRepository: ParamsRepository;
    private balanceRepository: BalanceRepository;
    private assetRepository: AssetRepository;
    private operationRepository: OperationRepository;
    private historyRepository: HistoryRepository;
    private log: (level: LogLevel, message: string, context?: any) => Promise<void>;
    private api: RippleAPI;

    constructor(private settings: Settings, private logService: LogService) {
        this.paramsRepository = new ParamsRepository(settings);
        this.balanceRepository = new BalanceRepository(settings);
        this.assetRepository = new AssetRepository(settings);
        this.operationRepository = new OperationRepository(settings);
        this.historyRepository = new HistoryRepository(settings);
        this.log = (l, m, c) => this.logService.write(l, RippleService.name, this.handleActions.name, m, JSON.stringify(c));
        this.api = new RippleAPI({
            server: settings.RippleJob.Ripple.Url
        });
    }

    async handleActions(): Promise<ProcessedInterval> {
        if (!this.api.isConnected()) {
            await this.api.connect();
        }

        const params = await this.paramsRepository.get();
        const lastProcessedLedger = (params && params.LastProcessedLedger) || 0;
        const lastValidatedLedger = await this.api.getLedgerVersion();
        const serverInfo = await this.api.getServerInfo();
        const availableHistory = serverInfo.completeLedgers.split(",").map(i => i.split("-"));
        
        // there are must be an available interval of ledger 
        // history that contains our required interval
        if (availableHistory.every(i => parseInt(i[0]) > lastProcessedLedger || parseInt(i[1]) < lastValidatedLedger)) {
            throw new Error(`History for interval [${lastProcessedLedger}-${lastValidatedLedger}] is not available. Available history [${serverInfo.completeLedgers}].`);
        }

        const history = await this.api.getTransactions(this.settings.RippleJob.HotWalletAddress, {
            minLedgerVersion: lastProcessedLedger,
            maxLedgerVersion: lastValidatedLedger
        });

        history.sort((a, b) => a.sequence - b.sequence);

        for (const tx of history) {
            const payment = tx.type.toLowerCase() == "payment" && tx as FormattedPaymentTransaction;
            const block = tx.outcome.ledgerVersion * 10;
            const blockTime = tx.outcome.timestamp && isoUTC(tx.outcome.timestamp) || new Date();
            const txId = tx.id;

            await this.log(LogLevel.info, `${tx.type} transaction ${!!payment ? "detected" : "skipped"}`, {
                Account: this.settings.RippleJob.HotWalletAddress,
                Seq: tx.sequence
            });

            if (!!payment) {
                const operationId = await this.operationRepository.getOperationIdByTxId(txId);
                if (!!operationId) {

                    // this is our operation, so use our data 
                    // to record balance changes and history

                    if (tx.outcome.result == "tesSUCCESS") {
                        const operation = await this.operationRepository.get(operationId);
                        const balanceChanges = [
                            { address: operation.FromAddress, affix: -operation.Amount, affixInBaseUnit: -operation.AmountInBaseUnit },
                            { address: operation.ToAddress, affix: operation.Amount, affixInBaseUnit: operation.AmountInBaseUnit }
                        ];

                        for (const bc of balanceChanges) {
                            await this.balanceRepository.upsert(bc.address, operation.AssetId, operationId, bc.affix, bc.affixInBaseUnit, block);
                            await this.log(LogLevel.info, "Balance change recorded", {
                                ...bc, assetId: operation.AssetId, txId
                            });
                        }

                        // upsert history of operation action
                        await this.historyRepository.upsert(operation.FromAddress, operation.ToAddress, operation.AssetId,
                            operation.Amount, operation.AmountInBaseUnit, block, blockTime, txId, operation.RowKey, operationId);

                        // set operation state to completed
                        await this.operationRepository.update(operationId, { completionTime: new Date(), blockTime, block });
                    } else {
                        await this.operationRepository.update(operationId, {
                            errorCode: ErrorCode.unknown,
                            error: tx.outcome.result,
                            failTime: new Date()
                        });
                    }
                } else {

                    // this is external transaction, so use blockchain 
                    // data to record balance changes and history

                    const assetAmount = (payment.outcome as any).deliveredAmount as Amount;
                    if (!!assetAmount) {
                        const asset = await this.assetRepository.get(assetAmount.currency);
                        if (!!asset) {
                            const assetId = asset.AssetId;
                            const amount = parseFloat(assetAmount.value);
                            const amountInBaseUnit = asset.toBaseUnit(amount);
                            const from = !!payment.specification.source.tag
                                ? payment.specification.source.address + ADDRESS_SEPARATOR + payment.specification.source.tag
                                : payment.specification.source.address;
                            const to = !!payment.specification.destination.tag
                                ? payment.specification.destination.address + ADDRESS_SEPARATOR + payment.specification.destination.tag
                                : payment.specification.destination.address;

                            // record history
                            await this.historyRepository.upsert(from, to, assetId, amount, amountInBaseUnit, block, blockTime, txId, operationId);
                            await this.log(LogLevel.info, "Payment recorded", payment);

                            // record balance changes;
                            // actually source amount may be in another currency,
                            // but we are interested in deliveries only
                            const balanceChanges = [
                                { address: from, affix: -amount, affixInBaseUnit: -amountInBaseUnit },
                                { address: to, affix: amount, affixInBaseUnit: amountInBaseUnit }
                            ];
                            for (const bc of balanceChanges) {
                                await this.balanceRepository.upsert(bc.address, assetId, txId, bc.affix, bc.affixInBaseUnit, block);
                                await this.log(LogLevel.info, "Balance change recorded", {
                                    ...bc, assetId, txId
                                });
                            }
                        } else {
                            await this.log(LogLevel.warning, "Not tracked currency", assetAmount.currency);
                        }
                    } else {
                        await this.log(LogLevel.warning, "No deliveredAmount", payment);
                    }
                }
            }
        }

        return {
            lastProcessedLedger: lastProcessedLedger,
            lastValidatedLedger: lastValidatedLedger
        };
    }

    async handleExpired(interval: ProcessedInterval) {

        // use interval of processed ledgers from handleActions() 
        // to not mark not yet processed transactions as expired
        const presumablyExpired = await this.operationRepository.geOperationIdByExpiration(
            interval.lastProcessedLedger,
            interval.lastValidatedLedger
        );

        // mark expired operations as failed, if any
        for (let i = 0; i < presumablyExpired.length; i++) {
            const operation = await this.operationRepository.get(presumablyExpired[i])
            if (!!operation && !operation.isCompleted() && !operation.isFailed()) {
                await this.operationRepository.update(operation.OperationId, {
                    errorCode: ErrorCode.buildingShouldBeRepeated,
                    error: "Transaction expired",
                    failTime: new Date()
                });
            }
        }

        // save last processed ledger number
        await this.paramsRepository.upsert(interval.lastValidatedLedger);
    }
}