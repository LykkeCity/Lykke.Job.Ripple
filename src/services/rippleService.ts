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
        const lastProcessedLedger = Number(params && params.LastProcessedLedger) || 0;
        const serverInfo = await this.api.getServerInfo();
        const lastValidatedLedger = serverInfo.validatedLedger.ledgerVersion - (this.settings.RippleJob.Confirmations || 0);
        const availableHistory = serverInfo.completeLedgers.split(",").map(i => i.split("-"));

        this.log(LogLevel.info, "Params", {
            lastProcessedLedger,
            lastValidatedLedger
        });

        if (lastProcessedLedger >= lastValidatedLedger) {
            // nothing to do here, all actions already processed
            return {
                lastProcessedLedger,
                lastValidatedLedger
            };
        }        
        
        // there are must be an available interval of ledger 
        // history that contains our required interval
        if (availableHistory.every(i => parseInt(i[0]) > lastProcessedLedger || parseInt(i[1]) < lastValidatedLedger)) {
            throw new Error(`History for interval [${lastProcessedLedger}-${lastValidatedLedger}] is not available. Available history [${serverInfo.completeLedgers}].`);
        }

        const history = await this.api.getTransactions(this.settings.RippleJob.HotWalletAddress, {
            minLedgerVersion: lastProcessedLedger + 1,
            maxLedgerVersion: lastValidatedLedger,
            earliestFirst: true,
            types: [
                "payment"
            ],
            limit: 100
        });

        this.log(LogLevel.info, "History (ids)", history.map(h => h.id));

        for (const tx of history) {
            const block = tx.outcome.ledgerVersion * 10;
            const blockTime = tx.outcome.timestamp && isoUTC(tx.outcome.timestamp) || new Date();
            const txId = tx.id;

            const operationId = await this.operationRepository.getOperationIdByTxId(txId);
            if (!!operationId) {

                await this.log(LogLevel.info, `Operation detected`, {
                    operationId, tx
                });

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
                            operationId, ...bc, assetId: operation.AssetId
                        });
                    }

                    // upsert history of operation action
                    await this.historyRepository.upsert(operation.FromAddress, operation.ToAddress, operation.AssetId,
                        operation.Amount, operation.AmountInBaseUnit, block, blockTime, txId, "", operationId);

                    // set operation state to completed
                    await this.operationRepository.update(operationId, { completionTime: new Date(), blockTime, block });
                    await this.log(LogLevel.info, "Operation completed", operationId);
                } else {
                    await this.operationRepository.update(operationId, { failTime: new Date(), error: tx.outcome.result, errorCode: ErrorCode.unknown });
                    await this.log(LogLevel.warning, "Operation failed", {
                        operationId, result: tx.outcome.result
                    });
                }
            } else {

                await this.log(LogLevel.info, `External transaction detected`, { tx });

                // this is external transaction, so use blockchain 
                // data to record balance changes and history

                const payment = tx as FormattedPaymentTransaction;
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
                        await this.historyRepository.upsert(from, to, assetId, amount, amountInBaseUnit,
                            block, blockTime, txId, "");

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
                                txId, ...bc, assetId
                            });
                        }

                        await this.log(LogLevel.info, "Transaction processed", txId);
                    } else {
                        await this.log(LogLevel.warning, "Not tracked currency", assetAmount.currency);
                    }
                } else {
                    await this.log(LogLevel.error, "No deliveredAmount", payment);
                }
            }
        }

        return {
            lastProcessedLedger,
            lastValidatedLedger
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
            if (!!operation && !operation.CompletionTime && !operation.FailTime) {
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