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

    async handleActions(): Promise<number> {
        const params = await this.paramsRepository.get();
        const lastProcessedLedger = (params && params.LastProcessedLedger) || 0;
        const lastClosedLedger = await this.api.getLedgerVersion();
        const history = await this.api.getTransactions(this.settings.RippleJob.HotWalletAddress, {
            minLedgerVersion: lastProcessedLedger,
            maxLedgerVersion: lastClosedLedger
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
                            failTime: new Date(),
                            error: tx.outcome.result,
                            errorCode: tx.outcome.result == "tecEXPIRED"
                                ? ErrorCode.buildingShouldBeRepeated
                                : ErrorCode.unknown
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

        return lastClosedLedger;
    }

    async handleExpired(lastClosedLedger: number) {

        // use lastClosedLedger from handleActions() to not mark not yet processed transactions as expired

        const params = await this.paramsRepository.get();
        const lastProcessedLedger = (params && params.LastProcessedLedger) || 0;
        const presumablyExpired = await this.operationRepository.geOperationIdByExpiration(lastProcessedLedger, lastClosedLedger);

        // mark expired operations as failed, if any

        for (let i = 0; i < presumablyExpired.length; i++) {
            const operation = await this.operationRepository.get(presumablyExpired[i])
            if (!!operation && !operation.isCompleted() && !operation.isFailed()) {
                const operationId = operation.OperationId;

                // mark operation as failed
                await this.operationRepository.update(operationId, {
                    errorCode: ErrorCode.buildingShouldBeRepeated,
                    error: "Transaction expired",
                    failTime: new Date()
                });

                // TODO: cancel balance changes?
            }
        }

        // update state
        await this.paramsRepository.upsert(lastClosedLedger);
    }
}