module RyakDB.Recovery.CheckpointTask

open RyakDB.TransactionService

module CheckpointTask =
    let createPeriodicCheckpoint transactionService = transactionService.CreateCheckpoint()

    let createMonitorCheckpoint transactionService txCount lastTxNo =
        let txNo = transactionService.GetNextTxNo()
        if txNo - lastTxNo > int64 txCount then
            transactionService.CreateCheckpoint()
            txNo
        else
            lastTxNo

let newPeriodicCheckpointTask transactionService (period: int32) =
    let rec checkpoint () =
        CheckpointTask.createPeriodicCheckpoint transactionService
        System.Threading.Thread.Sleep(period)
        checkpoint ()

    checkpoint

let newMonitorCheckpointTask transactionService (period: int32) txCount =
    let mutable lastTxNo = 0L

    let rec checkpoint () =
        lastTxNo <- CheckpointTask.createMonitorCheckpoint transactionService txCount lastTxNo
        System.Threading.Thread.Sleep(period)
        checkpoint ()

    checkpoint
