module RyakDB.Recovery.CheckpointTask

open RyakDB.Transaction
open RyakDB.TransactionManager

module CheckpointTask =
    let createPeriodicCheckpoint txMgr =
        let tx = txMgr.NewTransaction false Serializable
        txMgr.CreateCheckpoint tx
        tx.Commit()

    let createMonitorCheckpoint txMgr txCount lastTxNo =
        let txNo = txMgr.GetNextTxNo()
        if txNo - lastTxNo > int64 txCount then
            let tx = txMgr.NewTransaction false Serializable
            txMgr.CreateCheckpoint tx
            tx.Commit()
            txNo
        else
            lastTxNo

let newPeriodicCheckpointTask txMgr (period: int32) =
    fun () ->
        while true do
            CheckpointTask.createPeriodicCheckpoint txMgr
            System.Threading.Thread.Sleep(period)

let newMonitorCheckpointTask txMgr (period: int32) txCount =
    let mutable lastTxNo = 0L
    fun () ->
        while true do
            lastTxNo <- CheckpointTask.createMonitorCheckpoint txMgr txCount lastTxNo
            System.Threading.Thread.Sleep(period)
