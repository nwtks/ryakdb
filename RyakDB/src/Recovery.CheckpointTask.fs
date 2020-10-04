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
    let rec checkpoint () =
        CheckpointTask.createPeriodicCheckpoint txMgr
        System.Threading.Thread.Sleep(period)
        checkpoint ()

    checkpoint

let newMonitorCheckpointTask txMgr (period: int32) txCount =
    let mutable lastTxNo = 0L

    let rec checkpoint () =
        lastTxNo <- CheckpointTask.createMonitorCheckpoint txMgr txCount lastTxNo
        System.Threading.Thread.Sleep(period)
        checkpoint ()

    checkpoint
