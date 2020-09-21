module RyakDB.Recovery.SystemRecovery

open RyakDB.Storage.Log
open RyakDB.Buffer.TransactionBuffer
open RyakDB.Recovery.RecoveryLog
open RyakDB.Transaction
open RyakDB.Recovery.TransactionRecoveryFinalize

module SystemRecovery =
    let redo tx recoveryLog =
        match recoveryLog with
        | IndexPageInsertRecord (n, ibid, branch, kt, sid, optlsn) ->
            let buffer = tx.Buffer.Pin ibid
            match optlsn with
            | Some l when l > buffer.LastLogSeqNo() ->
                if branch then insertBTreeBranchSlot tx kt ibid sid else insertBTreeLeafSlot tx kt ibid sid
            | _ -> ()
            tx.Buffer.Unpin buffer
        | IndexPageInsertClear (n, ibid, branch, kt, sid, _, optlsn) ->
            let buffer = tx.Buffer.Pin ibid
            match optlsn with
            | Some l when l > buffer.LastLogSeqNo() ->
                if branch then insertBTreeBranchSlot tx kt ibid sid else insertBTreeLeafSlot tx kt ibid sid
            | _ -> ()
            tx.Buffer.Unpin buffer
        | IndexPageDeleteRecord (n, ibid, branch, kt, sid, optlsn) ->
            let buffer = tx.Buffer.Pin ibid
            match optlsn with
            | Some l when l > buffer.LastLogSeqNo() ->
                if branch then deleteBTreeBranchSlot tx kt ibid sid else deleteBTreeLeafSlot tx kt ibid sid
            | _ -> ()
            tx.Buffer.Unpin buffer
        | IndexPageDeleteClear (n, ibid, branch, kt, sid, _, optlsn) ->
            let buffer = tx.Buffer.Pin ibid
            match optlsn with
            | Some l when l > buffer.LastLogSeqNo() ->
                if branch then deleteBTreeBranchSlot tx kt ibid sid else deleteBTreeLeafSlot tx kt ibid sid
            | _ -> ()
            tx.Buffer.Unpin buffer
        | SetValueRecord (_, bid, off, _, _, nv, _) ->
            let buffer = tx.Buffer.Pin bid
            buffer.SetVal off nv None
            tx.Buffer.Unpin buffer
        | SetValueClear (_, bid, off, _, _, nv, _, _) ->
            let buffer = tx.Buffer.Pin bid
            buffer.SetVal off nv None
            tx.Buffer.Unpin buffer
        | _ -> ()

    let recoverSystem fileMgr logMgr catalogMgr tx =
        let undoLogRecord = undo fileMgr logMgr catalogMgr tx

        let mutable uncompletedTxs = Set.empty
        let mutable finishedTxs = Set.empty
        let mutable redoRecords = []
        let mutable inCheckpoint = false

        logMgr.Records()
        |> Seq.map fromLogRecord
        |> Seq.iter (fun rlog ->
            if not (inCheckpoint) then
                match rlog with
                | CheckpointRecord(txNos = txNos) ->
                    txNos
                    |> List.filter (finishedTxs.Contains >> not)
                    |> List.iter (fun txNo -> uncompletedTxs <- uncompletedTxs.Add txNo)
                    inCheckpoint <- true
                | CommitRecord(txNo = txNo) ->
                    finishedTxs <- finishedTxs.Add txNo
                    redoRecords <- rlog :: redoRecords
                | RollbackRecord(txNo = txNo) ->
                    finishedTxs <- finishedTxs.Add txNo
                    redoRecords <- rlog :: redoRecords
                | StartRecord(txNo = txNo) when not (finishedTxs.Contains txNo) ->
                    uncompletedTxs <- uncompletedTxs.Add txNo
                    redoRecords <- rlog :: redoRecords
                | _ -> redoRecords <- rlog :: redoRecords)

        redoRecords |> List.iter (redo tx)

        uncompletedTxs <- uncompletedTxs.Remove tx.TransactionNo

        let mutable txUndoNextLogSeqNos = Map.empty

        logMgr.Records()
        |> Seq.map fromLogRecord
        |> Seq.iter (fun rlog ->
            if not (uncompletedTxs.IsEmpty) then
                let txNo = transactionNo rlog
                getLogSeqNo rlog
                |> Option.iter (fun lsn ->
                    if uncompletedTxs.Contains txNo
                       && txUndoNextLogSeqNos.ContainsKey txNo
                       && txUndoNextLogSeqNos.[txNo] > lsn then
                        match rlog with
                        | CommitRecord (_) -> ()
                        | RollbackRecord (_) -> ()
                        | StartRecord (_) -> uncompletedTxs <- uncompletedTxs.Remove txNo
                        | LogicalAbortRecord(logicalStartLogSeqNo = lsn) ->
                            undoLogRecord rlog
                            txUndoNextLogSeqNos <- txUndoNextLogSeqNos.Add(txNo, lsn)
                        | TableFileInsertEndRecord(logicalStartLogSeqNo = lsn) ->
                            undoLogRecord rlog
                            txUndoNextLogSeqNos <- txUndoNextLogSeqNos.Add(txNo, lsn)
                        | TableFileDeleteEndRecord(logicalStartLogSeqNo = lsn) ->
                            undoLogRecord rlog
                            txUndoNextLogSeqNos <- txUndoNextLogSeqNos.Add(txNo, lsn)
                        | IndexInsertEndRecord(logicalStartLogSeqNo = lsn) ->
                            undoLogRecord rlog
                            txUndoNextLogSeqNos <- txUndoNextLogSeqNos.Add(txNo, lsn)
                        | IndexDeleteEndRecord(logicalStartLogSeqNo = lsn) ->
                            undoLogRecord rlog
                            txUndoNextLogSeqNos <- txUndoNextLogSeqNos.Add(txNo, lsn)
                        | IndexPageInsertClear(undoNextLogSeqNo = lsn) ->
                            txUndoNextLogSeqNos <- txUndoNextLogSeqNos.Add(txNo, lsn)
                        | IndexPageDeleteClear(undoNextLogSeqNo = lsn) ->
                            txUndoNextLogSeqNos <- txUndoNextLogSeqNos.Add(txNo, lsn)
                        | SetValueClear(undoNextLogSeqNo = lsn) ->
                            txUndoNextLogSeqNos <- txUndoNextLogSeqNos.Add(txNo, lsn)
                        | _ -> undoLogRecord rlog))

    let recover fileMgr logMgr catalogMgr tx =
        recoverSystem fileMgr logMgr catalogMgr tx
        tx.Buffer.FlushAll()
        logMgr.RemoveAndCreateNewLog()
        tx.TransactionNo
        |> newStartRecord
        |> writeToLog logMgr
        |> ignore
