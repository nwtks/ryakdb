module RyakDB.Recovery.SystemRecovery

open RyakDB.Storage.Log
open RyakDB.Index.BTreeIndex
open RyakDB.Buffer.TransactionBuffer
open RyakDB.Recovery
open RyakDB.Recovery.RecoveryLog
open RyakDB.Transaction

module SystemRecovery =
    let redo txBuffer recoveryLog =
        match recoveryLog with
        | IndexPageInsertRecord (n, ibid, dir, kt, sid, optlsn) ->
            let buffer = txBuffer.Pin ibid
            match optlsn with
            | Some l when l > buffer.LastLogSeqNo() ->
                if dir then BTreeDir.insertASlot ibid kt sid else BTreeLeaf.insertASlot ibid kt sid
            | _ -> ()
            txBuffer.Unpin buffer
        | IndexPageInsertClr (n, ibid, dir, kt, sid, _, optlsn) ->
            let buffer = txBuffer.Pin ibid
            match optlsn with
            | Some l when l > buffer.LastLogSeqNo() ->
                if dir then BTreeDir.insertASlot ibid kt sid else BTreeLeaf.insertASlot ibid kt sid
            | _ -> ()
            txBuffer.Unpin buffer
        | IndexPageDeleteRecord (n, ibid, dir, kt, sid, optlsn) ->
            let buffer = txBuffer.Pin ibid
            match optlsn with
            | Some l when l > buffer.LastLogSeqNo() ->
                if dir then BTreeDir.deleteASlot ibid kt sid else BTreeLeaf.deleteASlot ibid kt sid
            | _ -> ()
            txBuffer.Unpin buffer
        | IndexPageDeleteClr (n, ibid, dir, kt, sid, _, optlsn) ->
            let buffer = txBuffer.Pin ibid
            match optlsn with
            | Some l when l > buffer.LastLogSeqNo() ->
                if dir then BTreeDir.deleteASlot ibid kt sid else BTreeLeaf.deleteASlot ibid kt sid
            | _ -> ()
            txBuffer.Unpin buffer
        | SetValueRecord (_, bid, off, _, _, nv, _) ->
            let buffer = txBuffer.Pin bid
            buffer.SetVal off nv None
            txBuffer.Unpin buffer
        | SetValueClr (_, bid, off, _, _, nv, _, _) ->
            let buffer = txBuffer.Pin bid
            buffer.SetVal off nv None
            txBuffer.Unpin buffer
        | _ -> ()

    let recoverSystem fileMgr logMgr catalogMgr tx =
        let undoLogRecord =
            TransactionRecoveryFinalize.undo fileMgr logMgr catalogMgr tx

        let mutable unCompletedTxs = Set.empty
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
                    |> List.iter (fun txNo -> unCompletedTxs <- unCompletedTxs.Add txNo)
                    inCheckpoint <- true
                | CommitRecord(txNo = txNo) ->
                    finishedTxs <- finishedTxs.Add txNo
                    redoRecords <- rlog :: redoRecords
                | RollbackRecord(txNo = txNo) ->
                    finishedTxs <- finishedTxs.Add txNo
                    redoRecords <- rlog :: redoRecords
                | StartRecord(txNo = txNo) when not (finishedTxs.Contains txNo) ->
                    unCompletedTxs <- unCompletedTxs.Add txNo
                    redoRecords <- rlog :: redoRecords
                | _ -> redoRecords <- rlog :: redoRecords)

        redoRecords |> List.iter (redo tx.Buffer)

        unCompletedTxs <- unCompletedTxs.Remove tx.TransactionNo

        let mutable txUnDoNextLSN = Map.empty
        logMgr.Records()
        |> Seq.map fromLogRecord
        |> Seq.iter (fun rlog ->
            if not (unCompletedTxs.IsEmpty) then
                let txNo = RecoveryLog.transactionNo rlog
                RecoveryLog.getLogSeqNo rlog
                |> Option.iter (fun lsn ->
                    if unCompletedTxs.Contains txNo
                       && txUnDoNextLSN.ContainsKey txNo
                       && txUnDoNextLSN.[txNo] > lsn then
                        match rlog with
                        | CommitRecord (_) -> ()
                        | RollbackRecord (_) -> ()
                        | StartRecord (_) -> unCompletedTxs <- unCompletedTxs.Remove txNo
                        | LogicalAbortRecord(logicalStartLSN = lsn) ->
                            undoLogRecord rlog
                            txUnDoNextLSN <- txUnDoNextLSN.Add(txNo, lsn)
                        | TableFileInsertEndRecord(logicalStartLSN = lsn) ->
                            undoLogRecord rlog
                            txUnDoNextLSN <- txUnDoNextLSN.Add(txNo, lsn)
                        | TableFileDeleteEndRecord(logicalStartLSN = lsn) ->
                            undoLogRecord rlog
                            txUnDoNextLSN <- txUnDoNextLSN.Add(txNo, lsn)
                        | IndexInsertEndRecord(logicalStartLSN = lsn) ->
                            undoLogRecord rlog
                            txUnDoNextLSN <- txUnDoNextLSN.Add(txNo, lsn)
                        | IndexDeleteEndRecord(logicalStartLSN = lsn) ->
                            undoLogRecord rlog
                            txUnDoNextLSN <- txUnDoNextLSN.Add(txNo, lsn)
                        | IndexPageInsertClr(undoNextLSN = lsn) -> txUnDoNextLSN <- txUnDoNextLSN.Add(txNo, lsn)
                        | IndexPageDeleteClr(undoNextLSN = lsn) -> txUnDoNextLSN <- txUnDoNextLSN.Add(txNo, lsn)
                        | SetValueClr(undoNextLSN = lsn) -> txUnDoNextLSN <- txUnDoNextLSN.Add(txNo, lsn)
                        | _ -> undoLogRecord rlog))

    let recover fileMgr logMgr catalogMgr tx =
        recoverSystem fileMgr logMgr catalogMgr tx
        tx.Buffer.FlushAll()
        logMgr.RemoveAndCreateNewLog()
        tx.TransactionNo
        |> RecoveryLog.newStartRecord
        |> RecoveryLog.writeToLog logMgr
        |> ignore
