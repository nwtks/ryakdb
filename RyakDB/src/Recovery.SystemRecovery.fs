module RyakDB.Recovery.SystemRecovery

open RyakDB.Storage.Log
open RyakDB.Storage.BTree
open RyakDB.Buffer.BufferManager
open RyakDB.Recovery.RecoveryLog
open RyakDB.Recovery.TransactionRecoveryFinalize
open RyakDB.Transaction

module SystemRecovery =
    let redo bufferMgr r =
        match r with
        | IndexPageInsertRecord (n, ibid, dir, kt, sid, optlsn) ->
            let buffer = bufferMgr.Pin ibid
            match optlsn with
            | Some l when l > buffer.LastLsn() ->
                if dir then BTreeDir.insertASlot ibid kt sid else BTreeLeaf.insertASlot ibid kt sid
            | _ -> ()
            bufferMgr.Unpin buffer
        | IndexPageInsertClr (n, ibid, dir, kt, sid, _, optlsn) ->
            let buffer = bufferMgr.Pin ibid
            match optlsn with
            | Some l when l > buffer.LastLsn() ->
                if dir then BTreeDir.insertASlot ibid kt sid else BTreeLeaf.insertASlot ibid kt sid
            | _ -> ()
            bufferMgr.Unpin buffer
        | IndexPageDeleteRecord (n, ibid, dir, kt, sid, optlsn) ->
            let buffer = bufferMgr.Pin ibid
            match optlsn with
            | Some l when l > buffer.LastLsn() ->
                if dir then BTreeDir.deleteASlot ibid kt sid else BTreeLeaf.deleteASlot ibid kt sid
            | _ -> ()
            bufferMgr.Unpin buffer
        | IndexPageDeleteClr (n, ibid, dir, kt, sid, _, optlsn) ->
            let buffer = bufferMgr.Pin ibid
            match optlsn with
            | Some l when l > buffer.LastLsn() ->
                if dir then BTreeDir.deleteASlot ibid kt sid else BTreeLeaf.deleteASlot ibid kt sid
            | _ -> ()
            bufferMgr.Unpin buffer
        | SetValueRecord (_, bid, off, _, _, nv, _) ->
            let buffer = bufferMgr.Pin bid
            buffer.SetVal off nv None
            bufferMgr.Unpin buffer
        | SetValueClr (_, bid, off, _, _, nv, _, _) ->
            let buffer = bufferMgr.Pin bid
            buffer.SetVal off nv None
            bufferMgr.Unpin buffer
        | _ -> ()

    let recoverSystem fileMgr logMgr catalogMgr tx =
        let undoLogRecord =
            TransactionRecoveryFinalize.undo fileMgr logMgr catalogMgr tx

        let mutable unCompletedTxs = Set.empty
        let mutable finishedTxs = Set.empty
        let mutable redoRecords = []
        let mutable inCheckpoint = false

        logMgr.Records()
        |> Seq.map RecoveryLog.fromLogRecord
        |> Seq.iter (fun r ->
            if not (inCheckpoint) then
                match r with
                | CheckpointRecord(txNos = txNos) ->
                    txNos
                    |> List.filter (finishedTxs.Contains >> not)
                    |> List.iter (fun txNo -> unCompletedTxs <- unCompletedTxs.Add txNo)
                    inCheckpoint <- true
                | CommitRecord(txNo = txNo) ->
                    finishedTxs <- finishedTxs.Add txNo
                    redoRecords <- r :: redoRecords
                | RollbackRecord(txNo = txNo) ->
                    finishedTxs <- finishedTxs.Add txNo
                    redoRecords <- r :: redoRecords
                | StartRecord(txNo = txNo) when not (finishedTxs.Contains txNo) ->
                    unCompletedTxs <- unCompletedTxs.Add txNo
                    redoRecords <- r :: redoRecords
                | _ -> redoRecords <- r :: redoRecords)
        redoRecords |> List.iter (redo tx.BufferMgr)
        unCompletedTxs <- unCompletedTxs.Remove tx.TransactionNumber
        let mutable txUnDoNextLSN = Map.empty
        logMgr.Records()
        |> Seq.map RecoveryLog.fromLogRecord
        |> Seq.iter (fun r ->
            if not (unCompletedTxs.IsEmpty) then
                let txNo = RecoveryLog.transactionNumber r
                RecoveryLog.getLSN r
                |> Option.iter (fun lsn ->
                    if unCompletedTxs.Contains txNo
                       && txUnDoNextLSN.ContainsKey txNo
                       && txUnDoNextLSN.[txNo] > lsn then
                        match r with
                        | CommitRecord (_) -> ()
                        | RollbackRecord (_) -> ()
                        | StartRecord (_) -> unCompletedTxs <- unCompletedTxs.Remove txNo
                        | LogicalAbortRecord(logicalStartLSN = lsn) ->
                            undoLogRecord r
                            txUnDoNextLSN <- txUnDoNextLSN.Add(txNo, lsn)
                        | RecordFileInsertEndRecord(logicalStartLSN = lsn) ->
                            undoLogRecord r
                            txUnDoNextLSN <- txUnDoNextLSN.Add(txNo, lsn)
                        | RecordFileDeleteEndRecord(logicalStartLSN = lsn) ->
                            undoLogRecord r
                            txUnDoNextLSN <- txUnDoNextLSN.Add(txNo, lsn)
                        | IndexInsertEndRecord(logicalStartLSN = lsn) ->
                            undoLogRecord r
                            txUnDoNextLSN <- txUnDoNextLSN.Add(txNo, lsn)
                        | IndexDeleteEndRecord(logicalStartLSN = lsn) ->
                            undoLogRecord r
                            txUnDoNextLSN <- txUnDoNextLSN.Add(txNo, lsn)
                        | IndexPageInsertClr(undoNextLSN = lsn) -> txUnDoNextLSN <- txUnDoNextLSN.Add(txNo, lsn)
                        | IndexPageDeleteClr(undoNextLSN = lsn) -> txUnDoNextLSN <- txUnDoNextLSN.Add(txNo, lsn)
                        | SetValueClr(undoNextLSN = lsn) -> txUnDoNextLSN <- txUnDoNextLSN.Add(txNo, lsn)
                        | _ -> undoLogRecord r))

    let recover fileMgr logMgr catalogMgr tx =
        recoverSystem fileMgr logMgr catalogMgr tx
        tx.BufferMgr.FlushAll()
        logMgr.RemoveAndCreateNewLog()
        tx.TransactionNumber
        |> RecoveryLog.newStartRecord
        |> RecoveryLog.writeToLog logMgr
        |> ignore
