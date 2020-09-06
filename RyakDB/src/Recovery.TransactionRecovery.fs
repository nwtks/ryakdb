namespace RyakDB.Recovery.TransactionRecovery

open RyakDB.Storage
open RyakDB.Storage.Log
open RyakDB.Storage.BTree
open RyakDB.Table.Record
open RyakDB.Index
open RyakDB.Index.IndexFactory
open RyakDB.Buffer.BufferManager
open RyakDB.Recovery
open RyakDB.Recovery.RecoveryLog
open RyakDB.Transaction
open RyakDB.Catalog

module TransactionRecovery =
    let undo fileMgr logMgr (catalogMgr: CatalogManager) tx r =
        match r with
        | LogicalStartRecord (n, l) ->
            l
            |> Option.bind (tx.RecoveryMgr.LogLogicalAbort n)
            |> Option.iter logMgr.Flush
        | RecordFileInsertEndRecord (n, tn, bn, sid, start, _) ->
            catalogMgr.GetTableInfo tx tn
            |> Option.bind (fun ti ->
                let rf =
                    RecordFile.newRecordFile fileMgr tx true ti

                let rid =
                    RecordId.newBlockRecordId sid ti.FileName bn

                rf.DeleteByRecordId rid
                tx.RecoveryMgr.LogLogicalAbort n start)
            |> Option.iter logMgr.Flush
        | RecordFileDeleteEndRecord (n, tn, bn, sid, start, _) ->
            catalogMgr.GetTableInfo tx tn
            |> Option.bind (fun ti ->
                let rf =
                    RecordFile.newRecordFile fileMgr tx true ti

                let rid =
                    RecordId.newBlockRecordId sid ti.FileName bn

                rf.InsertByRecordId rid
                tx.RecoveryMgr.LogLogicalAbort n start)
            |> Option.iter logMgr.Flush
        | IndexInsertEndRecord (n, inm, sk, bn, sid, start, _) ->
            catalogMgr.GetIndexInfoByName tx inm
            |> Option.bind (fun ii ->
                let idx = IndexFactory.newIndex fileMgr tx ii

                let rid =
                    RecordId.newBlockRecordId sid ii.TableInfo.FileName bn

                idx.Delete false sk rid
                idx.Close()
                tx.RecoveryMgr.LogLogicalAbort n start)
            |> Option.iter logMgr.Flush
        | IndexDeleteEndRecord (n, inm, sk, bn, sid, start, _) ->
            catalogMgr.GetIndexInfoByName tx inm
            |> Option.bind (fun ii ->
                let idx = IndexFactory.newIndex fileMgr tx ii

                let rid =
                    RecordId.newBlockRecordId sid ii.TableInfo.FileName bn

                idx.Insert false sk rid
                idx.Close()
                tx.RecoveryMgr.LogLogicalAbort n start)
            |> Option.iter logMgr.Flush
        | IndexPageInsertRecord (n, ibid, dir, kt, sid, optlsn) ->
            let buffer = tx.BufferMgr.Pin ibid
            match optlsn with
            | Some lsn when lsn < buffer.LastLsn() ->
                if dir then BTreeDir.deleteASlot ibid kt sid else BTreeLeaf.deleteASlot ibid kt sid
                tx.RecoveryMgr.LogIndexPageDeletionClr dir n ibid kt sid lsn
                |> Option.iter logMgr.Flush
            | _ -> ()
            tx.BufferMgr.Unpin buffer
        | IndexPageDeleteRecord (n, ibid, dir, kt, sid, optlsn) ->
            let buffer = tx.BufferMgr.Pin ibid
            match optlsn with
            | Some lsn when lsn < buffer.LastLsn() ->
                if dir then BTreeDir.insertASlot ibid kt sid else BTreeLeaf.insertASlot ibid kt sid
                tx.RecoveryMgr.LogIndexPageInsertionClr dir n ibid kt sid lsn
                |> Option.iter logMgr.Flush
            | _ -> ()
            tx.BufferMgr.Unpin buffer
        | SetValueRecord (n, bid, off, _, v, _, optlsn) ->
            let buffer = tx.BufferMgr.Pin bid
            optlsn
            |> Option.bind (tx.RecoveryMgr.LogSetValClr n buffer off v)
            |> Option.iter logMgr.Flush
            buffer.SetVal off v None
            tx.BufferMgr.Unpin buffer
        | _ -> ()

    let rollback fileMgr logMgr catalogMgr tx =
        let undoLogRecord = undo fileMgr logMgr catalogMgr tx

        let mutable txUnDoNextLSN = None
        let mutable inStart = false
        logMgr.Records()
        |> Seq.map RecoveryLog.fromLogRecord
        |> Seq.filter (fun r -> RecoveryLog.transactionNumber r = tx.TransactionNumber)
        |> Seq.iter (fun r ->
            if not (inStart) then
                match txUnDoNextLSN, RecoveryLog.getLSN r with
                | None, _ ->
                    match r with
                    | StartRecord (_) -> inStart <- true
                    | LogicalAbortRecord(logicalStartLSN = lsn) ->
                        undoLogRecord r
                        txUnDoNextLSN <- Some lsn
                    | RecordFileInsertEndRecord(logicalStartLSN = lsn) ->
                        undoLogRecord r
                        txUnDoNextLSN <- Some lsn
                    | RecordFileDeleteEndRecord(logicalStartLSN = lsn) ->
                        undoLogRecord r
                        txUnDoNextLSN <- Some lsn
                    | IndexInsertEndRecord(logicalStartLSN = lsn) ->
                        undoLogRecord r
                        txUnDoNextLSN <- Some lsn
                    | IndexDeleteEndRecord(logicalStartLSN = lsn) ->
                        undoLogRecord r
                        txUnDoNextLSN <- Some lsn
                    | _ -> undoLogRecord r
                | Some (lsn1), Some (lsn2) when lsn1 > lsn2 ->
                    match r with
                    | StartRecord (_) -> inStart <- true
                    | LogicalAbortRecord(logicalStartLSN = lsn) ->
                        undoLogRecord r
                        txUnDoNextLSN <- Some lsn
                    | RecordFileInsertEndRecord(logicalStartLSN = lsn) ->
                        undoLogRecord r
                        txUnDoNextLSN <- Some lsn
                    | RecordFileDeleteEndRecord(logicalStartLSN = lsn) ->
                        undoLogRecord r
                        txUnDoNextLSN <- Some lsn
                    | IndexInsertEndRecord(logicalStartLSN = lsn) ->
                        undoLogRecord r
                        txUnDoNextLSN <- Some lsn
                    | IndexDeleteEndRecord(logicalStartLSN = lsn) ->
                        undoLogRecord r
                        txUnDoNextLSN <- Some lsn
                    | _ -> undoLogRecord r
                | _, _ -> ())

    let onCommit logMgr tx =
        if not (tx.ReadOnly) then
            RecoveryLog.newCommitRecord tx.TransactionNumber
            |> RecoveryLog.writeToLog logMgr
            |> logMgr.Flush

    let onRollback fileMgr logMgr catalogMgr tx =
        if not (tx.ReadOnly) then
            rollback fileMgr logMgr catalogMgr tx
            RecoveryLog.newRollbackRecord tx.TransactionNumber
            |> RecoveryLog.writeToLog logMgr
            |> logMgr.Flush
