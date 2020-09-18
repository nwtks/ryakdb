module RyakDB.Recovery.TransactionRecoveryFinalize

open RyakDB.Storage.Log
open RyakDB.Index.BTreeIndex
open RyakDB.Table
open RyakDB.Table.TableFile
open RyakDB.Index
open RyakDB.Index.IndexFactory
open RyakDB.Buffer.TransactionBuffer
open RyakDB.Recovery.RecoveryLog
open RyakDB.Transaction
open RyakDB.Catalog

let undo fileMgr logMgr (catalogMgr: CatalogManager) (tx: Transaction) recoveryLog =
    match recoveryLog with
    | LogicalStartRecord (n, l) ->
        l
        |> Option.bind (tx.Recovery.LogLogicalAbort n)
        |> Option.iter logMgr.Flush
    | TableFileInsertEndRecord (n, tn, bn, sid, start, _) ->
        catalogMgr.GetTableInfo tx tn
        |> Option.bind (fun ti ->
            let rf =
                newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true ti

            let rid =
                RecordId.newBlockRecordId sid ti.FileName bn

            rf.DeleteByRecordId rid
            tx.Recovery.LogLogicalAbort n start)
        |> Option.iter logMgr.Flush
    | TableFileDeleteEndRecord (n, tn, bn, sid, start, _) ->
        catalogMgr.GetTableInfo tx tn
        |> Option.bind (fun ti ->
            let rf =
                newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true ti

            let rid =
                RecordId.newBlockRecordId sid ti.FileName bn

            rf.InsertByRecordId rid
            tx.Recovery.LogLogicalAbort n start)
        |> Option.iter logMgr.Flush
    | IndexInsertEndRecord (n, inm, sk, bn, sid, start, _) ->
        catalogMgr.GetIndexInfoByName tx inm
        |> Option.bind (fun ii ->
            let idx = IndexFactory.newIndex fileMgr tx ii

            let rid =
                RecordId.newBlockRecordId sid ii.TableInfo.FileName bn

            idx.Delete false sk rid
            idx.Close()
            tx.Recovery.LogLogicalAbort n start)
        |> Option.iter logMgr.Flush
    | IndexDeleteEndRecord (n, inm, sk, bn, sid, start, _) ->
        catalogMgr.GetIndexInfoByName tx inm
        |> Option.bind (fun ii ->
            let idx = newIndex fileMgr tx ii

            let rid =
                RecordId.newBlockRecordId sid ii.TableInfo.FileName bn

            idx.Insert false sk rid
            idx.Close()
            tx.Recovery.LogLogicalAbort n start)
        |> Option.iter logMgr.Flush
    | IndexPageInsertRecord (n, ibid, dir, kt, sid, optlsn) ->
        let buffer = tx.Buffer.Pin ibid
        match optlsn with
        | Some lsn when lsn < buffer.LastLogSeqNo() ->
            if dir then BTreeDir.deleteASlot ibid kt sid else BTreeLeaf.deleteASlot ibid kt sid
            tx.Recovery.LogIndexPageDeletionClr dir n ibid kt sid lsn
            |> Option.iter logMgr.Flush
        | _ -> ()
        tx.Buffer.Unpin buffer
    | IndexPageDeleteRecord (n, ibid, dir, kt, sid, optlsn) ->
        let buffer = tx.Buffer.Pin ibid
        match optlsn with
        | Some lsn when lsn < buffer.LastLogSeqNo() ->
            if dir then BTreeDir.insertASlot ibid kt sid else BTreeLeaf.insertASlot ibid kt sid
            tx.Recovery.LogIndexPageInsertionClr dir n ibid kt sid lsn
            |> Option.iter logMgr.Flush
        | _ -> ()
        tx.Buffer.Unpin buffer
    | SetValueRecord (n, bid, off, _, v, _, optlsn) ->
        let buffer = tx.Buffer.Pin bid
        optlsn
        |> Option.bind (tx.Recovery.LogSetValClr n buffer off v)
        |> Option.iter logMgr.Flush
        buffer.SetVal off v None
        tx.Buffer.Unpin buffer
    | _ -> ()

let rollback fileMgr logMgr catalogMgr tx =
    let undoLogRecord = undo fileMgr logMgr catalogMgr tx

    let mutable txUnDoNextLSN = None
    let mutable inStart = false
    logMgr.Records()
    |> Seq.map fromLogRecord
    |> Seq.filter (fun rlog -> transactionNo rlog = tx.TransactionNo)
    |> Seq.iter (fun rlog ->
        if not (inStart) then
            match txUnDoNextLSN, getLogSeqNo rlog with
            | None, _ ->
                match rlog with
                | StartRecord (_) -> inStart <- true
                | LogicalAbortRecord(logicalStartLSN = lsn) ->
                    undoLogRecord rlog
                    txUnDoNextLSN <- Some lsn
                | TableFileInsertEndRecord(logicalStartLSN = lsn) ->
                    undoLogRecord rlog
                    txUnDoNextLSN <- Some lsn
                | TableFileDeleteEndRecord(logicalStartLSN = lsn) ->
                    undoLogRecord rlog
                    txUnDoNextLSN <- Some lsn
                | IndexInsertEndRecord(logicalStartLSN = lsn) ->
                    undoLogRecord rlog
                    txUnDoNextLSN <- Some lsn
                | IndexDeleteEndRecord(logicalStartLSN = lsn) ->
                    undoLogRecord rlog
                    txUnDoNextLSN <- Some lsn
                | _ -> undoLogRecord rlog
            | Some (lsn1), Some (lsn2) when lsn1 > lsn2 ->
                match rlog with
                | StartRecord (_) -> inStart <- true
                | LogicalAbortRecord(logicalStartLSN = lsn) ->
                    undoLogRecord rlog
                    txUnDoNextLSN <- Some lsn
                | TableFileInsertEndRecord(logicalStartLSN = lsn) ->
                    undoLogRecord rlog
                    txUnDoNextLSN <- Some lsn
                | TableFileDeleteEndRecord(logicalStartLSN = lsn) ->
                    undoLogRecord rlog
                    txUnDoNextLSN <- Some lsn
                | IndexInsertEndRecord(logicalStartLSN = lsn) ->
                    undoLogRecord rlog
                    txUnDoNextLSN <- Some lsn
                | IndexDeleteEndRecord(logicalStartLSN = lsn) ->
                    undoLogRecord rlog
                    txUnDoNextLSN <- Some lsn
                | _ -> undoLogRecord rlog
            | _, _ -> ())

let onCommit logMgr tx =
    if not (tx.ReadOnly) then
        newCommitRecord tx.TransactionNo
        |> writeToLog logMgr
        |> logMgr.Flush

let onRollback fileMgr logMgr catalogMgr tx =
    if not (tx.ReadOnly) then
        rollback fileMgr logMgr catalogMgr tx
        newRollbackRecord tx.TransactionNo
        |> writeToLog logMgr
        |> logMgr.Flush
