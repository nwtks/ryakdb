module RyakDB.Recovery.TransactionRecoveryFinalize

open RyakDB.Storage.Log
open RyakDB.Table
open RyakDB.Index
open RyakDB.Buffer.TransactionBuffer
open RyakDB.Recovery.RecoveryLog
open RyakDB.Table.TableFile
open RyakDB.Index.BTreeBranch
open RyakDB.Index.BTreeLeaf
open RyakDB.Transaction
open RyakDB.Catalog.CatalogManager

let insertBTreeBranchSlot tx keyType blockId slot =
    let schema = BTreeBranch.keyTypeToSchema keyType

    let page =
        BTreeBranch.initBTreePage tx.Buffer tx.Concurrency tx.Recovery schema blockId

    page.Insert slot
    page.Close()

let deleteBTreeBranchSlot tx keyType blockId slot =
    let schema = BTreeBranch.keyTypeToSchema keyType

    let page =
        BTreeBranch.initBTreePage tx.Buffer tx.Concurrency tx.Recovery schema blockId

    page.Delete slot
    page.Close()

let insertBTreeLeafSlot tx keyType blockId slot =
    let schema = BTreeLeaf.keyTypeToSchema keyType

    let page =
        BTreeLeaf.initBTreePage tx.Buffer tx.Concurrency tx.Recovery schema blockId

    page.Insert slot
    page.Close()

let deleteBTreeLeafSlot tx keyType blockId slot =
    let schema = BTreeLeaf.keyTypeToSchema keyType

    let page =
        BTreeLeaf.initBTreePage tx.Buffer tx.Concurrency tx.Recovery schema blockId

    page.Delete slot
    page.Close()

let undo fileMgr logMgr catalogMgr tx recoveryLog =
    match recoveryLog with
    | LogicalStartRecord (n, l) ->
        l
        |> Option.bind (tx.Recovery.LogLogicalAbort n)
        |> Option.iter logMgr.Flush
    | TableFileInsertEndRecord (n, tn, bn, sid, start, _) ->
        catalogMgr.GetTableInfo tx tn
        |> Option.bind (fun ti ->
            let tf =
                newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true ti

            RecordId.newBlockRecordId sid ti.FileName bn
            |> tf.DeleteByRecordId
            tx.Recovery.LogLogicalAbort n start)
        |> Option.iter logMgr.Flush
    | TableFileDeleteEndRecord (n, tn, bn, sid, start, _) ->
        catalogMgr.GetTableInfo tx tn
        |> Option.bind (fun ti ->
            let tf =
                newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true ti

            RecordId.newBlockRecordId sid ti.FileName bn
            |> tf.InsertByRecordId
            tx.Recovery.LogLogicalAbort n start)
        |> Option.iter logMgr.Flush
    | IndexInsertEndRecord (n, inm, sk, bn, sid, start, _) ->
        catalogMgr.GetIndexInfoByName tx inm
        |> Option.bind (fun ii ->
            let idx = IndexFactory.newIndex fileMgr tx ii
            RecordId.newBlockRecordId sid ii.TableInfo.FileName bn
            |> idx.Delete false sk
            idx.Close()
            tx.Recovery.LogLogicalAbort n start)
        |> Option.iter logMgr.Flush
    | IndexDeleteEndRecord (n, inm, sk, bn, sid, start, _) ->
        catalogMgr.GetIndexInfoByName tx inm
        |> Option.bind (fun ii ->
            let idx = IndexFactory.newIndex fileMgr tx ii
            RecordId.newBlockRecordId sid ii.TableInfo.FileName bn
            |> idx.Insert false sk
            idx.Close()
            tx.Recovery.LogLogicalAbort n start)
        |> Option.iter logMgr.Flush
    | IndexPageInsertRecord (n, ibid, branch, kt, sid, optlsn) ->
        let buffer = tx.Buffer.Pin ibid
        match optlsn with
        | Some lsn when lsn < buffer.LastLogSeqNo() ->
            if branch then deleteBTreeBranchSlot tx kt ibid sid else deleteBTreeLeafSlot tx kt ibid sid
            tx.Recovery.LogIndexPageDeletionClear branch n ibid kt sid lsn
            |> Option.iter logMgr.Flush
        | _ -> ()
        tx.Buffer.Unpin buffer
    | IndexPageDeleteRecord (n, ibid, branch, kt, sid, optlsn) ->
        let buffer = tx.Buffer.Pin ibid
        match optlsn with
        | Some lsn when lsn < buffer.LastLogSeqNo() ->
            if branch then insertBTreeBranchSlot tx kt ibid sid else insertBTreeLeafSlot tx kt ibid sid
            tx.Recovery.LogIndexPageInsertionClear branch n ibid kt sid lsn
            |> Option.iter logMgr.Flush
        | _ -> ()
        tx.Buffer.Unpin buffer
    | SetValueRecord (n, bid, off, _, v, _, optlsn) ->
        let buffer = tx.Buffer.Pin bid
        optlsn
        |> Option.bind (tx.Recovery.LogSetValClear n buffer off v)
        |> Option.iter logMgr.Flush
        buffer.SetVal off v None
        tx.Buffer.Unpin buffer
    | _ -> ()

let rollback fileMgr logMgr catalogMgr tx =
    let undoLogRecord = undo fileMgr logMgr catalogMgr tx

    let mutable txUndoNextLogSeqNo = None
    let mutable inStart = false

    logMgr.Records()
    |> Seq.map fromLogRecord
    |> Seq.filter (fun rlog -> transactionNo rlog = tx.TransactionNo)
    |> Seq.iter (fun rlog ->
        if not (inStart) then
            match txUndoNextLogSeqNo, getLogSeqNo rlog with
            | None, _ ->
                match rlog with
                | StartRecord (_) -> inStart <- true
                | LogicalAbortRecord(logicalStartLogSeqNo = lsn) ->
                    undoLogRecord rlog
                    txUndoNextLogSeqNo <- Some lsn
                | TableFileInsertEndRecord(logicalStartLogSeqNo = lsn) ->
                    undoLogRecord rlog
                    txUndoNextLogSeqNo <- Some lsn
                | TableFileDeleteEndRecord(logicalStartLogSeqNo = lsn) ->
                    undoLogRecord rlog
                    txUndoNextLogSeqNo <- Some lsn
                | IndexInsertEndRecord(logicalStartLogSeqNo = lsn) ->
                    undoLogRecord rlog
                    txUndoNextLogSeqNo <- Some lsn
                | IndexDeleteEndRecord(logicalStartLogSeqNo = lsn) ->
                    undoLogRecord rlog
                    txUndoNextLogSeqNo <- Some lsn
                | _ -> undoLogRecord rlog
            | Some lsn1, Some lsn2 when lsn1 > lsn2 ->
                match rlog with
                | StartRecord (_) -> inStart <- true
                | LogicalAbortRecord(logicalStartLogSeqNo = lsn) ->
                    undoLogRecord rlog
                    txUndoNextLogSeqNo <- Some lsn
                | TableFileInsertEndRecord(logicalStartLogSeqNo = lsn) ->
                    undoLogRecord rlog
                    txUndoNextLogSeqNo <- Some lsn
                | TableFileDeleteEndRecord(logicalStartLogSeqNo = lsn) ->
                    undoLogRecord rlog
                    txUndoNextLogSeqNo <- Some lsn
                | IndexInsertEndRecord(logicalStartLogSeqNo = lsn) ->
                    undoLogRecord rlog
                    txUndoNextLogSeqNo <- Some lsn
                | IndexDeleteEndRecord(logicalStartLogSeqNo = lsn) ->
                    undoLogRecord rlog
                    txUndoNextLogSeqNo <- Some lsn
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
