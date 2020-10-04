module RyakDB.Recovery.SystemRecovery

open RyakDB.Storage.Log
open RyakDB.Buffer.BufferPool
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
    | LogicalStartRecord (n, lsn) ->
        lsn
        |> Option.bind (tx.Recovery.LogLogicalAbort n)
        |> Option.iter logMgr.Flush
    | TableFileInsertEndRecord (n, tn, bn, sid, start, _) ->
        catalogMgr.GetTableInfo tx tn
        |> Option.iter (fun ti ->
            let tf =
                newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true ti

            RecordId.newBlockRecordId sid ti.FileName bn
            |> tf.UndoInsert)
        tx.Recovery.LogLogicalAbort n start
        |> Option.iter logMgr.Flush
    | TableFileDeleteEndRecord (n, tn, bn, sid, start, _) ->
        catalogMgr.GetTableInfo tx tn
        |> Option.iter (fun ti ->
            let tf =
                newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true ti

            RecordId.newBlockRecordId sid ti.FileName bn
            |> tf.UndoDelete)
        tx.Recovery.LogLogicalAbort n start
        |> Option.iter logMgr.Flush
    | IndexInsertEndRecord (n, inm, sk, bn, sid, start, _) ->
        catalogMgr.GetIndexInfoByName tx inm
        |> Option.iter (fun ii ->
            let idx = IndexFactory.newIndex fileMgr tx ii
            RecordId.newBlockRecordId sid ii.TableInfo.FileName bn
            |> idx.Delete false sk
            idx.Close())
        tx.Recovery.LogLogicalAbort n start
        |> Option.iter logMgr.Flush
    | IndexDeleteEndRecord (n, inm, sk, bn, sid, start, _) ->
        catalogMgr.GetIndexInfoByName tx inm
        |> Option.iter (fun ii ->
            let idx = IndexFactory.newIndex fileMgr tx ii
            RecordId.newBlockRecordId sid ii.TableInfo.FileName bn
            |> idx.Insert false sk
            idx.Close())
        tx.Recovery.LogLogicalAbort n start
        |> Option.iter logMgr.Flush
    | IndexPageInsertRecord (n, ibid, branch, kt, sid, lsn) ->
        let buffer = tx.Buffer.Pin ibid
        match lsn with
        | Some l when l < buffer.LastLogSeqNo() ->
            if branch then deleteBTreeBranchSlot tx kt ibid sid else deleteBTreeLeafSlot tx kt ibid sid
            tx.Recovery.LogIndexPageDeletionClear branch n ibid kt sid l
            |> Option.iter logMgr.Flush
        | _ -> ()
        tx.Buffer.Unpin buffer
    | IndexPageDeleteRecord (n, ibid, branch, kt, sid, lsn) ->
        let buffer = tx.Buffer.Pin ibid
        match lsn with
        | Some l when l < buffer.LastLogSeqNo() ->
            if branch then insertBTreeBranchSlot tx kt ibid sid else insertBTreeLeafSlot tx kt ibid sid
            tx.Recovery.LogIndexPageInsertionClear branch n ibid kt sid l
            |> Option.iter logMgr.Flush
        | _ -> ()
        tx.Buffer.Unpin buffer
    | SetValueRecord (n, bid, off, _, v, _, lsn) ->
        let buffer = tx.Buffer.Pin bid
        lsn
        |> Option.bind (tx.Recovery.LogSetValClear n buffer off v)
        |> Option.iter logMgr.Flush
        buffer.SetVal off v None
        tx.Buffer.Unpin buffer
    | _ -> ()

let redo tx recoveryLog =
    match recoveryLog with
    | IndexPageInsertRecord (_, ibid, branch, kt, sid, lsn)
    | IndexPageInsertClear (_, ibid, branch, kt, sid, _, lsn) ->
        let buffer = tx.Buffer.Pin ibid
        match lsn with
        | Some l when l > buffer.LastLogSeqNo() ->
            if branch then insertBTreeBranchSlot tx kt ibid sid else insertBTreeLeafSlot tx kt ibid sid
        | _ -> ()
        tx.Buffer.Unpin buffer
    | IndexPageDeleteRecord (_, ibid, branch, kt, sid, lsn)
    | IndexPageDeleteClear (_, ibid, branch, kt, sid, _, lsn) ->
        let buffer = tx.Buffer.Pin ibid
        match lsn with
        | Some l when l > buffer.LastLogSeqNo() ->
            if branch then deleteBTreeBranchSlot tx kt ibid sid else deleteBTreeLeafSlot tx kt ibid sid
        | _ -> ()
        tx.Buffer.Unpin buffer
    | SetValueRecord (_, bid, off, _, _, nv, _)
    | SetValueClear (_, bid, off, _, _, nv, _, _) ->
        let buffer = tx.Buffer.Pin bid
        buffer.SetVal off nv None
        tx.Buffer.Unpin buffer
    | _ -> ()

let rollbackPartially fileMgr logMgr catalogMgr tx stepsInUndo =
    let mutable txUndoNextLogSeqNo = None

    logMgr.Records()
    |> if stepsInUndo > 0 then Seq.take stepsInUndo else id
    |> Seq.map fromLogRecord
    |> Seq.filter (fun rlog -> transactionNo rlog = tx.TransactionNo)
    |> Seq.takeWhile (fun rlog -> operation rlog <> RecoveryLogOperation.Start)
    |> Seq.filter (fun rlog ->
        Option.isNone txUndoNextLogSeqNo
        || getLogSeqNo rlog
        |> Option.map (fun lsn -> txUndoNextLogSeqNo |> Option.get > lsn)
        |> Option.defaultValue false)
    |> Seq.iter (fun rlog ->
        undo fileMgr logMgr catalogMgr tx rlog
        txUndoNextLogSeqNo <-
            getLogicalStartLogSeqNo rlog
            |> Option.orElse txUndoNextLogSeqNo)

let rollback fileMgr logMgr catalogMgr tx =
    rollbackPartially fileMgr logMgr catalogMgr tx -1

let recoverPartially fileMgr logMgr catalogMgr tx stepsInUndo =
    let mutable inCheckpoint = false

    let redoRecords, uncompletedTxs, _ =
        logMgr.Records()
        |> Seq.map fromLogRecord
        |> Seq.takeWhile (fun _ -> not (inCheckpoint))
        |> Seq.fold (fun (redoRecords, uncompletedTxs: Set<int64>, finishedTxs: Set<int64>) rlog ->
            match rlog with
            | CheckpointRecord(txNos = txNos) ->
                inCheckpoint <- true
                redoRecords,
                txNos
                |> List.filter (finishedTxs.Contains >> not)
                |> List.fold (fun uncompletedTxs txNo -> uncompletedTxs.Add txNo) uncompletedTxs,
                finishedTxs
            | CommitRecord(txNo = txNo)
            | RollbackRecord(txNo = txNo) -> rlog :: redoRecords, uncompletedTxs, finishedTxs.Add txNo
            | StartRecord(txNo = txNo) when not (finishedTxs.Contains txNo) ->
                rlog :: redoRecords, uncompletedTxs.Add txNo, finishedTxs
            | _ -> rlog :: redoRecords, uncompletedTxs, finishedTxs) ([], Set.empty, Set.empty)

    redoRecords |> List.iter (redo tx)

    let mutable uncompletedTxs = uncompletedTxs.Remove tx.TransactionNo
    let mutable txUndoNextLogSeqNos = Map.empty

    logMgr.Records()
    |> if stepsInUndo > 0 then Seq.take stepsInUndo else id
    |> Seq.map fromLogRecord
    |> Seq.takeWhile (fun _ -> not (uncompletedTxs.IsEmpty))
    |> Seq.map (fun rlog -> rlog, transactionNo rlog)
    |> Seq.filter (fun (rlog, txNo) ->
        uncompletedTxs.Contains txNo
        && (not (txUndoNextLogSeqNos.ContainsKey txNo)
            || getLogSeqNo rlog
            |> Option.map (fun lsn -> txUndoNextLogSeqNos.[txNo] > lsn)
            |> Option.defaultValue false))
    |> Seq.iter (fun (rlog, txNo) ->
        match rlog, getLogicalStartLogSeqNo rlog, getUndoNextLogSeqNo rlog with
        | CommitRecord _, _, _
        | RollbackRecord _, _, _ -> ()
        | StartRecord _, _, _ -> uncompletedTxs <- uncompletedTxs.Remove txNo
        | _, Some logicalStartLSN, _ ->
            undo fileMgr logMgr catalogMgr tx rlog
            txUndoNextLogSeqNos <- txUndoNextLogSeqNos.Add(txNo, logicalStartLSN)
        | _, _, Some undoNextLSN -> txUndoNextLogSeqNos <- txUndoNextLogSeqNos.Add(txNo, undoNextLSN)
        | _, _, _ -> undo fileMgr logMgr catalogMgr tx rlog)

let recover fileMgr logMgr catalogMgr tx =
    recoverPartially fileMgr logMgr catalogMgr tx -1

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

let recoverSystem fileMgr logMgr bufferPool catalogMgr tx =
    recover fileMgr logMgr catalogMgr tx
    bufferPool.FlushAll()
    logMgr.RemoveAndCreateNewLog()
    newStartRecord tx.TransactionNo
    |> writeToLog logMgr
    |> ignore
