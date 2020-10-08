module RyakDB.Recovery.SystemRecovery

open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Storage.File
open RyakDB.Storage.Log
open RyakDB.Buffer.Buffer
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

type SystemRecovery =
    { RecoverSystem: Transaction -> unit
      OnCommit: Transaction -> unit
      OnRollback: Transaction -> unit
      Checkpoint: int64 list -> unit }

module SystemRecovery =
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

    let logLogicalAbort logMgr txNo undoNextLogSeqNo =
        newLogicalAbortRecord txNo undoNextLogSeqNo
        |> writeToLog logMgr
        |> logMgr.Flush

    let logIndexPageInsertionClear logMgr isBranch compTxNo indexBlockId keyType slot undoNextLogSeqNo =
        newIndexPageInsertClear isBranch compTxNo indexBlockId keyType slot undoNextLogSeqNo
        |> writeToLog logMgr
        |> logMgr.Flush

    let logIndexPageDeletionClear logMgr isBranch compTxNo indexBlockId keyType slot undoNextLogSeqNo =
        newIndexPageDeleteClear isBranch compTxNo indexBlockId keyType slot undoNextLogSeqNo
        |> writeToLog logMgr
        |> logMgr.Flush

    let logSetValClear logMgr compTxNo (buffer: Buffer) offset newValue undoNextLogSeqNo =
        let blockId = buffer.BlockId()
        let (BlockId (fileName, _)) = blockId
        if not (FileManager.isTempFile fileName) then
            newSetValueClear
                compTxNo
                blockId
                offset
                (buffer.GetVal offset (DbConstant.dbType newValue))
                newValue
                undoNextLogSeqNo
            |> writeToLog logMgr
            |> logMgr.Flush

    let undo fileMgr (logMgr: LogManager) catalogMgr tx recoveryLog =
        match recoveryLog with
        | LogicalStartRecord (txNo, lsn) -> lsn |> Option.iter (logLogicalAbort logMgr txNo)
        | TableFileInsertEndRecord (txNo, tableName, blockNo, slot, startLsn, _) ->
            catalogMgr.GetTableInfo tx tableName
            |> Option.iter (fun ti ->
                let tf =
                    newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true ti

                RecordId.newBlockRecordId slot ti.FileName blockNo
                |> tf.UndoInsert)
            logLogicalAbort logMgr txNo startLsn
        | TableFileDeleteEndRecord (txNo, tableName, blockNo, slot, startLsn, _) ->
            catalogMgr.GetTableInfo tx tableName
            |> Option.iter (fun ti ->
                let tf =
                    newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true ti

                RecordId.newBlockRecordId slot ti.FileName blockNo
                |> tf.UndoDelete)
            logLogicalAbort logMgr txNo startLsn
        | IndexInsertEndRecord (txNo, indexName, searchKey, blockNo, slot, startLsn, _) ->
            catalogMgr.GetIndexInfoByName tx indexName
            |> Option.iter (fun ii ->
                let idx = IndexFactory.newIndex fileMgr tx ii
                RecordId.newBlockRecordId slot ii.TableInfo.FileName blockNo
                |> idx.Delete false searchKey
                idx.Close())
            logLogicalAbort logMgr txNo startLsn
        | IndexDeleteEndRecord (txNo, indexName, searchKey, blockNo, slot, startLsn, _) ->
            catalogMgr.GetIndexInfoByName tx indexName
            |> Option.iter (fun ii ->
                let idx = IndexFactory.newIndex fileMgr tx ii
                RecordId.newBlockRecordId slot ii.TableInfo.FileName blockNo
                |> idx.Insert false searchKey
                idx.Close())
            logLogicalAbort logMgr txNo startLsn
        | IndexPageInsertRecord (txNo, blockId, branch, keyType, slot, lsn) ->
            let buffer = tx.Buffer.Pin blockId
            match lsn with
            | Some l when l < buffer.LastLogSeqNo() ->
                if branch
                then deleteBTreeBranchSlot tx keyType blockId slot
                else deleteBTreeLeafSlot tx keyType blockId slot
                logIndexPageDeletionClear logMgr branch txNo blockId keyType slot l
            | _ -> ()
            tx.Buffer.Unpin buffer
        | IndexPageDeleteRecord (txNo, blockId, branch, keyType, slot, lsn) ->
            let buffer = tx.Buffer.Pin blockId
            match lsn with
            | Some l when l < buffer.LastLogSeqNo() ->
                if branch
                then insertBTreeBranchSlot tx keyType blockId slot
                else insertBTreeLeafSlot tx keyType blockId slot
                logIndexPageInsertionClear logMgr branch txNo blockId keyType slot l
            | _ -> ()
            tx.Buffer.Unpin buffer
        | SetValueRecord (txNo, blockId, offset, _, oldValue, _, lsn) ->
            let buffer = tx.Buffer.Pin blockId
            match lsn with
            | Some l -> logSetValClear logMgr txNo buffer offset oldValue l
            | _ -> ()
            buffer.SetVal offset oldValue None
            tx.Buffer.Unpin buffer
        | _ -> ()

    let redo tx recoveryLog =
        match recoveryLog with
        | IndexPageInsertRecord (_, blockId, branch, keyType, slot, lsn)
        | IndexPageInsertClear (_, blockId, branch, keyType, slot, _, lsn) ->
            let buffer = tx.Buffer.Pin blockId
            match lsn with
            | Some l when l > buffer.LastLogSeqNo() ->
                if branch
                then insertBTreeBranchSlot tx keyType blockId slot
                else insertBTreeLeafSlot tx keyType blockId slot
            | _ -> ()
            tx.Buffer.Unpin buffer
        | IndexPageDeleteRecord (_, blockId, branch, keyType, slot, lsn)
        | IndexPageDeleteClear (_, blockId, branch, keyType, slot, _, lsn) ->
            let buffer = tx.Buffer.Pin blockId
            match lsn with
            | Some l when l > buffer.LastLogSeqNo() ->
                if branch
                then deleteBTreeBranchSlot tx keyType blockId slot
                else deleteBTreeLeafSlot tx keyType blockId slot
            | _ -> ()
            tx.Buffer.Unpin buffer
        | SetValueRecord (_, blockId, offset, _, _, newValue, _)
        | SetValueClear (_, blockId, offset, _, _, newValue, _, _) ->
            let buffer = tx.Buffer.Pin blockId
            buffer.SetVal offset newValue None
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
        |> logMgr.Flush

    let checkpoint logMgr txNos =
        newCheckpointRecord txNos
        |> writeToLog logMgr
        |> logMgr.Flush

let newSystemRecovery fileMgr logMgr bufferPool catalogMgr =
    { RecoverSystem = SystemRecovery.recoverSystem fileMgr logMgr bufferPool catalogMgr
      OnCommit = SystemRecovery.onCommit logMgr
      OnRollback = SystemRecovery.onRollback fileMgr logMgr catalogMgr
      Checkpoint = SystemRecovery.checkpoint logMgr }
