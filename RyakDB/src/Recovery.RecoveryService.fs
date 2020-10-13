module RyakDB.Recovery.RecoveryService

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
open RyakDB.Catalog.CatalogService

type RecoveryService =
    { RecoverSystem: Transaction -> unit
      OnCommit: Transaction -> unit
      OnRollback: Transaction -> unit
      Checkpoint: int64 list -> unit }

module RecoveryService =
    let insertBTreeBranchSlot tx keyType blockId slot =
        let schema = BTreeBranch.keyTypeToSchema keyType

        use page =
            BTreeBranch.initBTreePage tx.Buffer tx.Concurrency tx.Recovery schema blockId

        page.Insert slot

    let deleteBTreeBranchSlot tx keyType blockId slot =
        let schema = BTreeBranch.keyTypeToSchema keyType

        use page =
            BTreeBranch.initBTreePage tx.Buffer tx.Concurrency tx.Recovery schema blockId

        page.Delete slot

    let insertBTreeLeafSlot tx keyType blockId slot =
        let schema = BTreeLeaf.keyTypeToSchema keyType

        use page =
            BTreeLeaf.initBTreePage tx.Buffer tx.Concurrency tx.Recovery schema blockId

        page.Insert slot

    let deleteBTreeLeafSlot tx keyType blockId slot =
        let schema = BTreeLeaf.keyTypeToSchema keyType

        use page =
            BTreeLeaf.initBTreePage tx.Buffer tx.Concurrency tx.Recovery schema blockId

        page.Delete slot

    let logLogicalAbort logService txNo undoNextLogSeqNo =
        newLogicalAbortRecord txNo undoNextLogSeqNo
        |> writeToLog logService
        |> logService.Flush

    let logIndexPageInsertionClear logService isBranch compTxNo indexBlockId keyType slot undoNextLogSeqNo =
        newIndexPageInsertClear isBranch compTxNo indexBlockId keyType slot undoNextLogSeqNo
        |> writeToLog logService
        |> logService.Flush

    let logIndexPageDeletionClear logService isBranch compTxNo indexBlockId keyType slot undoNextLogSeqNo =
        newIndexPageDeleteClear isBranch compTxNo indexBlockId keyType slot undoNextLogSeqNo
        |> writeToLog logService
        |> logService.Flush

    let logSetValClear logService compTxNo (buffer: Buffer) offset newValue undoNextLogSeqNo =
        let blockId = buffer.BlockId()
        if not (BlockId.fileName blockId |> FileService.isTempFile) then
            newSetValueClear
                compTxNo
                blockId
                offset
                (buffer.GetVal offset (DbConstant.dbType newValue))
                newValue
                undoNextLogSeqNo
            |> writeToLog logService
            |> logService.Flush

    let undo fileService (logService: LogService) catalogService tx recoveryLog =
        match recoveryLog with
        | LogicalStartRecord (txNo, lsn) ->
            lsn
            |> Option.iter (logLogicalAbort logService txNo)
        | TableFileInsertEndRecord (txNo, tableName, blockNo, slot, startLsn, _) ->
            catalogService.GetTableInfo tx tableName
            |> Option.iter (fun ti ->
                use tf =
                    newTableFile fileService tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true ti

                RecordId.newBlockRecordId slot (TableInfo.tableFileName ti) blockNo
                |> tf.UndoInsert)
            logLogicalAbort logService txNo startLsn
        | TableFileDeleteEndRecord (txNo, tableName, blockNo, slot, startLsn, _) ->
            catalogService.GetTableInfo tx tableName
            |> Option.iter (fun ti ->
                use tf =
                    newTableFile fileService tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true ti

                RecordId.newBlockRecordId slot (TableInfo.tableFileName ti) blockNo
                |> tf.UndoDelete)
            logLogicalAbort logService txNo startLsn
        | IndexInsertEndRecord (txNo, indexName, searchKey, blockNo, slot, startLsn, _) ->
            catalogService.GetIndexInfoByName tx indexName
            |> Option.iter (fun ii ->
                use idx = IndexFactory.newIndex fileService tx ii
                RecordId.newBlockRecordId slot (IndexInfo.tableFileName ii) blockNo
                |> idx.Delete false searchKey)
            logLogicalAbort logService txNo startLsn
        | IndexDeleteEndRecord (txNo, indexName, searchKey, blockNo, slot, startLsn, _) ->
            catalogService.GetIndexInfoByName tx indexName
            |> Option.iter (fun ii ->
                use idx = IndexFactory.newIndex fileService tx ii
                RecordId.newBlockRecordId slot (IndexInfo.tableFileName ii) blockNo
                |> idx.Insert false searchKey)
            logLogicalAbort logService txNo startLsn
        | IndexPageInsertRecord (txNo, blockId, branch, keyType, slot, lsn) ->
            let buffer = tx.Buffer.Pin blockId
            match lsn with
            | Some l when l < buffer.LastLogSeqNo() ->
                if branch
                then deleteBTreeBranchSlot tx keyType blockId slot
                else deleteBTreeLeafSlot tx keyType blockId slot
                logIndexPageDeletionClear logService branch txNo blockId keyType slot l
            | _ -> ()
            tx.Buffer.Unpin buffer
        | IndexPageDeleteRecord (txNo, blockId, branch, keyType, slot, lsn) ->
            let buffer = tx.Buffer.Pin blockId
            match lsn with
            | Some l when l < buffer.LastLogSeqNo() ->
                if branch
                then insertBTreeBranchSlot tx keyType blockId slot
                else insertBTreeLeafSlot tx keyType blockId slot
                logIndexPageInsertionClear logService branch txNo blockId keyType slot l
            | _ -> ()
            tx.Buffer.Unpin buffer
        | SetValueRecord (txNo, blockId, offset, _, oldValue, _, lsn) ->
            let buffer = tx.Buffer.Pin blockId
            match lsn with
            | Some l -> logSetValClear logService txNo buffer offset oldValue l
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

    let rollbackPartially fileService logService catalogService tx stepsInUndo =
        let mutable txUndoNextLogSeqNo = None

        logService.Records()
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
            undo fileService logService catalogService tx rlog
            txUndoNextLogSeqNo <-
                getLogicalStartLogSeqNo rlog
                |> Option.orElse txUndoNextLogSeqNo)

    let rollback fileService logService catalogService tx =
        rollbackPartially fileService logService catalogService tx -1

    let recoverPartially fileService logService catalogService tx stepsInUndo =
        let mutable inCheckpoint = false

        let redoRecords, uncompletedTxs, _ =
            logService.Records()
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

        logService.Records()
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
                undo fileService logService catalogService tx rlog
                txUndoNextLogSeqNos <- txUndoNextLogSeqNos.Add(txNo, logicalStartLSN)
            | _, _, Some undoNextLSN -> txUndoNextLogSeqNos <- txUndoNextLogSeqNos.Add(txNo, undoNextLSN)
            | _, _, _ -> undo fileService logService catalogService tx rlog)

    let recover fileService logService catalogService tx =
        recoverPartially fileService logService catalogService tx -1

    let onCommit logService tx =
        if not (tx.ReadOnly) then
            newCommitRecord tx.TransactionNo
            |> writeToLog logService
            |> logService.Flush

    let onRollback fileService logService catalogService tx =
        if not (tx.ReadOnly) then
            rollback fileService logService catalogService tx
            newRollbackRecord tx.TransactionNo
            |> writeToLog logService
            |> logService.Flush

    let recoverSystem fileService logService bufferPool catalogService tx =
        recover fileService logService catalogService tx
        bufferPool.FlushAll()
        logService.RemoveAndCreateNewLog()
        newStartRecord tx.TransactionNo
        |> writeToLog logService
        |> logService.Flush

    let checkpoint logService txNos =
        newCheckpointRecord txNos
        |> writeToLog logService
        |> logService.Flush

let newRecoveryService fileService logService bufferPool catalogService =
    { RecoverSystem = RecoveryService.recoverSystem fileService logService bufferPool catalogService
      OnCommit = RecoveryService.onCommit logService
      OnRollback = RecoveryService.onRollback fileService logService catalogService
      Checkpoint = RecoveryService.checkpoint logService }
