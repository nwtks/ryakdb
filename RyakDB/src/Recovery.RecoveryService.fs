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
    let insertBTreeBranchSlot tx keyType blockId slotNo =
        let schema = BTreeBranch.keyTypeToSchema keyType

        use page =
            BTreeBranch.initBTreePage tx.Buffer tx.Concurrency tx.Recovery schema blockId

        page.Insert slotNo

    let deleteBTreeBranchSlot tx keyType blockId slotNo =
        let schema = BTreeBranch.keyTypeToSchema keyType

        use page =
            BTreeBranch.initBTreePage tx.Buffer tx.Concurrency tx.Recovery schema blockId

        page.Delete slotNo

    let insertBTreeLeafSlot tx keyType blockId slotNo =
        let schema = BTreeLeaf.keyTypeToSchema keyType

        use page =
            BTreeLeaf.initBTreePage tx.Buffer tx.Concurrency tx.Recovery schema blockId

        page.Insert slotNo

    let deleteBTreeLeafSlot tx keyType blockId slotNo =
        let schema = BTreeLeaf.keyTypeToSchema keyType

        use page =
            BTreeLeaf.initBTreePage tx.Buffer tx.Concurrency tx.Recovery schema blockId

        page.Delete slotNo

    let logLogicalUndo logService txNo undoNextLogSeqNo =
        newLogicalUndoRecord txNo undoNextLogSeqNo
        |> writeToLog logService
        |> logService.Flush

    let logIndexPageInsertionCompensationRecord logService
                                                compTxNo
                                                isBranch
                                                keyType
                                                indexBlockId
                                                indexSlotNo
                                                undoNextLogSeqNo
                                                =
        newIndexPageInsertCompensateRecord compTxNo isBranch keyType indexBlockId indexSlotNo undoNextLogSeqNo
        |> writeToLog logService
        |> logService.Flush

    let logIndexPageDeletionCompensationRecord logService
                                               compTxNo
                                               isBranch
                                               keyType
                                               indexBlockId
                                               indexSlotNo
                                               undoNextLogSeqNo
                                               =
        newIndexPageDeleteCompensateRecord compTxNo isBranch keyType indexBlockId indexSlotNo undoNextLogSeqNo
        |> writeToLog logService
        |> logService.Flush

    let logSetValCompensationRecord logService compTxNo (buffer: Buffer) offset newValue undoNextLogSeqNo =
        let blockId = buffer.BlockId()
        if not (BlockId.fileName blockId |> FileService.isTempFile) then
            newSetValueCompensateRecord
                compTxNo
                blockId
                offset
                (DbConstant.dbType newValue |> buffer.GetVal offset)
                newValue
                undoNextLogSeqNo
            |> writeToLog logService
            |> logService.Flush

    let undo fileService logService catalogService tx recoveryLog =
        match recoveryLog with
        | LogicalStartRecord (txNo, lsn) ->
            lsn
            |> Option.iter (logLogicalUndo logService txNo)
        | TableFileInsertEndRecord (txNo, tableName, blockNo, slotNo, startLsn, _) ->
            catalogService.GetTableInfo tx tableName
            |> Option.iter (fun ti ->
                use tf =
                    newTableFile fileService tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true ti

                RecordId.newBlockRecordId slotNo (TableInfo.tableFileName ti) blockNo
                |> tf.UndoInsert)
            logLogicalUndo logService txNo startLsn
        | TableFileDeleteEndRecord (txNo, tableName, blockNo, slotNo, startLsn, _) ->
            catalogService.GetTableInfo tx tableName
            |> Option.iter (fun ti ->
                use tf =
                    newTableFile fileService tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true ti

                RecordId.newBlockRecordId slotNo (TableInfo.tableFileName ti) blockNo
                |> tf.UndoDelete)
            logLogicalUndo logService txNo startLsn
        | IndexInsertEndRecord (txNo, indexName, searchKey, blockNo, slotNo, startLsn, _) ->
            catalogService.GetIndexInfoByName tx indexName
            |> Option.iter (fun ii ->
                use idx = IndexFactory.newIndex fileService tx ii
                RecordId.newBlockRecordId slotNo (IndexInfo.tableFileName ii) blockNo
                |> idx.Delete false searchKey)
            logLogicalUndo logService txNo startLsn
        | IndexDeleteEndRecord (txNo, indexName, searchKey, blockNo, slotNo, startLsn, _) ->
            catalogService.GetIndexInfoByName tx indexName
            |> Option.iter (fun ii ->
                use idx = IndexFactory.newIndex fileService tx ii
                RecordId.newBlockRecordId slotNo (IndexInfo.tableFileName ii) blockNo
                |> idx.Insert false searchKey)
            logLogicalUndo logService txNo startLsn
        | IndexPageInsertRecord (txNo, branch, keyType, blockId, slotNo, lsn) ->
            let buffer = tx.Buffer.Pin blockId
            match lsn with
            | Some no when no < buffer.LastLogSeqNo() ->
                if branch
                then deleteBTreeBranchSlot tx keyType blockId slotNo
                else deleteBTreeLeafSlot tx keyType blockId slotNo
                logIndexPageDeletionCompensationRecord logService txNo branch keyType blockId slotNo no
            | _ -> ()
            tx.Buffer.Unpin buffer
        | IndexPageDeleteRecord (txNo, branch, keyType, blockId, slotNo, lsn) ->
            let buffer = tx.Buffer.Pin blockId
            match lsn with
            | Some no when no < buffer.LastLogSeqNo() ->
                if branch
                then insertBTreeBranchSlot tx keyType blockId slotNo
                else insertBTreeLeafSlot tx keyType blockId slotNo
                logIndexPageInsertionCompensationRecord logService txNo branch keyType blockId slotNo no
            | _ -> ()
            tx.Buffer.Unpin buffer
        | SetValueRecord (txNo, blockId, offset, _, oldValue, _, lsn) ->
            let buffer = tx.Buffer.Pin blockId
            match lsn with
            | Some no -> logSetValCompensationRecord logService txNo buffer offset oldValue no
            | _ -> ()
            buffer.SetVal offset oldValue None
            tx.Buffer.Unpin buffer
        | _ -> ()

    let redo tx recoveryLog =
        match recoveryLog with
        | IndexPageInsertRecord (_, branch, keyType, blockId, slotNo, lsn)
        | IndexPageInsertCompensateRecord (_, branch, keyType, blockId, slotNo, _, lsn) ->
            let buffer = tx.Buffer.Pin blockId
            match lsn with
            | Some no when no > buffer.LastLogSeqNo() ->
                if branch
                then insertBTreeBranchSlot tx keyType blockId slotNo
                else insertBTreeLeafSlot tx keyType blockId slotNo
            | _ -> ()
            tx.Buffer.Unpin buffer
        | IndexPageDeleteRecord (_, branch, keyType, blockId, slotNo, lsn)
        | IndexPageDeleteCompensateRecord (_, branch, keyType, blockId, slotNo, _, lsn) ->
            let buffer = tx.Buffer.Pin blockId
            match lsn with
            | Some no when no > buffer.LastLogSeqNo() ->
                if branch
                then deleteBTreeBranchSlot tx keyType blockId slotNo
                else deleteBTreeLeafSlot tx keyType blockId slotNo
            | _ -> ()
            tx.Buffer.Unpin buffer
        | SetValueRecord (_, blockId, offset, _, _, newValue, _)
        | SetValueCompensateRecord (_, blockId, offset, _, _, newValue, _, _) ->
            let buffer = tx.Buffer.Pin blockId
            buffer.SetVal offset newValue None
            tx.Buffer.Unpin buffer
        | _ -> ()

    let rollbackPartially fileService logService catalogService tx stepsInUndo =
        let mutable undoNextLogSeqNo = None

        logService.Records()
        |> if stepsInUndo > 0 then Seq.truncate stepsInUndo else id
        |> Seq.map fromLogRecord
        |> Seq.filter (fun rlog -> transactionNo rlog = tx.TransactionNo)
        |> Seq.takeWhile (fun rlog -> operation rlog <> RecoveryLogOperation.Start)
        |> Seq.filter (fun rlog ->
            Option.isNone undoNextLogSeqNo
            || getLogSeqNo rlog
            |> Option.map (fun lsn -> undoNextLogSeqNo |> Option.get > lsn)
            |> Option.defaultValue false)
        |> Seq.iter (fun rlog ->
            undo fileService logService catalogService tx rlog
            undoNextLogSeqNo <-
                getLogicalStartLogSeqNo rlog
                |> Option.orElse undoNextLogSeqNo)

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
                    |> List.filter (fun txNo -> not (Set.contains txNo finishedTxs))
                    |> List.fold (fun uncompletedTxs txNo -> Set.add txNo uncompletedTxs) uncompletedTxs,
                    finishedTxs
                | CommitRecord(txNo = txNo)
                | RollbackRecord(txNo = txNo) -> rlog :: redoRecords, uncompletedTxs, Set.add txNo finishedTxs
                | StartRecord(txNo = txNo) when not (Set.contains txNo finishedTxs) ->
                    rlog :: redoRecords, Set.add txNo uncompletedTxs, finishedTxs
                | _ -> rlog :: redoRecords, uncompletedTxs, finishedTxs) ([], Set.empty, Set.empty)

        redoRecords |> List.iter (redo tx)

        let mutable uncompletedTxs =
            Set.remove tx.TransactionNo uncompletedTxs

        let mutable undoNextLogSeqNos = Map.empty

        logService.Records()
        |> if stepsInUndo > 0 then Seq.truncate stepsInUndo else id
        |> Seq.map fromLogRecord
        |> Seq.takeWhile (fun _ -> not (Seq.isEmpty uncompletedTxs))
        |> Seq.map (fun rlog -> rlog, transactionNo rlog)
        |> Seq.filter (fun (rlog, txNo) ->
            Set.contains txNo uncompletedTxs
            && (not (Map.containsKey txNo undoNextLogSeqNos)
                || getLogSeqNo rlog
                |> Option.map (fun lsn -> undoNextLogSeqNos.[txNo] > lsn)
                |> Option.defaultValue false))
        |> Seq.iter (fun (rlog, txNo) ->
            match rlog, getLogicalStartLogSeqNo rlog, getUndoNextLogSeqNo rlog with
            | CommitRecord _, _, _
            | RollbackRecord _, _, _ -> ()
            | StartRecord _, _, _ -> uncompletedTxs <- Set.remove txNo uncompletedTxs
            | _, Some startLsn, _ ->
                undo fileService logService catalogService tx rlog
                undoNextLogSeqNos <- Map.add txNo startLsn undoNextLogSeqNos
            | _, _, Some undoNextLsn -> undoNextLogSeqNos <- Map.add txNo undoNextLsn undoNextLogSeqNos
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
