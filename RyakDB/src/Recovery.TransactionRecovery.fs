module RyakDB.Recovery.TransactionRecovery

open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Storage.File
open RyakDB.Storage.Log
open RyakDB.Buffer.Buffer
open RyakDB.Recovery.RecoveryLog
open RyakDB.Index

type TransactionRecovery =
    { Checkpoint: int64 list -> LogSeqNo
      LogSetVal: Buffer -> int32 -> DbConstant -> LogSeqNo option
      LogLogicalStart: unit -> LogSeqNo option
      LogLogicalAbort: int64 -> LogSeqNo -> LogSeqNo option
      LogTableFileInsertionEnd: string -> int64 -> int32 -> LogSeqNo option
      LogTableFileDeletionEnd: string -> int64 -> int32 -> LogSeqNo option
      LogIndexInsertionEnd: string -> SearchKey -> int64 -> int32 -> LogSeqNo option
      LogIndexDeletionEnd: string -> SearchKey -> int64 -> int32 -> LogSeqNo option
      LogIndexPageInsertion: bool -> BlockId -> SearchKeyType -> int32 -> LogSeqNo option
      LogIndexPageDeletion: bool -> BlockId -> SearchKeyType -> int32 -> LogSeqNo option
      LogIndexPageInsertionClr: bool -> int64 -> BlockId -> SearchKeyType -> int32 -> LogSeqNo -> LogSeqNo option
      LogIndexPageDeletionClr: bool -> int64 -> BlockId -> SearchKeyType -> int32 -> LogSeqNo -> LogSeqNo option
      LogSetValClr: int64 -> Buffer -> int32 -> DbConstant -> LogSeqNo -> LogSeqNo option }

module TransactionRecovery =
    let checkpoint logMgr txNos =
        newCheckpointRecord txNos |> writeToLog logMgr

    let logSetVal logMgr txNo buffer offset newVal =
        let blockId = buffer.BlockId()
        let (BlockId (fileName, _)) = blockId
        if FileManager.isTempFile fileName then
            None
        else
            newSetValueRecord txNo blockId offset (buffer.GetVal offset (DbConstant.dbType newVal)) newVal
            |> writeToLog logMgr
            |> Some

    let logLogicalStart logMgr txNo =
        newLogicalStartRecord txNo
        |> writeToLog logMgr
        |> Some

    let logLogicalAbort logMgr txNo undoNextLSN =
        newLogicalAbortRecord txNo undoNextLSN
        |> writeToLog logMgr
        |> Some

    let logTableFileInsertionEnd logMgr txNo logicalStartLogSeqNo tableName blockNo slotNo =
        match logicalStartLogSeqNo with
        | Some (startLsn) ->
            newTableFileInsertEndRecord txNo tableName blockNo slotNo startLsn
            |> writeToLog logMgr
            |> Some
        | _ -> failwith "Logical start LogSeqNo is null (in logTableFileInsertionEnd)"

    let logTableFileDeletionEnd logMgr txNo logicalStartLogSeqNo tableName blockNo slotNo =
        match logicalStartLogSeqNo with
        | Some (startLsn) ->
            newTableFileDeleteEndRecord txNo tableName blockNo slotNo startLsn
            |> writeToLog logMgr
            |> Some
        | _ -> failwith "Logical start LogSeqNo is null (in logTableFileDeletionEnd)"

    let logIndexInsertionEnd logMgr txNo logicalStartLogSeqNo indexName searchKey blockNo slotNo =
        match logicalStartLogSeqNo with
        | Some (startLsn) ->
            newIndexInsertEndRecord txNo indexName searchKey blockNo slotNo startLsn
            |> writeToLog logMgr
            |> Some
        | _ -> failwith "Logical start LogSeqNo is null (in logIndexInsertionEnd)"

    let logIndexDeletionEnd logMgr txNo logicalStartLogSeqNo indexName searchKey blockNo slotNo =
        match logicalStartLogSeqNo with
        | Some (startLsn) ->
            newIndexDeleteEndRecord txNo indexName searchKey blockNo slotNo startLsn
            |> writeToLog logMgr
            |> Some
        | _ -> failwith "Logical start LogSeqNo is null (in logIndexDeletionEnd)"

    let logIndexPageInsertion logMgr txNo isBranch indexBlkId keyType slotId =
        newIndexPageInsertRecord isBranch txNo indexBlkId keyType slotId
        |> writeToLog logMgr
        |> Some

    let logIndexPageDeletion logMgr txNo isBranch indexBlkId keyType slotId =
        newIndexPageDeleteRecord isBranch txNo indexBlkId keyType slotId
        |> writeToLog logMgr
        |> Some

    let logIndexPageInsertionClr logMgr isBranch compTxNo indexBlkId keyType slotId undoNextLSN =
        newIndexPageInsertClr isBranch compTxNo indexBlkId keyType slotId undoNextLSN
        |> writeToLog logMgr
        |> Some

    let logIndexPageDeletionClr logMgr isBranch compTxNo indexBlkId keyType slotId undoNextLSN =
        newIndexPageDeleteClr isBranch compTxNo indexBlkId keyType slotId undoNextLSN
        |> writeToLog logMgr
        |> Some

    let logSetValClr logMgr compTxNo buffer offset newVal undoNextLSN =
        let blk = buffer.BlockId()
        let (BlockId (fileName, _)) = blk
        if FileManager.isTempFile fileName then
            None
        else
            newSetValueClr compTxNo blk offset (buffer.GetVal offset (DbConstant.dbType newVal)) newVal undoNextLSN
            |> writeToLog logMgr
            |> Some

let newTransactionRecovery logMgr txNo isReadOnly =
    if not (isReadOnly)
    then newStartRecord txNo |> writeToLog logMgr |> ignore

    let mutable logicalStartLogSeqNo = None
    { Checkpoint = TransactionRecovery.checkpoint logMgr
      LogSetVal = TransactionRecovery.logSetVal logMgr txNo
      LogLogicalStart =
          fun () ->
              let lsn =
                  TransactionRecovery.logLogicalStart logMgr txNo

              logicalStartLogSeqNo <- lsn
              lsn
      LogLogicalAbort = TransactionRecovery.logLogicalAbort logMgr
      LogTableFileInsertionEnd =
          fun tableName blockNo slotNo ->
              let lsn =
                  TransactionRecovery.logTableFileInsertionEnd logMgr txNo logicalStartLogSeqNo tableName blockNo slotNo

              logicalStartLogSeqNo <- None
              lsn
      LogTableFileDeletionEnd =
          fun tableName blockNo slotNo ->
              let lsn =
                  TransactionRecovery.logTableFileDeletionEnd logMgr txNo logicalStartLogSeqNo tableName blockNo slotNo

              logicalStartLogSeqNo <- None
              lsn
      LogIndexInsertionEnd =
          fun indexName searchKey blockNo slotNo ->
              let lsn =
                  TransactionRecovery.logIndexInsertionEnd
                      logMgr
                      txNo
                      logicalStartLogSeqNo
                      indexName
                      searchKey
                      blockNo
                      slotNo

              logicalStartLogSeqNo <- None
              lsn
      LogIndexDeletionEnd =
          fun indexName searchKey blockNo slotNo ->
              let lsn =
                  TransactionRecovery.logIndexDeletionEnd
                      logMgr
                      txNo
                      logicalStartLogSeqNo
                      indexName
                      searchKey
                      blockNo
                      slotNo

              logicalStartLogSeqNo <- None
              lsn
      LogIndexPageInsertion = TransactionRecovery.logIndexPageInsertion logMgr txNo
      LogIndexPageDeletion = TransactionRecovery.logIndexPageDeletion logMgr txNo
      LogIndexPageInsertionClr = TransactionRecovery.logIndexPageInsertionClr logMgr
      LogIndexPageDeletionClr = TransactionRecovery.logIndexPageDeletionClr logMgr
      LogSetValClr = TransactionRecovery.logSetValClr logMgr }
