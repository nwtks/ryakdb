module RyakDB.Recovery.TransactionRecovery

open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Index
open RyakDB.Storage.File
open RyakDB.Storage.Log
open RyakDB.Buffer.Buffer
open RyakDB.Recovery.RecoveryLog

type TransactionRecovery =
    { LogLogicalStart: unit -> LogSeqNo
      LogTableFileInsertionEnd: string -> int64 -> int32 -> LogSeqNo
      LogTableFileDeletionEnd: string -> int64 -> int32 -> LogSeqNo
      LogIndexInsertionEnd: string -> SearchKey -> int64 -> int32 -> LogSeqNo
      LogIndexDeletionEnd: string -> SearchKey -> int64 -> int32 -> LogSeqNo
      LogIndexPageInsertion: bool -> SearchKeyType -> BlockId -> int32 -> LogSeqNo
      LogIndexPageDeletion: bool -> SearchKeyType -> BlockId -> int32 -> LogSeqNo
      LogSetVal: Buffer -> int32 -> DbConstant -> LogSeqNo option }

module TransactionRecovery =
    let logLogicalStart logService txNo =
        newLogicalStartRecord txNo
        |> writeToLog logService

    let logTableFileInsertionEnd logService txNo logicalStartLogSeqNo tableName blockNo slotNo =
        newTableFileInsertEndRecord txNo tableName blockNo slotNo logicalStartLogSeqNo
        |> writeToLog logService

    let logTableFileDeletionEnd logService txNo logicalStartLogSeqNo tableName blockNo slotNo =
        newTableFileDeleteEndRecord txNo tableName blockNo slotNo logicalStartLogSeqNo
        |> writeToLog logService

    let logIndexInsertionEnd logService txNo logicalStartLogSeqNo indexName searchKey blockNo slotNo =
        newIndexInsertEndRecord txNo indexName searchKey blockNo slotNo logicalStartLogSeqNo
        |> writeToLog logService

    let logIndexDeletionEnd logService txNo logicalStartLogSeqNo indexName searchKey blockNo slotNo =
        newIndexDeleteEndRecord txNo indexName searchKey blockNo slotNo logicalStartLogSeqNo
        |> writeToLog logService

    let logIndexPageInsertion logService txNo isBranch keyType blockId slotNo =
        newIndexPageInsertRecord txNo isBranch keyType blockId slotNo
        |> writeToLog logService

    let logIndexPageDeletion logService txNo isBranch keyType blockId slotNo =
        newIndexPageDeleteRecord txNo isBranch keyType blockId slotNo
        |> writeToLog logService

    let logSetVal logService txNo buffer offset newValue =
        let blockId = buffer.BlockId()
        if BlockId.fileName blockId |> FileService.isTempFile then
            None
        else
            newSetValueRecord txNo blockId offset (buffer.GetVal offset (DbConstant.dbType newValue)) newValue
            |> writeToLog logService
            |> Some

let newTransactionRecovery logService txNo isReadOnly =
    if not (isReadOnly) then
        newStartRecord txNo
        |> writeToLog logService
        |> ignore

    let mutable logicalStartLogSeqNo = None
    { LogLogicalStart =
          fun () ->
              let lsn =
                  TransactionRecovery.logLogicalStart logService txNo

              logicalStartLogSeqNo <- Some lsn
              lsn
      LogTableFileInsertionEnd =
          fun tableName blockNo slotNo ->
              match logicalStartLogSeqNo with
              | Some startLsn ->
                  logicalStartLogSeqNo <- None
                  TransactionRecovery.logTableFileInsertionEnd logService txNo startLsn tableName blockNo slotNo
              | _ -> failwith "No logical start LogSeqNo"
      LogTableFileDeletionEnd =
          fun tableName blockNo slotNo ->
              match logicalStartLogSeqNo with
              | Some startLsn ->
                  logicalStartLogSeqNo <- None
                  TransactionRecovery.logTableFileDeletionEnd logService txNo startLsn tableName blockNo slotNo
              | _ -> failwith "No logical start LogSeqNo"
      LogIndexInsertionEnd =
          fun indexName searchKey blockNo slotNo ->
              match logicalStartLogSeqNo with
              | Some startLsn ->
                  logicalStartLogSeqNo <- None
                  TransactionRecovery.logIndexInsertionEnd logService txNo startLsn indexName searchKey blockNo slotNo
              | _ -> failwith "No logical start LogSeqNo"
      LogIndexDeletionEnd =
          fun indexName searchKey blockNo slotNo ->
              match logicalStartLogSeqNo with
              | Some startLsn ->
                  logicalStartLogSeqNo <- None
                  TransactionRecovery.logIndexDeletionEnd logService txNo startLsn indexName searchKey blockNo slotNo
              | _ -> failwith "No logical start LogSeqNo"
      LogIndexPageInsertion = TransactionRecovery.logIndexPageInsertion logService txNo
      LogIndexPageDeletion = TransactionRecovery.logIndexPageDeletion logService txNo
      LogSetVal = TransactionRecovery.logSetVal logService txNo }
