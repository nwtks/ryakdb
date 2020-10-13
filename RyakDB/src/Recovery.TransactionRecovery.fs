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
      LogIndexPageInsertion: bool -> BlockId -> SearchKeyType -> int32 -> LogSeqNo
      LogIndexPageDeletion: bool -> BlockId -> SearchKeyType -> int32 -> LogSeqNo
      LogSetVal: Buffer -> int32 -> DbConstant -> LogSeqNo option }

module TransactionRecovery =
    let logLogicalStart logService txNo =
        newLogicalStartRecord txNo
        |> writeToLog logService

    let logTableFileInsertionEnd logService txNo logicalStartLogSeqNo tableName blockNo slotNo =
        match logicalStartLogSeqNo with
        | Some startLsn ->
            newTableFileInsertEndRecord txNo tableName blockNo slotNo startLsn
            |> writeToLog logService
        | _ -> failwith "Logical start LogSeqNo is null (in logTableFileInsertionEnd)"

    let logTableFileDeletionEnd logService txNo logicalStartLogSeqNo tableName blockNo slotNo =
        match logicalStartLogSeqNo with
        | Some startLsn ->
            newTableFileDeleteEndRecord txNo tableName blockNo slotNo startLsn
            |> writeToLog logService
        | _ -> failwith "Logical start LogSeqNo is null (in logTableFileDeletionEnd)"

    let logIndexInsertionEnd logService txNo logicalStartLogSeqNo indexName searchKey blockNo slotNo =
        match logicalStartLogSeqNo with
        | Some startLsn ->
            newIndexInsertEndRecord txNo indexName searchKey blockNo slotNo startLsn
            |> writeToLog logService
        | _ -> failwith "Logical start LogSeqNo is null (in logIndexInsertionEnd)"

    let logIndexDeletionEnd logService txNo logicalStartLogSeqNo indexName searchKey blockNo slotNo =
        match logicalStartLogSeqNo with
        | Some startLsn ->
            newIndexDeleteEndRecord txNo indexName searchKey blockNo slotNo startLsn
            |> writeToLog logService
        | _ -> failwith "Logical start LogSeqNo is null (in logIndexDeletionEnd)"

    let logIndexPageInsertion logService txNo isBranch indexBlockId keyType slot =
        newIndexPageInsertRecord isBranch txNo indexBlockId keyType slot
        |> writeToLog logService

    let logIndexPageDeletion logService txNo isBranch indexBlockId keyType slot =
        newIndexPageDeleteRecord isBranch txNo indexBlockId keyType slot
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
              let lsn =
                  TransactionRecovery.logTableFileInsertionEnd
                      logService
                      txNo
                      logicalStartLogSeqNo
                      tableName
                      blockNo
                      slotNo

              logicalStartLogSeqNo <- None
              lsn
      LogTableFileDeletionEnd =
          fun tableName blockNo slotNo ->
              let lsn =
                  TransactionRecovery.logTableFileDeletionEnd
                      logService
                      txNo
                      logicalStartLogSeqNo
                      tableName
                      blockNo
                      slotNo

              logicalStartLogSeqNo <- None
              lsn
      LogIndexInsertionEnd =
          fun indexName searchKey blockNo slotNo ->
              let lsn =
                  TransactionRecovery.logIndexInsertionEnd
                      logService
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
                      logService
                      txNo
                      logicalStartLogSeqNo
                      indexName
                      searchKey
                      blockNo
                      slotNo

              logicalStartLogSeqNo <- None
              lsn
      LogIndexPageInsertion = TransactionRecovery.logIndexPageInsertion logService txNo
      LogIndexPageDeletion = TransactionRecovery.logIndexPageDeletion logService txNo
      LogSetVal = TransactionRecovery.logSetVal logService txNo }
