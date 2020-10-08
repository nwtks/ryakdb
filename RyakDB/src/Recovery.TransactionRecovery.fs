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
    let logLogicalStart logMgr txNo =
        newLogicalStartRecord txNo |> writeToLog logMgr

    let logTableFileInsertionEnd logMgr txNo logicalStartLogSeqNo tableName blockNo slotNo =
        match logicalStartLogSeqNo with
        | Some startLsn ->
            newTableFileInsertEndRecord txNo tableName blockNo slotNo startLsn
            |> writeToLog logMgr
        | _ -> failwith "Logical start LogSeqNo is null (in logTableFileInsertionEnd)"

    let logTableFileDeletionEnd logMgr txNo logicalStartLogSeqNo tableName blockNo slotNo =
        match logicalStartLogSeqNo with
        | Some startLsn ->
            newTableFileDeleteEndRecord txNo tableName blockNo slotNo startLsn
            |> writeToLog logMgr
        | _ -> failwith "Logical start LogSeqNo is null (in logTableFileDeletionEnd)"

    let logIndexInsertionEnd logMgr txNo logicalStartLogSeqNo indexName searchKey blockNo slotNo =
        match logicalStartLogSeqNo with
        | Some startLsn ->
            newIndexInsertEndRecord txNo indexName searchKey blockNo slotNo startLsn
            |> writeToLog logMgr
        | _ -> failwith "Logical start LogSeqNo is null (in logIndexInsertionEnd)"

    let logIndexDeletionEnd logMgr txNo logicalStartLogSeqNo indexName searchKey blockNo slotNo =
        match logicalStartLogSeqNo with
        | Some startLsn ->
            newIndexDeleteEndRecord txNo indexName searchKey blockNo slotNo startLsn
            |> writeToLog logMgr
        | _ -> failwith "Logical start LogSeqNo is null (in logIndexDeletionEnd)"

    let logIndexPageInsertion logMgr txNo isBranch indexBlockId keyType slot =
        newIndexPageInsertRecord isBranch txNo indexBlockId keyType slot
        |> writeToLog logMgr

    let logIndexPageDeletion logMgr txNo isBranch indexBlockId keyType slot =
        newIndexPageDeleteRecord isBranch txNo indexBlockId keyType slot
        |> writeToLog logMgr

    let logSetVal logMgr txNo buffer offset newValue =
        let blockId = buffer.BlockId()
        let (BlockId (fileName, _)) = blockId
        if FileManager.isTempFile fileName then
            None
        else
            newSetValueRecord txNo blockId offset (buffer.GetVal offset (DbConstant.dbType newValue)) newValue
            |> writeToLog logMgr
            |> Some

let newTransactionRecovery logMgr txNo isReadOnly =
    if not (isReadOnly)
    then newStartRecord txNo |> writeToLog logMgr |> ignore

    let mutable logicalStartLogSeqNo = None
    { LogLogicalStart =
          fun () ->
              let lsn =
                  TransactionRecovery.logLogicalStart logMgr txNo

              logicalStartLogSeqNo <- Some lsn
              lsn
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
      LogSetVal = TransactionRecovery.logSetVal logMgr txNo }
