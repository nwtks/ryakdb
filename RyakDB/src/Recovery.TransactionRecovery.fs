module RyakDB.Recovery.TransactionRecovery

open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Storage.File
open RyakDB.Storage.Log
open RyakDB.Buffer.Buffer
open RyakDB.Recovery
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
    type TransactionRecoveryState =
        { TxNo: int64
          mutable LogicalStartLSN: LogSeqNo option }

    let checkpoint logMgr txNos =
        RecoveryLog.newCheckpointRecord txNos
        |> RecoveryLog.writeToLog logMgr

    let logSetVal logMgr state buffer offset newVal =
        let blk = buffer.BlockId()
        let (BlockId (fileName, _)) = blk
        if FileManager.isTempFile fileName then
            None
        else
            let record =
                RecoveryLog.newSetValueRecord
                    state.TxNo
                    blk
                    offset
                    (buffer.GetVal offset (DbConstant.dbType newVal))
                    newVal

            let lsn = RecoveryLog.writeToLog logMgr record
            Some(lsn)

    let logLogicalStart logMgr state =
        let record =
            RecoveryLog.newLogicalStartRecord state.TxNo

        let lsn = RecoveryLog.writeToLog logMgr record
        state.LogicalStartLSN <- Some(lsn)
        state.LogicalStartLSN

    let logLogicalAbort logMgr txNo undoNextLSN =
        let record =
            RecoveryLog.newLogicalAbortRecord txNo undoNextLSN

        let lsn = RecoveryLog.writeToLog logMgr record
        Some(lsn)

    let logTableFileInsertionEnd logMgr state tableName blockNo slotId =
        match state.LogicalStartLSN with
        | Some (startLsn) ->
            let record =
                RecoveryLog.newTableFileInsertEndRecord state.TxNo tableName blockNo slotId startLsn

            let lsn = RecoveryLog.writeToLog logMgr record
            state.LogicalStartLSN <- None
            Some(lsn)
        | _ -> failwith "Logical start LSN is null (in logTableFileInsertionEnd)"

    let logTableFileDeletionEnd logMgr state tableName blockNo slotId =
        match state.LogicalStartLSN with
        | Some (startLsn) ->
            let record =
                RecoveryLog.newTableFileDeleteEndRecord state.TxNo tableName blockNo slotId startLsn

            let lsn = RecoveryLog.writeToLog logMgr record
            state.LogicalStartLSN <- None
            Some(lsn)
        | _ -> failwith "Logical start LSN is null (in logTableFileDeletionEnd)"

    let logIndexInsertionEnd logMgr state indexName searchKey recordBlockNo recordSlotId =
        match state.LogicalStartLSN with
        | Some (startLsn) ->
            let record =
                RecoveryLog.newIndexInsertEndRecord state.TxNo indexName searchKey recordBlockNo recordSlotId startLsn

            let lsn = RecoveryLog.writeToLog logMgr record
            state.LogicalStartLSN <- None
            Some(lsn)
        | _ -> failwith "Logical start LSN is null (in logIndexInsertionEnd)"

    let logIndexDeletionEnd logMgr state indexName searchKey recordBlockNo recordSlotId =
        match state.LogicalStartLSN with
        | Some (startLsn) ->
            let record =
                RecoveryLog.newIndexDeleteEndRecord state.TxNo indexName searchKey recordBlockNo recordSlotId startLsn

            let lsn = RecoveryLog.writeToLog logMgr record
            state.LogicalStartLSN <- None
            Some(lsn)
        | _ -> failwith "Logical start LSN is null (in logIndexDeletionEnd)"

    let logIndexPageInsertion logMgr state isDirPage indexBlkId keyType slotId =
        let record =
            RecoveryLog.newIndexPageInsertRecord isDirPage state.TxNo indexBlkId keyType slotId

        let lsn = RecoveryLog.writeToLog logMgr record
        Some(lsn)

    let logIndexPageDeletion logMgr state isDirPage indexBlkId keyType slotId =
        let record =
            RecoveryLog.newIndexPageDeleteRecord isDirPage state.TxNo indexBlkId keyType slotId

        let lsn = RecoveryLog.writeToLog logMgr record
        Some(lsn)

    let logIndexPageInsertionClr logMgr isDirPage compTxNo indexBlkId keyType slotId undoNextLSN =
        let record =
            RecoveryLog.newIndexPageInsertClr isDirPage compTxNo indexBlkId keyType slotId undoNextLSN

        let lsn = RecoveryLog.writeToLog logMgr record
        Some(lsn)

    let logIndexPageDeletionClr logMgr isDirPage compTxNo indexBlkId keyType slotId undoNextLSN =
        let record =
            RecoveryLog.newIndexPageDeleteClr isDirPage compTxNo indexBlkId keyType slotId undoNextLSN

        let lsn = RecoveryLog.writeToLog logMgr record
        Some(lsn)

    let logSetValClr logMgr compTxNo buffer offset newVal undoNextLSN =
        let blk = buffer.BlockId()
        let (BlockId (fileName, _)) = blk
        if FileManager.isTempFile fileName then
            None
        else
            let record =
                RecoveryLog.newSetValueClr
                    compTxNo
                    blk
                    offset
                    (buffer.GetVal offset (DbConstant.dbType newVal))
                    newVal
                    undoNextLSN

            let lsn = RecoveryLog.writeToLog logMgr record
            Some(lsn)

let newTransactionRecovery logMgr txNo isReadOnly =
    let state :TransactionRecovery.TransactionRecoveryState= { TxNo = txNo; LogicalStartLSN = None }

    if not (isReadOnly) then
        newStartRecord txNo
        |> writeToLog logMgr
        |> ignore

    { Checkpoint = TransactionRecovery.checkpoint logMgr
      LogSetVal = TransactionRecovery.logSetVal logMgr state
      LogLogicalStart = (fun () -> TransactionRecovery.logLogicalStart logMgr state)
      LogLogicalAbort = TransactionRecovery.logLogicalAbort logMgr
      LogTableFileInsertionEnd = TransactionRecovery.logTableFileInsertionEnd logMgr state
      LogTableFileDeletionEnd = TransactionRecovery.logTableFileDeletionEnd logMgr state
      LogIndexInsertionEnd = TransactionRecovery.logIndexInsertionEnd logMgr state
      LogIndexDeletionEnd = TransactionRecovery.logIndexDeletionEnd logMgr state
      LogIndexPageInsertion = TransactionRecovery.logIndexPageInsertion logMgr state
      LogIndexPageDeletion = TransactionRecovery.logIndexPageDeletion logMgr state
      LogIndexPageInsertionClr = TransactionRecovery.logIndexPageInsertionClr logMgr
      LogIndexPageDeletionClr = TransactionRecovery.logIndexPageDeletionClr logMgr
      LogSetValClr = TransactionRecovery.logSetValClr logMgr }
