namespace RyakDB.Recovery

open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Storage.File
open RyakDB.Storage.Log
open RyakDB.Buffer
open RyakDB.Recovery.RecoveryLog
open RyakDB.Index

type RecoveryManager =
    { Checkpoint: int64 list -> LogSeqNo
      LogSetVal: Buffer -> int32 -> SqlConstant -> LogSeqNo option
      LogLogicalStart: unit -> LogSeqNo option
      LogLogicalAbort: int64 -> LogSeqNo -> LogSeqNo option
      LogRecordFileInsertionEnd: string -> int64 -> int32 -> LogSeqNo option
      LogRecordFileDeletionEnd: string -> int64 -> int32 -> LogSeqNo option
      LogIndexInsertionEnd: string -> SearchKey -> int64 -> int32 -> LogSeqNo option
      LogIndexDeletionEnd: string -> SearchKey -> int64 -> int32 -> LogSeqNo option
      LogIndexPageInsertion: bool -> BlockId -> SearchKeyType -> int32 -> LogSeqNo option
      LogIndexPageDeletion: bool -> BlockId -> SearchKeyType -> int32 -> LogSeqNo option
      LogIndexPageInsertionClr: bool -> int64 -> BlockId -> SearchKeyType -> int32 -> LogSeqNo -> LogSeqNo option
      LogIndexPageDeletionClr: bool -> int64 -> BlockId -> SearchKeyType -> int32 -> LogSeqNo -> LogSeqNo option
      LogSetValClr: int64 -> Buffer -> int32 -> SqlConstant -> LogSeqNo -> LogSeqNo option }

module RecoveryManager =
    type RecoveryManagerState =
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
                    (buffer.GetVal offset (SqlConstant.sqlType newVal))
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

    let logRecordFileInsertionEnd logMgr state tableName blockNo slotId =
        match state.LogicalStartLSN with
        | Some (startLsn) ->
            let record =
                RecoveryLog.newRecordFileInsertEndRecord state.TxNo tableName blockNo slotId startLsn

            let lsn = RecoveryLog.writeToLog logMgr record
            state.LogicalStartLSN <- None
            Some(lsn)
        | _ -> failwith "Logical start LSN is null (in logRecordFileInsertionEnd)"

    let logRecordFileDeletionEnd logMgr state tableName blockNo slotId =
        match state.LogicalStartLSN with
        | Some (startLsn) ->
            let record =
                RecoveryLog.newRecordFileDeleteEndRecord state.TxNo tableName blockNo slotId startLsn

            let lsn = RecoveryLog.writeToLog logMgr record
            state.LogicalStartLSN <- None
            Some(lsn)
        | _ -> failwith "Logical start LSN is null (in logRecordFileDeletionEnd)"

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
                    (buffer.GetVal offset (SqlConstant.sqlType newVal))
                    newVal
                    undoNextLSN

            let lsn = RecoveryLog.writeToLog logMgr record
            Some(lsn)

    let newRecoveryManager logMgr txNo isReadOnly =
        let state = { TxNo = txNo; LogicalStartLSN = None }

        if not (isReadOnly) then
            RecoveryLog.newStartRecord txNo
            |> RecoveryLog.writeToLog logMgr
            |> ignore

        { Checkpoint = checkpoint logMgr
          LogSetVal = logSetVal logMgr state
          LogLogicalStart = (fun () -> logLogicalStart logMgr state)
          LogLogicalAbort = logLogicalAbort logMgr
          LogRecordFileInsertionEnd = logRecordFileInsertionEnd logMgr state
          LogRecordFileDeletionEnd = logRecordFileDeletionEnd logMgr state
          LogIndexInsertionEnd = logIndexInsertionEnd logMgr state
          LogIndexDeletionEnd = logIndexDeletionEnd logMgr state
          LogIndexPageInsertion = logIndexPageInsertion logMgr state
          LogIndexPageDeletion = logIndexPageDeletion logMgr state
          LogIndexPageInsertionClr = logIndexPageInsertionClr logMgr
          LogIndexPageDeletionClr = logIndexPageDeletionClr logMgr
          LogSetValClr = logSetValClr logMgr }
