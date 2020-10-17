module RyakDB.Recovery.RecoveryLog

open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Index
open RyakDB.Storage.Log

type RecoveryLogOperation =
    | Start = -41
    | Commit = -42
    | Rollback = -43
    | Checkpoint = -44
    | LogicalStart = -51
    | LogicalUndo = -52
    | TableFileInsertEnd = -71
    | TableFileDeleteEnd = -72
    | IndexFileInsertEnd = -73
    | IndexFileDeleteEnd = -74
    | IndexPageInsert = -75
    | IndexPageInsertCompensate = -76
    | IndexPageDelete = -77
    | IndexPageDeleteCompensate = -78
    | SetValue = -61
    | SetValueCompensate = -62

type RecoveryLog =
    | StartRecord of txNo: int64 * lsn: LogSeqNo option
    | CommitRecord of txNo: int64 * lsn: LogSeqNo option
    | RollbackRecord of txNo: int64 * lsn: LogSeqNo option
    | CheckpointRecord of txNos: int64 list * lsn: LogSeqNo option
    | LogicalStartRecord of txNo: int64 * lsn: LogSeqNo option
    | LogicalUndoRecord of txNo: int64 * logicalStartLogSeqNo: LogSeqNo * lsn: LogSeqNo option
    | TableFileInsertEndRecord of txNo: int64 * tableName: string * blockNo: int64 * slotNo: int32 * logicalStartLogSeqNo: LogSeqNo * lsn: LogSeqNo option
    | TableFileDeleteEndRecord of txNo: int64 * tableName: string * blockNo: int64 * slotNo: int32 * logicalStartLogSeqNo: LogSeqNo * lsn: LogSeqNo option
    | IndexInsertEndRecord of txNo: int64 * indexName: string * searchKey: SearchKey * recordBlockNo: int64 * recordSlotNo: int32 * logicalStartLogSeqNo: LogSeqNo * lsn: LogSeqNo option
    | IndexDeleteEndRecord of txNo: int64 * indexName: string * searchKey: SearchKey * recordBlockNo: int64 * recordSlotNo: int32 * logicalStartLogSeqNo: LogSeqNo * lsn: LogSeqNo option
    | IndexPageInsertRecord of txNo: int64 * isBranch: bool * keyType: SearchKeyType * indexBlockId: BlockId * indexSlotNo: int32 * lsn: LogSeqNo option
    | IndexPageInsertCompensateRecord of compensateTxNo: int64 * isBranch: bool * keyType: SearchKeyType * indexBlockId: BlockId * indexSlotNo: int32 * undoNextLogSeqNo: LogSeqNo * lsn: LogSeqNo option
    | IndexPageDeleteRecord of txNo: int64 * isBranch: bool * keyType: SearchKeyType * indexBlockId: BlockId * indexSlotNo: int32 * lsn: LogSeqNo option
    | IndexPageDeleteCompensateRecord of compensateTxNo: int64 * isBranch: bool * keyType: SearchKeyType * indexBlockId: BlockId * indexSlotNo: int32 * undoNextLogSeqNo: LogSeqNo * lsn: LogSeqNo option
    | SetValueRecord of txNo: int64 * blockId: BlockId * offset: int32 * dbType: DbType * oldValue: DbConstant * newValue: DbConstant * lsn: LogSeqNo option
    | SetValueCompensateRecord of compensateTxNo: int64 * blockId: BlockId * offset: int32 * dbType: DbType * oldValue: DbConstant * newValue: DbConstant * undoNextLogSeqNo: LogSeqNo * lsn: LogSeqNo option

let newStartRecord txNo = StartRecord(txNo, None)

let newStartRecordByLogRecord record =
    StartRecord(record.NextVal BigIntDbType |> DbConstant.toLong, Some record.LogSeqNo)

let newCommitRecord txNo = CommitRecord(txNo, None)

let newCommitRecordByLogRecord record =
    CommitRecord(record.NextVal BigIntDbType |> DbConstant.toLong, Some record.LogSeqNo)

let newRollbackRecord txNo = RollbackRecord(txNo, None)

let newRollbackRecordByLogRecord record =
    RollbackRecord(record.NextVal BigIntDbType |> DbConstant.toLong, Some record.LogSeqNo)

let newCheckpointRecord txNos = CheckpointRecord(txNos, None)

let newCheckpointRecordByLogRecord record =
    let count =
        record.NextVal IntDbType |> DbConstant.toInt

    let txNos =
        [ 1 .. count ]
        |> List.map (fun _ -> record.NextVal BigIntDbType |> DbConstant.toLong)

    CheckpointRecord(txNos, Some record.LogSeqNo)

let newLogicalStartRecord txNo = LogicalStartRecord(txNo, None)

let newLogicalStartRecordByLogRecord record =
    LogicalStartRecord(record.NextVal BigIntDbType |> DbConstant.toLong, Some record.LogSeqNo)

let newLogicalUndoRecord txNo logicalStartLogSeqNo =
    LogicalUndoRecord(txNo, logicalStartLogSeqNo, None)

let newLogicalUndoRecordByLogRecord record =
    let txNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let logicalStartLogSeqNo =
        newLogSeqNo
            (record.NextVal BigIntDbType |> DbConstant.toLong)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    LogicalUndoRecord(txNo, logicalStartLogSeqNo, Some record.LogSeqNo)

let newTableFileInsertEndRecord txNo tableName blockNo slotNo logicalStartLogSeqNo =
    TableFileInsertEndRecord(txNo, tableName, blockNo, slotNo, logicalStartLogSeqNo, None)

let newTableFileInsertEndRecordByLogRecord record =
    let txNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let tableName =
        VarcharDbType 0
        |> record.NextVal
        |> DbConstant.toString

    let blockNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let slotNo =
        record.NextVal IntDbType |> DbConstant.toInt

    let logicalStartLogSeqNo =
        newLogSeqNo
            (record.NextVal BigIntDbType |> DbConstant.toLong)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    TableFileInsertEndRecord(txNo, tableName, blockNo, slotNo, logicalStartLogSeqNo, Some record.LogSeqNo)

let newTableFileDeleteEndRecord txNo tableName blockNo slotNo logicalStartLogSeqNo =
    TableFileDeleteEndRecord(txNo, tableName, blockNo, slotNo, logicalStartLogSeqNo, None)

let newTableFileDeleteEndRecordByLogRecord record =
    let txNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let tableName =
        VarcharDbType 0
        |> record.NextVal
        |> DbConstant.toString

    let blockNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let slotNo =
        record.NextVal IntDbType |> DbConstant.toInt

    let logicalStartLogSeqNo =
        newLogSeqNo
            (record.NextVal BigIntDbType |> DbConstant.toLong)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    TableFileDeleteEndRecord(txNo, tableName, blockNo, slotNo, logicalStartLogSeqNo, Some record.LogSeqNo)

let newIndexInsertEndRecord txNo indexName searchKey recordBlockNo recordSlotNo logicalStartLogSeqNo =
    IndexInsertEndRecord(txNo, indexName, searchKey, recordBlockNo, recordSlotNo, logicalStartLogSeqNo, None)

let newIndexInsertEndRecordByLogRecord record =
    let txNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let indexName =
        VarcharDbType 0
        |> record.NextVal
        |> DbConstant.toString

    let recordBlockNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let recordSlotNo =
        record.NextVal IntDbType |> DbConstant.toInt

    let logicalStartLogSeqNo =
        newLogSeqNo
            (record.NextVal BigIntDbType |> DbConstant.toLong)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    let count =
        record.NextVal IntDbType |> DbConstant.toInt

    let searchKey =
        [ 1 .. count ]
        |> List.map (fun _ ->
            DbType.fromInt (record.NextVal IntDbType |> DbConstant.toInt) (record.NextVal IntDbType |> DbConstant.toInt)
            |> record.NextVal)
        |> SearchKey.newSearchKey

    IndexInsertEndRecord
        (txNo, indexName, searchKey, recordBlockNo, recordSlotNo, logicalStartLogSeqNo, Some record.LogSeqNo)

let newIndexDeleteEndRecord txNo indexName searchKey recordBlockNo recordSlotId logicalStartLogSeqNo =
    IndexDeleteEndRecord(txNo, indexName, searchKey, recordBlockNo, recordSlotId, logicalStartLogSeqNo, None)

let newIndexDeleteEndRecordByLogRecord record =
    let txNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let indexName =
        VarcharDbType 0
        |> record.NextVal
        |> DbConstant.toString

    let recordBlockNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let recordSlotNo =
        record.NextVal IntDbType |> DbConstant.toInt

    let logicalStartLogSeqNo =
        newLogSeqNo
            (record.NextVal BigIntDbType |> DbConstant.toLong)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    let count =
        record.NextVal IntDbType |> DbConstant.toInt

    let searchKey =
        [ 1 .. count ]
        |> List.map (fun _ ->
            DbType.fromInt (record.NextVal IntDbType |> DbConstant.toInt) (record.NextVal IntDbType |> DbConstant.toInt)
            |> record.NextVal)
        |> SearchKey.newSearchKey

    IndexDeleteEndRecord
        (txNo, indexName, searchKey, recordBlockNo, recordSlotNo, logicalStartLogSeqNo, Some record.LogSeqNo)

let newIndexPageInsertRecord txNo isBranch keyType indexBlockId indexSlotNo =
    IndexPageInsertRecord(txNo, isBranch, keyType, indexBlockId, indexSlotNo, None)

let newIndexPageInsertRecordByLogRecord record =
    let txNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let isBranch =
        record.NextVal IntDbType |> DbConstant.toInt = 1

    let indexBlockId =
        BlockId.newBlockId
            (VarcharDbType 0
             |> record.NextVal
             |> DbConstant.toString)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    let indexSlotNo =
        record.NextVal IntDbType |> DbConstant.toInt

    let count =
        record.NextVal IntDbType |> DbConstant.toInt

    let keyType =
        [ 1 .. count ]
        |> List.map (fun _ ->
            DbType.fromInt (record.NextVal IntDbType |> DbConstant.toInt) (record.NextVal IntDbType |> DbConstant.toInt))
        |> SearchKeyType.newSearchKeyTypeByTypes

    IndexPageInsertRecord(txNo, isBranch, keyType, indexBlockId, indexSlotNo, Some record.LogSeqNo)

let newIndexPageInsertCompensateRecord compTxNo isBranch keyType indexBlockId indexSlotNo undoNextLogSeqNo =
    IndexPageInsertCompensateRecord(compTxNo, isBranch, keyType, indexBlockId, indexSlotNo, undoNextLogSeqNo, None)

let newIndexPageInsertCompensateRecordByLogRecord record =
    let compTxNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let isBranch =
        record.NextVal IntDbType |> DbConstant.toInt = 1

    let indexBlockId =
        BlockId.newBlockId
            (VarcharDbType 0
             |> record.NextVal
             |> DbConstant.toString)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    let indexSlotNo =
        record.NextVal IntDbType |> DbConstant.toInt

    let undoNextLogSeqNo =
        newLogSeqNo
            (record.NextVal BigIntDbType |> DbConstant.toLong)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    let count =
        record.NextVal IntDbType |> DbConstant.toInt

    let keyType =
        [ 1 .. count ]
        |> List.map (fun _ ->
            DbType.fromInt (record.NextVal IntDbType |> DbConstant.toInt) (record.NextVal IntDbType |> DbConstant.toInt))
        |> SearchKeyType.newSearchKeyTypeByTypes

    IndexPageInsertCompensateRecord
        (compTxNo, isBranch, keyType, indexBlockId, indexSlotNo, undoNextLogSeqNo, Some record.LogSeqNo)

let newIndexPageDeleteRecord txNo isBranch keyType indexBlockId indexSlotNo =
    IndexPageDeleteRecord(txNo, isBranch, keyType, indexBlockId, indexSlotNo, None)

let newIndexPageDeleteRecordByLogRecord record =
    let txNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let isBranch =
        record.NextVal IntDbType |> DbConstant.toInt = 1

    let indexBlockId =
        BlockId.newBlockId
            (VarcharDbType 0
             |> record.NextVal
             |> DbConstant.toString)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    let indexSlotNo =
        record.NextVal IntDbType |> DbConstant.toInt

    let count =
        record.NextVal IntDbType |> DbConstant.toInt

    let keyType =
        [ 1 .. count ]
        |> List.map (fun _ ->
            DbType.fromInt (record.NextVal IntDbType |> DbConstant.toInt) (record.NextVal IntDbType |> DbConstant.toInt))
        |> SearchKeyType.newSearchKeyTypeByTypes

    IndexPageDeleteRecord(txNo, isBranch, keyType, indexBlockId, indexSlotNo, Some record.LogSeqNo)

let newIndexPageDeleteCompensateRecord compTxNo isBranch keyType indexBlockId indexSlotNo undoNextLogSeqNo =
    IndexPageDeleteCompensateRecord(compTxNo, isBranch, keyType, indexBlockId, indexSlotNo, undoNextLogSeqNo, None)

let newIndexPageDeleteCompensateRecordByLogRecord record =
    let compTxNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let isBranch =
        record.NextVal IntDbType |> DbConstant.toInt = 1

    let indexBlockId =
        BlockId.newBlockId
            (VarcharDbType 0
             |> record.NextVal
             |> DbConstant.toString)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    let indexSlotNo =
        record.NextVal IntDbType |> DbConstant.toInt

    let undoNextLogSeqNo =
        newLogSeqNo
            (record.NextVal BigIntDbType |> DbConstant.toLong)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    let count =
        record.NextVal IntDbType |> DbConstant.toInt

    let keyType =
        [ 1 .. count ]
        |> List.map (fun _ ->
            DbType.fromInt (record.NextVal IntDbType |> DbConstant.toInt) (record.NextVal IntDbType |> DbConstant.toInt))
        |> SearchKeyType.newSearchKeyTypeByTypes

    IndexPageDeleteCompensateRecord
        (compTxNo, isBranch, keyType, indexBlockId, indexSlotNo, undoNextLogSeqNo, Some record.LogSeqNo)

let newSetValueRecord txNo blockId offset value newValue =
    SetValueRecord(txNo, blockId, offset, DbConstant.dbType value, value, newValue, None)

let newSetValueRecordByLogRecord record =
    let txNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let blockId =
        BlockId.newBlockId
            (VarcharDbType 0
             |> record.NextVal
             |> DbConstant.toString)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    let offset =
        record.NextVal IntDbType |> DbConstant.toInt

    let dbType =
        DbType.fromInt (record.NextVal IntDbType |> DbConstant.toInt) (record.NextVal IntDbType |> DbConstant.toInt)

    let value = record.NextVal dbType
    let newValue = record.NextVal dbType
    SetValueRecord(txNo, blockId, offset, dbType, value, newValue, Some record.LogSeqNo)

let newSetValueCompensateRecord compTxNo blockId offset value newValue undoNextLogSeqNo =
    SetValueCompensateRecord
        (compTxNo, blockId, offset, DbConstant.dbType value, value, newValue, undoNextLogSeqNo, None)

let newSetValueCompensateRecordByLogRecord record =
    let txNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let blockId =
        BlockId.newBlockId
            (VarcharDbType 0
             |> record.NextVal
             |> DbConstant.toString)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    let offset =
        record.NextVal IntDbType |> DbConstant.toInt

    let dbType =
        DbType.fromInt (record.NextVal IntDbType |> DbConstant.toInt) (record.NextVal IntDbType |> DbConstant.toInt)

    let value = record.NextVal dbType
    let newValue = record.NextVal dbType

    let undoNextLogSeqNo =
        newLogSeqNo
            (record.NextVal BigIntDbType |> DbConstant.toLong)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    SetValueCompensateRecord(txNo, blockId, offset, dbType, value, newValue, undoNextLogSeqNo, Some record.LogSeqNo)

let transactionNo record =
    match record with
    | CheckpointRecord _ -> -1L
    | StartRecord(txNo = txNo)
    | CommitRecord(txNo = txNo)
    | RollbackRecord(txNo = txNo)
    | LogicalStartRecord(txNo = txNo)
    | LogicalUndoRecord(txNo = txNo)
    | TableFileInsertEndRecord(txNo = txNo)
    | TableFileDeleteEndRecord(txNo = txNo)
    | IndexInsertEndRecord(txNo = txNo)
    | IndexDeleteEndRecord(txNo = txNo)
    | IndexPageInsertRecord(txNo = txNo)
    | IndexPageInsertCompensateRecord(compensateTxNo = txNo)
    | IndexPageDeleteRecord(txNo = txNo)
    | IndexPageDeleteCompensateRecord(compensateTxNo = txNo)
    | SetValueRecord(txNo = txNo)
    | SetValueCompensateRecord(compensateTxNo = txNo) -> txNo

let getLogSeqNo record =
    match record with
    | StartRecord(lsn = lsn)
    | CommitRecord(lsn = lsn)
    | RollbackRecord(lsn = lsn)
    | CheckpointRecord(lsn = lsn)
    | LogicalStartRecord(lsn = lsn)
    | LogicalUndoRecord(lsn = lsn)
    | TableFileInsertEndRecord(lsn = lsn)
    | TableFileDeleteEndRecord(lsn = lsn)
    | IndexInsertEndRecord(lsn = lsn)
    | IndexDeleteEndRecord(lsn = lsn)
    | IndexPageInsertRecord(lsn = lsn)
    | IndexPageInsertCompensateRecord(lsn = lsn)
    | IndexPageDeleteRecord(lsn = lsn)
    | IndexPageDeleteCompensateRecord(lsn = lsn)
    | SetValueRecord(lsn = lsn)
    | SetValueCompensateRecord(lsn = lsn) -> lsn

let getLogicalStartLogSeqNo record =
    match record with
    | LogicalUndoRecord(logicalStartLogSeqNo = startLsn)
    | TableFileInsertEndRecord(logicalStartLogSeqNo = startLsn)
    | TableFileDeleteEndRecord(logicalStartLogSeqNo = startLsn)
    | IndexInsertEndRecord(logicalStartLogSeqNo = startLsn)
    | IndexDeleteEndRecord(logicalStartLogSeqNo = startLsn) -> Some startLsn
    | _ -> None

let getUndoNextLogSeqNo record =
    match record with
    | IndexPageInsertCompensateRecord(undoNextLogSeqNo = undoNextLsn)
    | IndexPageDeleteCompensateRecord(undoNextLogSeqNo = undoNextLsn)
    | SetValueCompensateRecord(undoNextLogSeqNo = undoNextLsn) -> Some undoNextLsn
    | _ -> None

let operation record =
    match record with
    | StartRecord _ -> RecoveryLogOperation.Start
    | CommitRecord _ -> RecoveryLogOperation.Commit
    | RollbackRecord _ -> RecoveryLogOperation.Rollback
    | CheckpointRecord _ -> RecoveryLogOperation.Checkpoint
    | LogicalStartRecord _ -> RecoveryLogOperation.LogicalStart
    | LogicalUndoRecord _ -> RecoveryLogOperation.LogicalUndo
    | TableFileInsertEndRecord _ -> RecoveryLogOperation.TableFileInsertEnd
    | TableFileDeleteEndRecord _ -> RecoveryLogOperation.TableFileDeleteEnd
    | IndexInsertEndRecord _ -> RecoveryLogOperation.IndexFileInsertEnd
    | IndexDeleteEndRecord _ -> RecoveryLogOperation.IndexFileDeleteEnd
    | IndexPageInsertRecord _ -> RecoveryLogOperation.IndexPageInsert
    | IndexPageInsertCompensateRecord _ -> RecoveryLogOperation.IndexPageInsertCompensate
    | IndexPageDeleteRecord _ -> RecoveryLogOperation.IndexPageDelete
    | IndexPageDeleteCompensateRecord _ -> RecoveryLogOperation.IndexPageDeleteCompensate
    | SetValueRecord _ -> RecoveryLogOperation.SetValue
    | SetValueCompensateRecord _ -> RecoveryLogOperation.SetValueCompensate

let buildRecord record =
    let op =
        operation record |> int32 |> IntDbConstant

    let txNo = transactionNo record |> BigIntDbConstant

    match record with
    | CheckpointRecord(txNos = txNos) ->
        [ op
          List.length txNos |> IntDbConstant ]
        @ (txNos |> List.map BigIntDbConstant)
    | LogicalUndoRecord (_, LogSeqNo (startBlockNo, startOffset), _) ->
        [ op
          txNo
          BigIntDbConstant startBlockNo
          BigIntDbConstant startOffset ]
    | TableFileInsertEndRecord (_, name, blockNo, slotNo, LogSeqNo (startBlockNo, startOffset), _) ->
        [ op
          txNo
          DbConstant.newVarchar name
          BigIntDbConstant blockNo
          IntDbConstant slotNo
          BigIntDbConstant startBlockNo
          BigIntDbConstant startOffset ]
    | TableFileDeleteEndRecord (_, name, blockNo, slotNo, LogSeqNo (startBlockNo, startOffset), _) ->
        [ op
          txNo
          DbConstant.newVarchar name
          BigIntDbConstant blockNo
          IntDbConstant slotNo
          BigIntDbConstant startBlockNo
          BigIntDbConstant startOffset ]
    | IndexInsertEndRecord (_, name, SearchKey key, blockNo, slotNo, LogSeqNo (startBlockNo, startOffset), _) ->
        [ op
          txNo
          DbConstant.newVarchar name
          BigIntDbConstant blockNo
          IntDbConstant slotNo
          BigIntDbConstant startBlockNo
          BigIntDbConstant startOffset
          List.length key |> IntDbConstant ]
        @ (key
           |> List.collect (fun v ->
               let t = DbConstant.dbType v
               [ DbType.toInt t |> IntDbConstant
                 DbType.argument t |> IntDbConstant
                 v ]))
    | IndexDeleteEndRecord (_, name, SearchKey key, blockNo, slotNo, LogSeqNo (startBlockNo, startOffset), _) ->
        [ op
          txNo
          DbConstant.newVarchar name
          BigIntDbConstant blockNo
          IntDbConstant slotNo
          BigIntDbConstant startBlockNo
          BigIntDbConstant startOffset
          List.length key |> IntDbConstant ]
        @ (key
           |> List.collect (fun v ->
               let t = DbConstant.dbType v
               [ DbType.toInt t |> IntDbConstant
                 DbType.argument t |> IntDbConstant
                 v ]))
    | IndexPageInsertRecord (_, branch, SearchKeyType keyType, BlockId (fileName, blockNo), slotNo, _) ->
        [ op
          txNo
          (if branch then 1 else 0) |> IntDbConstant
          DbConstant.newVarchar fileName
          BigIntDbConstant blockNo
          IntDbConstant slotNo
          List.length keyType |> IntDbConstant ]
        @ (keyType
           |> List.collect (fun t ->
               [ DbType.toInt t |> IntDbConstant
                 DbType.argument t |> IntDbConstant ]))
    | IndexPageInsertCompensateRecord (_,
                                       branch,
                                       SearchKeyType keyType,
                                       BlockId (fileName, blockNo),
                                       slotNo,
                                       LogSeqNo (undoBlockNo, undoOffset),
                                       _) ->
        [ op
          txNo
          (if branch then 1 else 0) |> IntDbConstant
          DbConstant.newVarchar fileName
          BigIntDbConstant blockNo
          IntDbConstant slotNo
          BigIntDbConstant undoBlockNo
          BigIntDbConstant undoOffset
          List.length keyType |> IntDbConstant ]
        @ (keyType
           |> List.collect (fun t ->
               [ DbType.toInt t |> IntDbConstant
                 DbType.argument t |> IntDbConstant ]))
    | IndexPageDeleteRecord (_, branch, SearchKeyType keyType, BlockId (fileName, blockNo), slotNo, _) ->
        [ op
          txNo
          (if branch then 1 else 0) |> IntDbConstant
          DbConstant.newVarchar fileName
          BigIntDbConstant blockNo
          IntDbConstant slotNo
          List.length keyType |> IntDbConstant ]
        @ (keyType
           |> List.collect (fun t ->
               [ DbType.toInt t |> IntDbConstant
                 DbType.argument t |> IntDbConstant ]))
    | IndexPageDeleteCompensateRecord (_,
                                       branch,
                                       SearchKeyType keyType,
                                       BlockId (fileName, blockNo),
                                       slotNo,
                                       LogSeqNo (undoBlockNo, undoOffset),
                                       _) ->
        [ op
          txNo
          (if branch then 1 else 0) |> IntDbConstant
          DbConstant.newVarchar fileName
          BigIntDbConstant blockNo
          IntDbConstant slotNo
          BigIntDbConstant undoBlockNo
          BigIntDbConstant undoOffset
          List.length keyType |> IntDbConstant ]
        @ (keyType
           |> List.collect (fun t ->
               [ DbType.toInt t |> IntDbConstant
                 DbType.argument t |> IntDbConstant ]))
    | SetValueRecord (_, BlockId (fileName, blockNo), offset, dbType, oldValue, newValue, _) ->
        [ op
          txNo
          DbConstant.newVarchar fileName
          BigIntDbConstant blockNo
          IntDbConstant offset
          DbType.toInt dbType |> IntDbConstant
          DbType.argument dbType |> IntDbConstant
          oldValue
          newValue ]
    | SetValueCompensateRecord (_,
                                BlockId (fileName, blockNo),
                                offset,
                                dbType,
                                oldValue,
                                newValue,
                                LogSeqNo (undoBlockNo, undoOffset),
                                _) ->
        [ op
          txNo
          DbConstant.newVarchar fileName
          BigIntDbConstant blockNo
          IntDbConstant offset
          DbType.toInt dbType |> IntDbConstant
          DbType.argument dbType |> IntDbConstant
          oldValue
          newValue
          BigIntDbConstant undoBlockNo
          BigIntDbConstant undoOffset ]
    | _ -> [ op; txNo ]

let writeToLog logService record = buildRecord record |> logService.Append

let fromLogRecord record =
    let operation =
        record.NextVal IntDbType |> DbConstant.toInt

    match operation |> enum<RecoveryLogOperation> with
    | RecoveryLogOperation.Start -> newStartRecordByLogRecord record
    | RecoveryLogOperation.Commit -> newCommitRecordByLogRecord record
    | RecoveryLogOperation.Rollback -> newRollbackRecordByLogRecord record
    | RecoveryLogOperation.Checkpoint -> newCheckpointRecordByLogRecord record
    | RecoveryLogOperation.LogicalStart -> newLogicalStartRecordByLogRecord record
    | RecoveryLogOperation.LogicalUndo -> newLogicalUndoRecordByLogRecord record
    | RecoveryLogOperation.TableFileInsertEnd -> newTableFileInsertEndRecordByLogRecord record
    | RecoveryLogOperation.TableFileDeleteEnd -> newTableFileDeleteEndRecordByLogRecord record
    | RecoveryLogOperation.IndexFileInsertEnd -> newIndexInsertEndRecordByLogRecord record
    | RecoveryLogOperation.IndexFileDeleteEnd -> newIndexDeleteEndRecordByLogRecord record
    | RecoveryLogOperation.IndexPageInsert -> newIndexPageInsertRecordByLogRecord record
    | RecoveryLogOperation.IndexPageInsertCompensate -> newIndexPageInsertCompensateRecordByLogRecord record
    | RecoveryLogOperation.IndexPageDelete -> newIndexPageDeleteRecordByLogRecord record
    | RecoveryLogOperation.IndexPageDeleteCompensate -> newIndexPageDeleteCompensateRecordByLogRecord record
    | RecoveryLogOperation.SetValue -> newSetValueRecordByLogRecord record
    | RecoveryLogOperation.SetValueCompensate -> newSetValueCompensateRecordByLogRecord record
    | _ -> failwith ("Not supported operation:" + operation.ToString())
