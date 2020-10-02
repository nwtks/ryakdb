module RyakDB.Recovery.RecoveryLog

open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Index
open RyakDB.Storage.Log

type RecoveryLogOperation =
    | Start = -42
    | Commit = -43
    | Rollback = -44
    | Checkpoint = -41
    | LogicalStart = -61
    | LogicalAbort = -77
    | TableFileInsertEnd = -71
    | TableFileDeleteEnd = -72
    | IndexFileInsertEnd = -73
    | IndexFileDeleteEnd = -74
    | IndexPageInsert = -75
    | IndexPageInsertClear = -79
    | IndexPageDelete = -76
    | IndexPageDeleteClear = -80
    | SetValue = -62
    | SetValueClear = -78

type RecoveryLog =
    | StartRecord of txNo: int64 * lsn: LogSeqNo option
    | CommitRecord of txNo: int64 * lsn: LogSeqNo option
    | RollbackRecord of txNo: int64 * lsn: LogSeqNo option
    | CheckpointRecord of txNos: int64 list * lsn: LogSeqNo option
    | LogicalStartRecord of txNo: int64 * lsn: LogSeqNo option
    | LogicalAbortRecord of txNo: int64 * logicalStartLogSeqNo: LogSeqNo * lsn: LogSeqNo option
    | TableFileInsertEndRecord of txNo: int64 * tableName: string * blockNo: int64 * slotNo: int32 * logicalStartLogSeqNo: LogSeqNo * lsn: LogSeqNo option
    | TableFileDeleteEndRecord of txNo: int64 * tableName: string * blockNo: int64 * slotNo: int32 * logicalStartLogSeqNo: LogSeqNo * lsn: LogSeqNo option
    | IndexInsertEndRecord of txNo: int64 * indexName: string * searchKey: SearchKey * recordBlockNo: int64 * recordSlotNo: int32 * logicalStartLogSeqNo: LogSeqNo * lsn: LogSeqNo option
    | IndexDeleteEndRecord of txNo: int64 * indexName: string * searchKey: SearchKey * recordBlockNo: int64 * recordSlotNo: int32 * logicalStartLogSeqNo: LogSeqNo * lsn: LogSeqNo option
    | IndexPageInsertRecord of txNo: int64 * indexBlockId: BlockId * isBranch: bool * keyType: SearchKeyType * slot: int32 * lsn: LogSeqNo option
    | IndexPageInsertClear of compesationTxNo: int64 * indexBlockId: BlockId * isBranch: bool * keyType: SearchKeyType * slot: int32 * undoNextLogSeqNo: LogSeqNo * lsn: LogSeqNo option
    | IndexPageDeleteRecord of txNo: int64 * indexBlockId: BlockId * isBranch: bool * keyType: SearchKeyType * slot: int32 * lsn: LogSeqNo option
    | IndexPageDeleteClear of compesationTxNo: int64 * indexBlockId: BlockId * isBranch: bool * keyType: SearchKeyType * slot: int32 * undoNextLogSeqNo: LogSeqNo * lsn: LogSeqNo option
    | SetValueRecord of txNo: int64 * blockId: BlockId * offset: int32 * dbType: DbType * value: DbConstant * newValue: DbConstant * lsn: LogSeqNo option
    | SetValueClear of compesationTxNo: int64 * blockId: BlockId * offset: int32 * dbType: DbType * value: DbConstant * newValue: DbConstant * undoNextLogSeqNo: LogSeqNo * lsn: LogSeqNo option

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

let newLogicalAbortRecord txNo logicalStartLogSeqNo =
    LogicalAbortRecord(txNo, logicalStartLogSeqNo, None)

let newLogicalAbortRecordByLogRecord record =
    let txNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let logicalStartLogSeqNo =
        newLogSeqNo
            (record.NextVal BigIntDbType |> DbConstant.toLong)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    LogicalAbortRecord(txNo, logicalStartLogSeqNo, Some record.LogSeqNo)

let newTableFileInsertEndRecord txNo tblName blockNo slotNo logicalStartLogSeqNo =
    TableFileInsertEndRecord(txNo, tblName, blockNo, slotNo, logicalStartLogSeqNo, None)

let newTableFileInsertEndRecordByLogRecord record =
    let txNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let tblName =
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

    TableFileInsertEndRecord(txNo, tblName, blockNo, slotNo, logicalStartLogSeqNo, Some record.LogSeqNo)

let newTableFileDeleteEndRecord txNo tblName blockNo slotNo logicalStartLogSeqNo =
    TableFileDeleteEndRecord(txNo, tblName, blockNo, slotNo, logicalStartLogSeqNo, None)

let newTableFileDeleteEndRecordByLogRecord record =
    let txNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let tblName =
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

    TableFileDeleteEndRecord(txNo, tblName, blockNo, slotNo, logicalStartLogSeqNo, Some record.LogSeqNo)

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

let newIndexPageInsertRecord isBranch txNo indexBlockId keyType slot =
    IndexPageInsertRecord(txNo, indexBlockId, isBranch, keyType, slot, None)

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

    let slot =
        record.NextVal IntDbType |> DbConstant.toInt

    let count =
        record.NextVal IntDbType |> DbConstant.toInt

    let keyType =
        [ 1 .. count ]
        |> List.map (fun _ ->
            DbType.fromInt (record.NextVal IntDbType |> DbConstant.toInt) (record.NextVal IntDbType |> DbConstant.toInt))
        |> SearchKeyType.newSearchKeyTypeByTypes

    IndexPageInsertRecord(txNo, indexBlockId, isBranch, keyType, slot, Some record.LogSeqNo)

let newIndexPageInsertClear isBranch compTxNo indexBlockId keyType slot undoNextLogSeqNo =
    IndexPageInsertClear(compTxNo, indexBlockId, isBranch, keyType, slot, undoNextLogSeqNo, None)

let newIndexPageInsertClearByLogRecord record =
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

    let slot =
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

    IndexPageInsertClear(compTxNo, indexBlockId, isBranch, keyType, slot, undoNextLogSeqNo, Some record.LogSeqNo)

let newIndexPageDeleteRecord isBranch txNo indexBlockId keyType slot =
    IndexPageDeleteRecord(txNo, indexBlockId, isBranch, keyType, slot, None)

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

    let slot =
        record.NextVal IntDbType |> DbConstant.toInt

    let count =
        record.NextVal IntDbType |> DbConstant.toInt

    let keyType =
        [ 1 .. count ]
        |> List.map (fun _ ->
            DbType.fromInt (record.NextVal IntDbType |> DbConstant.toInt) (record.NextVal IntDbType |> DbConstant.toInt))
        |> SearchKeyType.newSearchKeyTypeByTypes

    IndexPageDeleteRecord(txNo, indexBlockId, isBranch, keyType, slot, Some record.LogSeqNo)

let newIndexPageDeleteClear isBranch compTxNo indexBlockId keyType slot undoNextLogSeqNo =
    IndexPageDeleteClear(compTxNo, indexBlockId, isBranch, keyType, slot, undoNextLogSeqNo, None)

let newIndexPageDeleteClearByLogRecord record =
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

    let slot =
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

    IndexPageDeleteClear(compTxNo, indexBlockId, isBranch, keyType, slot, undoNextLogSeqNo, Some record.LogSeqNo)

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

let newSetValueClear compTxNo blockId offset value newValue undoNextLogSeqNo =
    SetValueClear(compTxNo, blockId, offset, DbConstant.dbType value, value, newValue, undoNextLogSeqNo, None)

let newSetValueClearByLogRecord record =
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

    SetValueClear(txNo, blockId, offset, dbType, value, newValue, undoNextLogSeqNo, Some record.LogSeqNo)

let transactionNo record =
    match record with
    | StartRecord(txNo = n) -> n
    | CommitRecord(txNo = n) -> n
    | RollbackRecord(txNo = n) -> n
    | CheckpointRecord _ -> -1L
    | LogicalStartRecord(txNo = n) -> n
    | LogicalAbortRecord(txNo = n) -> n
    | TableFileInsertEndRecord(txNo = n) -> n
    | TableFileDeleteEndRecord(txNo = n) -> n
    | IndexInsertEndRecord(txNo = n) -> n
    | IndexDeleteEndRecord(txNo = n) -> n
    | IndexPageInsertRecord(txNo = n) -> n
    | IndexPageInsertClear(compesationTxNo = n) -> n
    | IndexPageDeleteRecord(txNo = n) -> n
    | IndexPageDeleteClear(compesationTxNo = n) -> n
    | SetValueRecord(txNo = n) -> n
    | SetValueClear(compesationTxNo = n) -> n

let getLogSeqNo record =
    match record with
    | StartRecord(lsn = n) -> n
    | CommitRecord(lsn = n) -> n
    | RollbackRecord(lsn = n) -> n
    | CheckpointRecord(lsn = n) -> n
    | LogicalStartRecord(lsn = n) -> n
    | LogicalAbortRecord(lsn = n) -> n
    | TableFileInsertEndRecord(lsn = n) -> n
    | TableFileDeleteEndRecord(lsn = n) -> n
    | IndexInsertEndRecord(lsn = n) -> n
    | IndexDeleteEndRecord(lsn = n) -> n
    | IndexPageInsertRecord(lsn = n) -> n
    | IndexPageInsertClear(lsn = n) -> n
    | IndexPageDeleteRecord(lsn = n) -> n
    | IndexPageDeleteClear(lsn = n) -> n
    | SetValueRecord(lsn = n) -> n
    | SetValueClear(lsn = n) -> n

let getLogicalStartLogSeqNo record =
    match record with
    | StartRecord _ -> None
    | CommitRecord _ -> None
    | RollbackRecord _ -> None
    | CheckpointRecord _ -> None
    | LogicalStartRecord _ -> None
    | LogicalAbortRecord(logicalStartLogSeqNo = lsn) -> Some lsn
    | TableFileInsertEndRecord(logicalStartLogSeqNo = lsn) -> Some lsn
    | TableFileDeleteEndRecord(logicalStartLogSeqNo = lsn) -> Some lsn
    | IndexInsertEndRecord(logicalStartLogSeqNo = lsn) -> Some lsn
    | IndexDeleteEndRecord(logicalStartLogSeqNo = lsn) -> Some lsn
    | IndexPageInsertRecord _ -> None
    | IndexPageInsertClear _ -> None
    | IndexPageDeleteRecord _ -> None
    | IndexPageDeleteClear _ -> None
    | SetValueRecord _ -> None
    | SetValueClear _ -> None

let getUndoNextLogSeqNo record =
    match record with
    | StartRecord _ -> None
    | CommitRecord _ -> None
    | RollbackRecord _ -> None
    | CheckpointRecord _ -> None
    | LogicalStartRecord _ -> None
    | LogicalAbortRecord _ -> None
    | TableFileInsertEndRecord _ -> None
    | TableFileDeleteEndRecord _ -> None
    | IndexInsertEndRecord _ -> None
    | IndexDeleteEndRecord _ -> None
    | IndexPageInsertRecord _ -> None
    | IndexPageInsertClear(undoNextLogSeqNo = lsn) -> Some lsn
    | IndexPageDeleteRecord _ -> None
    | IndexPageDeleteClear(undoNextLogSeqNo = lsn) -> Some lsn
    | SetValueRecord _ -> None
    | SetValueClear(undoNextLogSeqNo = lsn) -> Some lsn

let operation record =
    match record with
    | StartRecord _ -> RecoveryLogOperation.Start
    | CommitRecord _ -> RecoveryLogOperation.Commit
    | RollbackRecord _ -> RecoveryLogOperation.Rollback
    | CheckpointRecord _ -> RecoveryLogOperation.Checkpoint
    | LogicalStartRecord _ -> RecoveryLogOperation.LogicalStart
    | LogicalAbortRecord _ -> RecoveryLogOperation.LogicalAbort
    | TableFileInsertEndRecord _ -> RecoveryLogOperation.TableFileInsertEnd
    | TableFileDeleteEndRecord _ -> RecoveryLogOperation.TableFileDeleteEnd
    | IndexInsertEndRecord _ -> RecoveryLogOperation.IndexFileInsertEnd
    | IndexDeleteEndRecord _ -> RecoveryLogOperation.IndexFileDeleteEnd
    | IndexPageInsertRecord _ -> RecoveryLogOperation.IndexPageInsert
    | IndexPageInsertClear _ -> RecoveryLogOperation.IndexPageInsertClear
    | IndexPageDeleteRecord _ -> RecoveryLogOperation.IndexPageDelete
    | IndexPageDeleteClear _ -> RecoveryLogOperation.IndexPageDeleteClear
    | SetValueRecord _ -> RecoveryLogOperation.SetValue
    | SetValueClear _ -> RecoveryLogOperation.SetValueClear

let buildRecord record =
    let op =
        operation record |> int32 |> IntDbConstant

    let txNo = transactionNo record |> BigIntDbConstant

    match record with
    | CheckpointRecord(txNos = nums) ->
        [ op
          List.length nums |> IntDbConstant ]
        @ (nums |> List.map BigIntDbConstant)
    | LogicalAbortRecord (_, start, _) ->
        let (LogSeqNo (startBlockNo, startOffset)) = start
        [ op
          txNo
          BigIntDbConstant startBlockNo
          BigIntDbConstant startOffset ]
    | TableFileInsertEndRecord (_, name, blockNo, slot, start, _) ->
        let (LogSeqNo (startBlockNo, startOffset)) = start
        [ op
          txNo
          DbConstant.newVarchar name
          BigIntDbConstant blockNo
          IntDbConstant slot
          BigIntDbConstant startBlockNo
          BigIntDbConstant startOffset ]
    | TableFileDeleteEndRecord (_, name, blockNo, slot, start, _) ->
        let (LogSeqNo (startBlockNo, startOffset)) = start
        [ op
          txNo
          DbConstant.newVarchar name
          BigIntDbConstant blockNo
          IntDbConstant slot
          BigIntDbConstant startBlockNo
          BigIntDbConstant startOffset ]
    | IndexInsertEndRecord (_, name, sk, blockNo, slot, start, _) ->
        let (LogSeqNo (startBlockNo, startOffset)) = start
        let (SearchKey key) = sk
        [ op
          txNo
          DbConstant.newVarchar name
          BigIntDbConstant blockNo
          IntDbConstant slot
          BigIntDbConstant startBlockNo
          BigIntDbConstant startOffset
          List.length key |> IntDbConstant ]
        @ (key
           |> List.collect (fun v ->
               let t = DbConstant.dbType v
               [ DbType.toInt t |> IntDbConstant
                 DbType.argument t |> IntDbConstant
                 v ]))
    | IndexDeleteEndRecord (_, name, sk, blockNo, slot, start, _) ->
        let (LogSeqNo (startBlockNo, startOffset)) = start
        let (SearchKey key) = sk
        [ op
          txNo
          DbConstant.newVarchar name
          BigIntDbConstant blockNo
          IntDbConstant slot
          BigIntDbConstant startBlockNo
          BigIntDbConstant startOffset
          List.length key |> IntDbConstant ]
        @ (key
           |> List.collect (fun v ->
               let t = DbConstant.dbType v
               [ DbType.toInt t |> IntDbConstant
                 DbType.argument t |> IntDbConstant
                 v ]))
    | IndexPageInsertRecord (_, bid, branch, kt, slot, _) ->
        let (BlockId (fileName, blockNo)) = bid
        let (SearchKeyType keyType) = kt
        [ op
          txNo
          (if branch then 1 else 0) |> IntDbConstant
          DbConstant.newVarchar fileName
          BigIntDbConstant blockNo
          IntDbConstant slot
          List.length keyType |> IntDbConstant ]
        @ (keyType
           |> List.collect (fun t ->
               [ DbType.toInt t |> IntDbConstant
                 DbType.argument t |> IntDbConstant ]))
    | IndexPageInsertClear (_, bid, branch, kt, slot, undo, _) ->
        let (BlockId (fileName, blockNo)) = bid
        let (LogSeqNo (undoBlockNo, undoOffset)) = undo
        let (SearchKeyType keyType) = kt
        [ op
          txNo
          (if branch then 1 else 0) |> IntDbConstant
          DbConstant.newVarchar fileName
          BigIntDbConstant blockNo
          IntDbConstant slot
          BigIntDbConstant undoBlockNo
          BigIntDbConstant undoOffset
          List.length keyType |> IntDbConstant ]
        @ (keyType
           |> List.collect (fun t ->
               [ DbType.toInt t |> IntDbConstant
                 DbType.argument t |> IntDbConstant ]))
    | IndexPageDeleteRecord (_, bid, branch, kt, slot, _) ->
        let (BlockId (fileName, blockNo)) = bid
        let (SearchKeyType keyType) = kt
        [ op
          txNo
          (if branch then 1 else 0) |> IntDbConstant
          DbConstant.newVarchar fileName
          BigIntDbConstant blockNo
          IntDbConstant slot
          List.length keyType |> IntDbConstant ]
        @ (keyType
           |> List.collect (fun t ->
               [ DbType.toInt t |> IntDbConstant
                 DbType.argument t |> IntDbConstant ]))
    | IndexPageDeleteClear (_, bid, branch, kt, slot, undo, _) ->
        let (BlockId (fileName, blockNo)) = bid
        let (LogSeqNo (undoBlockNo, undoOffset)) = undo
        let (SearchKeyType keyType) = kt
        [ op
          txNo
          (if branch then 1 else 0) |> IntDbConstant
          DbConstant.newVarchar fileName
          BigIntDbConstant blockNo
          IntDbConstant slot
          BigIntDbConstant undoBlockNo
          BigIntDbConstant undoOffset
          List.length keyType |> IntDbConstant ]
        @ (keyType
           |> List.collect (fun t ->
               [ DbType.toInt t |> IntDbConstant
                 DbType.argument t |> IntDbConstant ]))
    | SetValueRecord (_, bid, off, t, v, nv, _) ->
        let (BlockId (fileName, blockNo)) = bid
        [ op
          txNo
          DbConstant.newVarchar fileName
          BigIntDbConstant blockNo
          IntDbConstant off
          DbType.toInt t |> IntDbConstant
          DbType.argument t |> IntDbConstant
          v
          nv ]
    | SetValueClear (_, bid, off, t, v, nv, undo, _) ->
        let (BlockId (fileName, blockNo)) = bid
        let (LogSeqNo (undoBlockNo, undoOffset)) = undo
        [ op
          txNo
          DbConstant.newVarchar fileName
          BigIntDbConstant blockNo
          IntDbConstant off
          DbType.toInt t |> IntDbConstant
          DbType.argument t |> IntDbConstant
          v
          nv
          BigIntDbConstant undoBlockNo
          BigIntDbConstant undoOffset ]
    | _ -> [ op; txNo ]

let writeToLog logMgr record = buildRecord record |> logMgr.Append

let fromLogRecord record =
    let operation =
        record.NextVal IntDbType
        |> DbConstant.toInt
        |> enum<RecoveryLogOperation>

    match operation with
    | RecoveryLogOperation.Start -> newStartRecordByLogRecord record
    | RecoveryLogOperation.Commit -> newCommitRecordByLogRecord record
    | RecoveryLogOperation.Rollback -> newRollbackRecordByLogRecord record
    | RecoveryLogOperation.Checkpoint -> newCheckpointRecordByLogRecord record
    | RecoveryLogOperation.LogicalStart -> newLogicalStartRecordByLogRecord record
    | RecoveryLogOperation.LogicalAbort -> newLogicalAbortRecordByLogRecord record
    | RecoveryLogOperation.TableFileInsertEnd -> newTableFileInsertEndRecordByLogRecord record
    | RecoveryLogOperation.TableFileDeleteEnd -> newTableFileDeleteEndRecordByLogRecord record
    | RecoveryLogOperation.IndexFileInsertEnd -> newIndexInsertEndRecordByLogRecord record
    | RecoveryLogOperation.IndexFileDeleteEnd -> newIndexDeleteEndRecordByLogRecord record
    | RecoveryLogOperation.IndexPageInsert -> newIndexPageInsertRecordByLogRecord record
    | RecoveryLogOperation.IndexPageInsertClear -> newIndexPageInsertClearByLogRecord record
    | RecoveryLogOperation.IndexPageDelete -> newIndexPageDeleteRecordByLogRecord record
    | RecoveryLogOperation.IndexPageDeleteClear -> newIndexPageDeleteClearByLogRecord record
    | RecoveryLogOperation.SetValue -> newSetValueRecordByLogRecord record
    | RecoveryLogOperation.SetValueClear -> newSetValueClearByLogRecord record
    | _ -> failwith ("Not supported operation:" + operation.ToString())
