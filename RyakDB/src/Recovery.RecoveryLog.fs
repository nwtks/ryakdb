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
    | IndexPageInsertClear of completedTxNo: int64 * indexBlockId: BlockId * isBranch: bool * keyType: SearchKeyType * slot: int32 * undoNextLogSeqNo: LogSeqNo * lsn: LogSeqNo option
    | IndexPageDeleteRecord of txNo: int64 * indexBlockId: BlockId * isBranch: bool * keyType: SearchKeyType * slot: int32 * lsn: LogSeqNo option
    | IndexPageDeleteClear of completedTxNo: int64 * indexBlockId: BlockId * isBranch: bool * keyType: SearchKeyType * slot: int32 * undoNextLogSeqNo: LogSeqNo * lsn: LogSeqNo option
    | SetValueRecord of txNo: int64 * blockId: BlockId * offset: int32 * dbType: DbType * value: DbConstant * newValue: DbConstant * lsn: LogSeqNo option
    | SetValueClear of completedTxNo: int64 * blockId: BlockId * offset: int32 * dbType: DbType * value: DbConstant * newValue: DbConstant * undoNextLogSeqNo: LogSeqNo * lsn: LogSeqNo option

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
        record.NextVal(VarcharDbType 0)
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
        record.NextVal(VarcharDbType 0)
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
        record.NextVal(VarcharDbType 0)
        |> DbConstant.toString

    let count =
        record.NextVal IntDbType |> DbConstant.toInt

    let searchKey =
        [ 1 .. count ]
        |> List.map (fun _ ->
            DbType.fromInt (record.NextVal IntDbType |> DbConstant.toInt) (record.NextVal IntDbType |> DbConstant.toInt)
            |> record.NextVal)
        |> SearchKey.newSearchKey

    let recordBlockNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let recordSlotNo =
        record.NextVal IntDbType |> DbConstant.toInt

    let logicalStartLogSeqNo =
        newLogSeqNo
            (record.NextVal BigIntDbType |> DbConstant.toLong)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    IndexInsertEndRecord
        (txNo, indexName, searchKey, recordBlockNo, recordSlotNo, logicalStartLogSeqNo, Some record.LogSeqNo)

let newIndexDeleteEndRecord txNo indexName searchKey recordBlockNo recordSlotId logicalStartLogSeqNo =
    IndexDeleteEndRecord(txNo, indexName, searchKey, recordBlockNo, recordSlotId, logicalStartLogSeqNo, None)

let newIndexDeleteEndRecordByLogRecord record =
    let txNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let indexName =
        record.NextVal(VarcharDbType 0)
        |> DbConstant.toString

    let count =
        record.NextVal IntDbType |> DbConstant.toInt

    let searchKey =
        [ 1 .. count ]
        |> List.map (fun _ ->
            DbType.fromInt (record.NextVal IntDbType |> DbConstant.toInt) (record.NextVal IntDbType |> DbConstant.toInt)
            |> record.NextVal)
        |> SearchKey.newSearchKey

    let recordBlockNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let recordSlotId =
        record.NextVal IntDbType |> DbConstant.toInt

    let logicalStartLogSeqNo =
        newLogSeqNo
            (record.NextVal BigIntDbType |> DbConstant.toLong)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    IndexDeleteEndRecord
        (txNo, indexName, searchKey, recordBlockNo, recordSlotId, logicalStartLogSeqNo, Some record.LogSeqNo)

let newIndexPageInsertRecord isBranch txNo indexBlockId keyType slot =
    IndexPageInsertRecord(txNo, indexBlockId, isBranch, keyType, slot, None)

let newIndexPageInsertRecordByLogRecord record =
    let txNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let isBranch =
        (record.NextVal IntDbType |> DbConstant.toInt) = 1

    let count =
        record.NextVal IntDbType |> DbConstant.toInt

    let keyType =
        [ 1 .. count ]
        |> List.map (fun _ ->
            DbType.fromInt (record.NextVal IntDbType |> DbConstant.toInt) (record.NextVal IntDbType |> DbConstant.toInt))
        |> SearchKeyType.newSearchKeyTypeByTypes

    let indexBlockId =
        BlockId.newBlockId
            (record.NextVal(VarcharDbType 0)
             |> DbConstant.toString)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    let slot =
        record.NextVal IntDbType |> DbConstant.toInt

    IndexPageInsertRecord(txNo, indexBlockId, isBranch, keyType, slot, Some record.LogSeqNo)

let newIndexPageInsertClear isBranch compTxNo indexBlockId keyType slot undoNextLogSeqNo =
    IndexPageInsertClear(compTxNo, indexBlockId, isBranch, keyType, slot, undoNextLogSeqNo, None)

let newIndexPageInsertClearByLogRecord record =
    let compTxNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let isBranch =
        (record.NextVal IntDbType |> DbConstant.toInt) = 1

    let count =
        record.NextVal IntDbType |> DbConstant.toInt

    let keyType =
        [ 1 .. count ]
        |> List.map (fun _ ->
            DbType.fromInt (record.NextVal IntDbType |> DbConstant.toInt) (record.NextVal IntDbType |> DbConstant.toInt))
        |> SearchKeyType.newSearchKeyTypeByTypes

    let indexBlockId =
        BlockId.newBlockId
            (record.NextVal(VarcharDbType 0)
             |> DbConstant.toString)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    let slot =
        record.NextVal IntDbType |> DbConstant.toInt

    let undoNextLogSeqNo =
        newLogSeqNo
            (record.NextVal BigIntDbType |> DbConstant.toLong)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    IndexPageInsertClear(compTxNo, indexBlockId, isBranch, keyType, slot, undoNextLogSeqNo, Some record.LogSeqNo)

let newIndexPageDeleteRecord isBranch txNo indexBlockId keyType slot =
    IndexPageDeleteRecord(txNo, indexBlockId, isBranch, keyType, slot, None)

let newIndexPageDeleteRecordByLogRecord record =
    let txNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let isBranch =
        (record.NextVal IntDbType |> DbConstant.toInt) = 1

    let count =
        record.NextVal IntDbType |> DbConstant.toInt

    let keyType =
        [ 1 .. count ]
        |> List.map (fun _ ->
            DbType.fromInt (record.NextVal IntDbType |> DbConstant.toInt) (record.NextVal IntDbType |> DbConstant.toInt))
        |> SearchKeyType.newSearchKeyTypeByTypes

    let indexBlockId =
        BlockId.newBlockId
            (record.NextVal(VarcharDbType 0)
             |> DbConstant.toString)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    let slot =
        record.NextVal IntDbType |> DbConstant.toInt

    IndexPageDeleteRecord(txNo, indexBlockId, isBranch, keyType, slot, Some record.LogSeqNo)

let newIndexPageDeleteClear isBranch compTxNo indexBlockId keyType slot undoNextLogSeqNo =
    IndexPageDeleteClear(compTxNo, indexBlockId, isBranch, keyType, slot, undoNextLogSeqNo, None)

let newIndexPageDeleteClearByLogRecord record =
    let compTxNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let isBranch =
        (record.NextVal IntDbType |> DbConstant.toInt) = 1

    let count =
        record.NextVal IntDbType |> DbConstant.toInt

    let keyType =
        [ 1 .. count ]
        |> List.map (fun _ ->
            DbType.fromInt (record.NextVal IntDbType |> DbConstant.toInt) (record.NextVal IntDbType |> DbConstant.toInt))
        |> SearchKeyType.newSearchKeyTypeByTypes

    let indexBlockId =
        BlockId.newBlockId
            (record.NextVal(VarcharDbType 0)
             |> DbConstant.toString)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    let slot =
        record.NextVal IntDbType |> DbConstant.toInt

    let undoNextLogSeqNo =
        newLogSeqNo
            (record.NextVal BigIntDbType |> DbConstant.toLong)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    IndexPageDeleteClear(compTxNo, indexBlockId, isBranch, keyType, slot, undoNextLogSeqNo, Some record.LogSeqNo)

let newSetValueRecord txNo blockId offset value newValue =
    SetValueRecord(txNo, blockId, offset, DbConstant.dbType value, value, newValue, None)

let newSetValueRecordByLogRecord record =
    let txNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let blockId =
        BlockId.newBlockId
            (record.NextVal(VarcharDbType 0)
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
            (record.NextVal(VarcharDbType 0)
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

let transactionNo r =
    match r with
    | StartRecord(txNo = n) -> n
    | CommitRecord(txNo = n) -> n
    | RollbackRecord(txNo = n) -> n
    | CheckpointRecord (_) -> -1L
    | LogicalStartRecord(txNo = n) -> n
    | LogicalAbortRecord(txNo = n) -> n
    | TableFileInsertEndRecord(txNo = n) -> n
    | TableFileDeleteEndRecord(txNo = n) -> n
    | IndexInsertEndRecord(txNo = n) -> n
    | IndexDeleteEndRecord(txNo = n) -> n
    | IndexPageInsertRecord(txNo = n) -> n
    | IndexPageInsertClear(completedTxNo = n) -> n
    | IndexPageDeleteRecord(txNo = n) -> n
    | IndexPageDeleteClear(completedTxNo = n) -> n
    | SetValueRecord(txNo = n) -> n
    | SetValueClear(completedTxNo = n) -> n

let getLogSeqNo r =
    match r with
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

let operation r =
    match r with
    | StartRecord (_) -> RecoveryLogOperation.Start
    | CommitRecord (_) -> RecoveryLogOperation.Commit
    | RollbackRecord (_) -> RecoveryLogOperation.Rollback
    | CheckpointRecord (_) -> RecoveryLogOperation.Checkpoint
    | LogicalStartRecord (_) -> RecoveryLogOperation.LogicalStart
    | LogicalAbortRecord (_) -> RecoveryLogOperation.LogicalAbort
    | TableFileInsertEndRecord (_) -> RecoveryLogOperation.TableFileInsertEnd
    | TableFileDeleteEndRecord (_) -> RecoveryLogOperation.TableFileDeleteEnd
    | IndexInsertEndRecord (_) -> RecoveryLogOperation.IndexFileInsertEnd
    | IndexDeleteEndRecord (_) -> RecoveryLogOperation.IndexFileDeleteEnd
    | IndexPageInsertRecord (_) -> RecoveryLogOperation.IndexPageInsert
    | IndexPageInsertClear (_) -> RecoveryLogOperation.IndexPageInsertClear
    | IndexPageDeleteRecord (_) -> RecoveryLogOperation.IndexPageDelete
    | IndexPageDeleteClear (_) -> RecoveryLogOperation.IndexPageDeleteClear
    | SetValueRecord (_) -> RecoveryLogOperation.SetValue
    | SetValueClear (_) -> RecoveryLogOperation.SetValueClear

let buildRecord r =
    match r with
    | CheckpointRecord(txNos = nums) ->
        let list =
            nums |> List.map (fun n -> BigIntDbConstant(n))

        IntDbConstant(int (operation r))
        :: IntDbConstant(List.length nums)
        :: list
    | LogicalAbortRecord (n, start, _) ->
        let (LogSeqNo (blockNo, offset)) = start
        [ IntDbConstant(int (operation r))
          BigIntDbConstant(n)
          BigIntDbConstant(blockNo)
          BigIntDbConstant(offset) ]
    | TableFileInsertEndRecord (n, tn, bn, sid, start, _) ->
        let (LogSeqNo (blockNo, offset)) = start
        [ IntDbConstant(int (operation r))
          BigIntDbConstant(n)
          DbConstant.newVarchar tn
          BigIntDbConstant(bn)
          IntDbConstant(sid)
          BigIntDbConstant(blockNo)
          BigIntDbConstant(offset) ]
    | TableFileDeleteEndRecord (n, tn, bn, sid, start, _) ->
        let (LogSeqNo (blockNo, offset)) = start
        [ IntDbConstant(int (operation r))
          BigIntDbConstant(n)
          DbConstant.newVarchar tn
          BigIntDbConstant(bn)
          IntDbConstant(sid)
          BigIntDbConstant(blockNo)
          BigIntDbConstant(offset) ]
    | IndexInsertEndRecord (n, inm, sk, bn, sid, start, _) ->
        let (LogSeqNo (blockNo, offset)) = start
        let (SearchKey (key)) = sk
        IntDbConstant(int (operation r))
        :: BigIntDbConstant(n)
        :: DbConstant.newVarchar inm
        :: BigIntDbConstant(bn)
        :: IntDbConstant(sid)
        :: BigIntDbConstant(blockNo)
        :: BigIntDbConstant(offset)
        :: IntDbConstant(List.length key)
        :: key
        |> List.collect (fun v ->
            let t = DbConstant.dbType v
            [ IntDbConstant(DbType.toInt t)
              IntDbConstant(DbType.argument t)
              v ])
    | IndexDeleteEndRecord (n, inm, sk, bn, sid, start, _) ->
        let (LogSeqNo (blockNo, offset)) = start
        let (SearchKey (key)) = sk
        IntDbConstant(int (operation r))
        :: BigIntDbConstant(n)
        :: DbConstant.newVarchar inm
        :: BigIntDbConstant(bn)
        :: IntDbConstant(sid)
        :: BigIntDbConstant(blockNo)
        :: BigIntDbConstant(offset)
        :: IntDbConstant(List.length key)
        :: key
        |> List.collect (fun v ->
            let t = DbConstant.dbType v
            [ IntDbConstant(DbType.toInt t)
              IntDbConstant(DbType.argument t)
              v ])
    | IndexPageInsertRecord (n, ibid, branch, kt, sid, _) ->
        let (BlockId (fileName, blockNo)) = ibid
        let (SearchKeyType (keyType)) = kt
        IntDbConstant(int (operation r))
        :: BigIntDbConstant(n)
        :: IntDbConstant(if branch then 1 else 0)
        :: DbConstant.newVarchar fileName
        :: BigIntDbConstant(blockNo)
        :: IntDbConstant(sid)
        :: IntDbConstant(List.length keyType)
        :: (keyType
            |> List.collect (fun t ->
                [ IntDbConstant(DbType.toInt t)
                  IntDbConstant(DbType.argument t) ]))
    | IndexPageInsertClear (n, ibid, branch, kt, sid, undo, _) ->
        let (BlockId (fileName, blockNo)) = ibid
        let (LogSeqNo (undoBlockNo, undoOffset)) = undo
        let (SearchKeyType (keyType)) = kt
        IntDbConstant(int (operation r))
        :: BigIntDbConstant(n)
        :: IntDbConstant(if branch then 1 else 0)
        :: DbConstant.newVarchar fileName
        :: BigIntDbConstant(blockNo)
        :: IntDbConstant(sid)
        :: BigIntDbConstant(undoBlockNo)
        :: BigIntDbConstant(undoOffset)
        :: IntDbConstant(List.length keyType)
        :: (keyType
            |> List.collect (fun t ->
                [ IntDbConstant(DbType.toInt t)
                  IntDbConstant(DbType.argument t) ]))
    | IndexPageDeleteRecord (n, ibid, branch, kt, sid, _) ->
        let (BlockId (fileName, blockNo)) = ibid
        let (SearchKeyType (keyType)) = kt
        IntDbConstant(int (operation r))
        :: BigIntDbConstant(n)
        :: IntDbConstant(if branch then 1 else 0)
        :: DbConstant.newVarchar fileName
        :: BigIntDbConstant(blockNo)
        :: IntDbConstant(sid)
        :: IntDbConstant(List.length keyType)
        :: (keyType
            |> List.collect (fun t ->
                [ IntDbConstant(DbType.toInt t)
                  IntDbConstant(DbType.argument t) ]))
    | IndexPageDeleteClear (n, ibid, branch, kt, sid, undo, _) ->
        let (BlockId (fileName, blockNo)) = ibid
        let (LogSeqNo (undoBlockNo, undoOffset)) = undo
        let (SearchKeyType (keyType)) = kt
        IntDbConstant(int (operation r))
        :: BigIntDbConstant(n)
        :: IntDbConstant(if branch then 1 else 0)
        :: DbConstant.newVarchar fileName
        :: BigIntDbConstant(blockNo)
        :: IntDbConstant(sid)
        :: BigIntDbConstant(undoBlockNo)
        :: BigIntDbConstant(undoOffset)
        :: IntDbConstant(List.length keyType)
        :: (keyType
            |> List.collect (fun t ->
                [ IntDbConstant(DbType.toInt t)
                  IntDbConstant(DbType.argument t) ]))
    | SetValueRecord (n, bid, off, t, v, nv, _) ->
        let (BlockId (fileName, blockNo)) = bid
        [ IntDbConstant(int (operation r))
          BigIntDbConstant(n)
          DbConstant.newVarchar fileName
          BigIntDbConstant(blockNo)
          IntDbConstant(off)
          IntDbConstant(DbType.toInt t)
          IntDbConstant(DbType.argument t)
          v
          nv ]
    | SetValueClear (n, bid, off, t, v, nv, undo, _) ->
        let (BlockId (fileName, blockNo)) = bid
        let (LogSeqNo (undoBlockNo, undoOffset)) = undo
        [ IntDbConstant(int (operation r))
          BigIntDbConstant(n)
          DbConstant.newVarchar fileName
          BigIntDbConstant(blockNo)
          IntDbConstant(off)
          IntDbConstant(DbType.toInt t)
          IntDbConstant(DbType.argument t)
          v
          nv
          BigIntDbConstant(undoBlockNo)
          BigIntDbConstant(undoOffset) ]
    | _ ->
        [ IntDbConstant(int (operation r))
          BigIntDbConstant(transactionNo r) ]

let writeToLog logMgr r = buildRecord r |> logMgr.Append

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
