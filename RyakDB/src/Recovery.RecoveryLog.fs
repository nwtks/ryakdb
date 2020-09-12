module RyakDB.Recovery.RecoveryLog

open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Storage.Log
open RyakDB.Index

type RecoveryLogOp =
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
    | IndexPageInsertClr = -79
    | IndexPageDelete = -76
    | IndexPageDeleteClr = -80
    | SetValue = -62
    | SetValueClr = -78

type RecoveryLog =
    | StartRecord of txNo: int64 * lsn: LogSeqNo option
    | CommitRecord of txNo: int64 * lsn: LogSeqNo option
    | RollbackRecord of txNo: int64 * lsn: LogSeqNo option
    | CheckpointRecord of txNos: int64 list * lsn: LogSeqNo option
    | LogicalStartRecord of txNo: int64 * lsn: LogSeqNo option
    | LogicalAbortRecord of txNo: int64 * logicalStartLSN: LogSeqNo * lsn: LogSeqNo option
    | TableFileInsertEndRecord of txNo: int64 * tblName: string * blockNo: int64 * slotId: int32 * logicalStartLSN: LogSeqNo * lsn: LogSeqNo option
    | TableFileDeleteEndRecord of txNo: int64 * tblName: string * blockNo: int64 * slotId: int32 * logicalStartLSN: LogSeqNo * lsn: LogSeqNo option
    | IndexInsertEndRecord of txNo: int64 * indexName: string * searchKey: SearchKey * recordBlockNo: int64 * recordSlotId: int32 * logicalStartLSN: LogSeqNo * lsn: LogSeqNo option
    | IndexDeleteEndRecord of txNo: int64 * indexName: string * searchKey: SearchKey * recordBlockNo: int64 * recordSlotId: int32 * logicalStartLSN: LogSeqNo * lsn: LogSeqNo option
    | IndexPageInsertRecord of txNo: int64 * indexBlkId: BlockId * isDirPage: bool * keyType: SearchKeyType * slotId: int32 * lsn: LogSeqNo option
    | IndexPageInsertClr of compTxNo: int64 * indexBlkId: BlockId * isDirPage: bool * keyType: SearchKeyType * slotId: int32 * undoNextLSN: LogSeqNo * lsn: LogSeqNo option
    | IndexPageDeleteRecord of txNo: int64 * indexBlkId: BlockId * isDirPage: bool * keyType: SearchKeyType * slotId: int32 * lsn: LogSeqNo option
    | IndexPageDeleteClr of compTxNo: int64 * indexBlkId: BlockId * isDirPage: bool * keyType: SearchKeyType * slotId: int32 * undoNextLSN: LogSeqNo * lsn: LogSeqNo option
    | SetValueRecord of txNo: int64 * blkId: BlockId * offset: int32 * dbType: DbType * value: DbConstant * newValue: DbConstant * lsn: LogSeqNo option
    | SetValueClr of compTxNo: int64 * blkId: BlockId * offset: int32 * dbType: DbType * value: DbConstant * newValue: DbConstant * undoNextLSN: LogSeqNo * lsn: LogSeqNo option

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

let newLogicalAbortRecord txNo logicalStartLSN =
    LogicalAbortRecord(txNo, logicalStartLSN, None)

let newLogicalAbortRecordByLogRecord record =
    let txNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let logicalStartLSN =
        newLogSeqNo
            (record.NextVal BigIntDbType |> DbConstant.toLong)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    LogicalAbortRecord(txNo, logicalStartLSN, Some record.LogSeqNo)

let newTableFileInsertEndRecord txNo tblName blockNo slotId logicalStartLSN =
    TableFileInsertEndRecord(txNo, tblName, blockNo, slotId, logicalStartLSN, None)

let newTableFileInsertEndRecordByLogRecord record =
    let txNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let tblName =
        record.NextVal(VarcharDbType 0)
        |> DbConstant.toString

    let blockNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let slotId =
        record.NextVal IntDbType |> DbConstant.toInt

    let logicalStartLSN =
        newLogSeqNo
            (record.NextVal BigIntDbType |> DbConstant.toLong)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    TableFileInsertEndRecord(txNo, tblName, blockNo, slotId, logicalStartLSN, Some record.LogSeqNo)

let newTableFileDeleteEndRecord txNo tblName blockNo slotId logicalStartLSN =
    TableFileDeleteEndRecord(txNo, tblName, blockNo, slotId, logicalStartLSN, None)

let newTableFileDeleteEndRecordByLogRecord record =
    let txNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let tblName =
        record.NextVal(VarcharDbType 0)
        |> DbConstant.toString

    let blockNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let slotId =
        record.NextVal IntDbType |> DbConstant.toInt

    let logicalStartLSN =
        newLogSeqNo
            (record.NextVal BigIntDbType |> DbConstant.toLong)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    TableFileDeleteEndRecord(txNo, tblName, blockNo, slotId, logicalStartLSN, Some record.LogSeqNo)

let newIndexInsertEndRecord txNo indexName searchKey recordBlockNo recordSlotId logicalStartLSN =
    IndexInsertEndRecord(txNo, indexName, searchKey, recordBlockNo, recordSlotId, logicalStartLSN, None)

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
            DbType.fromInt
                (record.NextVal IntDbType |> DbConstant.toInt)
                (record.NextVal IntDbType |> DbConstant.toInt)
            |> record.NextVal)
        |> SearchKey.newSearchKey

    let recordBlockNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let recordSlotId =
        record.NextVal IntDbType |> DbConstant.toInt

    let logicalStartLSN =
        newLogSeqNo
            (record.NextVal BigIntDbType |> DbConstant.toLong)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    IndexInsertEndRecord(txNo, indexName, searchKey, recordBlockNo, recordSlotId, logicalStartLSN, Some record.LogSeqNo)

let newIndexDeleteEndRecord txNo indexName searchKey recordBlockNo recordSlotId logicalStartLSN =
    IndexDeleteEndRecord(txNo, indexName, searchKey, recordBlockNo, recordSlotId, logicalStartLSN, None)

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
            DbType.fromInt
                (record.NextVal IntDbType |> DbConstant.toInt)
                (record.NextVal IntDbType |> DbConstant.toInt)
            |> record.NextVal)
        |> SearchKey.newSearchKey

    let recordBlockNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let recordSlotId =
        record.NextVal IntDbType |> DbConstant.toInt

    let logicalStartLSN =
        newLogSeqNo
            (record.NextVal BigIntDbType |> DbConstant.toLong)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    IndexDeleteEndRecord(txNo, indexName, searchKey, recordBlockNo, recordSlotId, logicalStartLSN, Some record.LogSeqNo)

let newIndexPageInsertRecord isDirPage txNo indexBlkId keyType slotId =
    IndexPageInsertRecord(txNo, indexBlkId, isDirPage, keyType, slotId, None)

let newIndexPageInsertRecordByLogRecord record =
    let txNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let isDirPage =
        (record.NextVal IntDbType |> DbConstant.toInt) = 1

    let count =
        record.NextVal IntDbType |> DbConstant.toInt

    let keyType =
        [ 1 .. count ]
        |> List.map (fun _ ->
            DbType.fromInt
                (record.NextVal IntDbType |> DbConstant.toInt)
                (record.NextVal IntDbType |> DbConstant.toInt))
        |> SearchKeyType.newSearchKeyTypeByTypes

    let indexBlkId =
        BlockId.newBlockId
            (record.NextVal(VarcharDbType 0)
             |> DbConstant.toString)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    let slotId =
        record.NextVal IntDbType |> DbConstant.toInt

    IndexPageInsertRecord(txNo, indexBlkId, isDirPage, keyType, slotId, Some record.LogSeqNo)

let newIndexPageInsertClr isDirPage compTxNo indexBlkId keyType slotId undoNextLSN =
    IndexPageInsertClr(compTxNo, indexBlkId, isDirPage, keyType, slotId, undoNextLSN, None)

let newIndexPageInsertClrByLogRecord record =
    let compTxNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let isDirPage =
        (record.NextVal IntDbType |> DbConstant.toInt) = 1

    let count =
        record.NextVal IntDbType |> DbConstant.toInt

    let keyType =
        [ 1 .. count ]
        |> List.map (fun _ ->
            DbType.fromInt
                (record.NextVal IntDbType |> DbConstant.toInt)
                (record.NextVal IntDbType |> DbConstant.toInt))
        |> SearchKeyType.newSearchKeyTypeByTypes

    let indexBlkId =
        BlockId.newBlockId
            (record.NextVal(VarcharDbType 0)
             |> DbConstant.toString)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    let slotId =
        record.NextVal IntDbType |> DbConstant.toInt

    let undoNextLSN =
        newLogSeqNo
            (record.NextVal BigIntDbType |> DbConstant.toLong)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    IndexPageInsertClr(compTxNo, indexBlkId, isDirPage, keyType, slotId, undoNextLSN, Some record.LogSeqNo)

let newIndexPageDeleteRecord isDirPage txNo indexBlkId keyType slotId =
    IndexPageDeleteRecord(txNo, indexBlkId, isDirPage, keyType, slotId, None)

let newIndexPageDeleteRecordByLogRecord record =
    let txNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let isDirPage =
        (record.NextVal IntDbType |> DbConstant.toInt) = 1

    let count =
        record.NextVal IntDbType |> DbConstant.toInt

    let keyType =
        [ 1 .. count ]
        |> List.map (fun _ ->
            DbType.fromInt
                (record.NextVal IntDbType |> DbConstant.toInt)
                (record.NextVal IntDbType |> DbConstant.toInt))
        |> SearchKeyType.newSearchKeyTypeByTypes

    let indexBlkId =
        BlockId.newBlockId
            (record.NextVal(VarcharDbType 0)
             |> DbConstant.toString)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    let slotId =
        record.NextVal IntDbType |> DbConstant.toInt

    IndexPageDeleteRecord(txNo, indexBlkId, isDirPage, keyType, slotId, Some record.LogSeqNo)

let newIndexPageDeleteClr isDirPage compTxNo indexBlkId keyType slotId undoNextLSN =
    IndexPageDeleteClr(compTxNo, indexBlkId, isDirPage, keyType, slotId, undoNextLSN, None)

let newIndexPageDeleteClrByLogRecord record =
    let compTxNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let isDirPage =
        (record.NextVal IntDbType |> DbConstant.toInt) = 1

    let count =
        record.NextVal IntDbType |> DbConstant.toInt

    let keyType =
        [ 1 .. count ]
        |> List.map (fun _ ->
            DbType.fromInt
                (record.NextVal IntDbType |> DbConstant.toInt)
                (record.NextVal IntDbType |> DbConstant.toInt))
        |> SearchKeyType.newSearchKeyTypeByTypes

    let indexBlkId =
        BlockId.newBlockId
            (record.NextVal(VarcharDbType 0)
             |> DbConstant.toString)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    let slotId =
        record.NextVal IntDbType |> DbConstant.toInt

    let undoNextLSN =
        newLogSeqNo
            (record.NextVal BigIntDbType |> DbConstant.toLong)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    IndexPageDeleteClr(compTxNo, indexBlkId, isDirPage, keyType, slotId, undoNextLSN, Some record.LogSeqNo)

let newSetValueRecord txNo blkId offset value newValue =
    SetValueRecord(txNo, blkId, offset, DbConstant.dbType value, value, newValue, None)

let newSetValueRecordByLogRecord record =
    let txNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let blkId =
        BlockId.newBlockId
            (record.NextVal(VarcharDbType 0)
             |> DbConstant.toString)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    let offset =
        record.NextVal IntDbType |> DbConstant.toInt

    let dbType =
        DbType.fromInt
            (record.NextVal IntDbType |> DbConstant.toInt)
            (record.NextVal IntDbType |> DbConstant.toInt)

    let value = record.NextVal dbType
    let newValue = record.NextVal dbType
    SetValueRecord(txNo, blkId, offset, dbType, value, newValue, Some record.LogSeqNo)

let newSetValueClr compTxNo blkId offset value newValue undoNextLSN =
    SetValueClr(compTxNo, blkId, offset, DbConstant.dbType value, value, newValue, undoNextLSN, None)

let newSetValueClrByLogRecord record =
    let txNo =
        record.NextVal BigIntDbType |> DbConstant.toLong

    let blkId =
        BlockId.newBlockId
            (record.NextVal(VarcharDbType 0)
             |> DbConstant.toString)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    let offset =
        record.NextVal IntDbType |> DbConstant.toInt

    let dbType =
        DbType.fromInt
            (record.NextVal IntDbType |> DbConstant.toInt)
            (record.NextVal IntDbType |> DbConstant.toInt)

    let value = record.NextVal dbType
    let newValue = record.NextVal dbType

    let undoNextLSN =
        newLogSeqNo
            (record.NextVal BigIntDbType |> DbConstant.toLong)
            (record.NextVal BigIntDbType |> DbConstant.toLong)

    SetValueClr(txNo, blkId, offset, dbType, value, newValue, undoNextLSN, Some record.LogSeqNo)

let transactionNumber r =
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
    | IndexPageInsertClr(compTxNo = n) -> n
    | IndexPageDeleteRecord(txNo = n) -> n
    | IndexPageDeleteClr(compTxNo = n) -> n
    | SetValueRecord(txNo = n) -> n
    | SetValueClr(compTxNo = n) -> n

let getLSN r =
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
    | IndexPageInsertClr(lsn = n) -> n
    | IndexPageDeleteRecord(lsn = n) -> n
    | IndexPageDeleteClr(lsn = n) -> n
    | SetValueRecord(lsn = n) -> n
    | SetValueClr(lsn = n) -> n

let op r =
    match r with
    | StartRecord (_) -> RecoveryLogOp.Start
    | CommitRecord (_) -> RecoveryLogOp.Commit
    | RollbackRecord (_) -> RecoveryLogOp.Rollback
    | CheckpointRecord (_) -> RecoveryLogOp.Checkpoint
    | LogicalStartRecord (_) -> RecoveryLogOp.LogicalStart
    | LogicalAbortRecord (_) -> RecoveryLogOp.LogicalAbort
    | TableFileInsertEndRecord (_) -> RecoveryLogOp.TableFileInsertEnd
    | TableFileDeleteEndRecord (_) -> RecoveryLogOp.TableFileDeleteEnd
    | IndexInsertEndRecord (_) -> RecoveryLogOp.IndexFileInsertEnd
    | IndexDeleteEndRecord (_) -> RecoveryLogOp.IndexFileDeleteEnd
    | IndexPageInsertRecord (_) -> RecoveryLogOp.IndexPageInsert
    | IndexPageInsertClr (_) -> RecoveryLogOp.IndexPageInsertClr
    | IndexPageDeleteRecord (_) -> RecoveryLogOp.IndexPageDelete
    | IndexPageDeleteClr (_) -> RecoveryLogOp.IndexPageDeleteClr
    | SetValueRecord (_) -> RecoveryLogOp.SetValue
    | SetValueClr (_) -> RecoveryLogOp.SetValueClr

let buildRecord r =
    match r with
    | CheckpointRecord(txNos = nums) ->
        let list =
            nums |> List.map (fun n -> BigIntDbConstant(n))

        IntDbConstant(int (op r))
        :: IntDbConstant(List.length nums)
        :: list
    | LogicalAbortRecord (n, start, _) ->
        let (LogSeqNo (blockNo, offset)) = start
        [ IntDbConstant(int (op r))
          BigIntDbConstant(n)
          BigIntDbConstant(blockNo)
          BigIntDbConstant(offset) ]
    | TableFileInsertEndRecord (n, tn, bn, sid, start, _) ->
        let (LogSeqNo (blockNo, offset)) = start
        [ IntDbConstant(int (op r))
          BigIntDbConstant(n)
          DbConstant.newVarchar tn
          BigIntDbConstant(bn)
          IntDbConstant(sid)
          BigIntDbConstant(blockNo)
          BigIntDbConstant(offset) ]
    | TableFileDeleteEndRecord (n, tn, bn, sid, start, _) ->
        let (LogSeqNo (blockNo, offset)) = start
        [ IntDbConstant(int (op r))
          BigIntDbConstant(n)
          DbConstant.newVarchar tn
          BigIntDbConstant(bn)
          IntDbConstant(sid)
          BigIntDbConstant(blockNo)
          BigIntDbConstant(offset) ]
    | IndexInsertEndRecord (n, inm, sk, bn, sid, start, _) ->
        let (LogSeqNo (blockNo, offset)) = start
        let (SearchKey (key)) = sk
        IntDbConstant(int (op r))
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
        IntDbConstant(int (op r))
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
    | IndexPageInsertRecord (n, ibid, dir, kt, sid, _) ->
        let (BlockId (fileName, blockNo)) = ibid
        let (SearchKeyType (keyType)) = kt
        IntDbConstant(int (op r))
        :: BigIntDbConstant(n)
        :: IntDbConstant(if dir then 1 else 0)
        :: DbConstant.newVarchar fileName
        :: BigIntDbConstant(blockNo)
        :: IntDbConstant(sid)
        :: IntDbConstant(List.length keyType)
        :: (keyType
            |> List.collect (fun t ->
                [ IntDbConstant(DbType.toInt t)
                  IntDbConstant(DbType.argument t) ]))
    | IndexPageInsertClr (n, ibid, dir, kt, sid, undo, _) ->
        let (BlockId (fileName, blockNo)) = ibid
        let (LogSeqNo (undoBlockNo, undoOffset)) = undo
        let (SearchKeyType (keyType)) = kt
        IntDbConstant(int (op r))
        :: BigIntDbConstant(n)
        :: IntDbConstant(if dir then 1 else 0)
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
    | IndexPageDeleteRecord (n, ibid, dir, kt, sid, _) ->
        let (BlockId (fileName, blockNo)) = ibid
        let (SearchKeyType (keyType)) = kt
        IntDbConstant(int (op r))
        :: BigIntDbConstant(n)
        :: IntDbConstant(if dir then 1 else 0)
        :: DbConstant.newVarchar fileName
        :: BigIntDbConstant(blockNo)
        :: IntDbConstant(sid)
        :: IntDbConstant(List.length keyType)
        :: (keyType
            |> List.collect (fun t ->
                [ IntDbConstant(DbType.toInt t)
                  IntDbConstant(DbType.argument t) ]))
    | IndexPageDeleteClr (n, ibid, dir, kt, sid, undo, _) ->
        let (BlockId (fileName, blockNo)) = ibid
        let (LogSeqNo (undoBlockNo, undoOffset)) = undo
        let (SearchKeyType (keyType)) = kt
        IntDbConstant(int (op r))
        :: BigIntDbConstant(n)
        :: IntDbConstant(if dir then 1 else 0)
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
        [ IntDbConstant(int (op r))
          BigIntDbConstant(n)
          DbConstant.newVarchar fileName
          BigIntDbConstant(blockNo)
          IntDbConstant(off)
          IntDbConstant(DbType.toInt t)
          IntDbConstant(DbType.argument t)
          v
          nv ]
    | SetValueClr (n, bid, off, t, v, nv, undo, _) ->
        let (BlockId (fileName, blockNo)) = bid
        let (LogSeqNo (undoBlockNo, undoOffset)) = undo
        [ IntDbConstant(int (op r))
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
        [ IntDbConstant(int (op r))
          BigIntDbConstant(transactionNumber r) ]

let writeToLog logMgr r = buildRecord r |> logMgr.Append

let fromLogRecord record =
    let op =
        record.NextVal IntDbType
        |> DbConstant.toInt
        |> enum<RecoveryLogOp>

    match op with
    | RecoveryLogOp.Start -> newStartRecordByLogRecord record
    | RecoveryLogOp.Commit -> newCommitRecordByLogRecord record
    | RecoveryLogOp.Rollback -> newRollbackRecordByLogRecord record
    | RecoveryLogOp.Checkpoint -> newCheckpointRecordByLogRecord record
    | RecoveryLogOp.LogicalStart -> newLogicalStartRecordByLogRecord record
    | RecoveryLogOp.LogicalAbort -> newLogicalAbortRecordByLogRecord record
    | RecoveryLogOp.TableFileInsertEnd -> newTableFileInsertEndRecordByLogRecord record
    | RecoveryLogOp.TableFileDeleteEnd -> newTableFileDeleteEndRecordByLogRecord record
    | RecoveryLogOp.IndexFileInsertEnd -> newIndexInsertEndRecordByLogRecord record
    | RecoveryLogOp.IndexFileDeleteEnd -> newIndexDeleteEndRecordByLogRecord record
    | RecoveryLogOp.IndexPageInsert -> newIndexPageInsertRecordByLogRecord record
    | RecoveryLogOp.IndexPageInsertClr -> newIndexPageInsertClrByLogRecord record
    | RecoveryLogOp.IndexPageDelete -> newIndexPageDeleteRecordByLogRecord record
    | RecoveryLogOp.IndexPageDeleteClr -> newIndexPageDeleteClrByLogRecord record
    | RecoveryLogOp.SetValue -> newSetValueRecordByLogRecord record
    | RecoveryLogOp.SetValueClr -> newSetValueClrByLogRecord record
    | _ -> failwith "Not supported"
