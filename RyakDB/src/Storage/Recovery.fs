namespace RyakDB.Storage.Recovery

open RyakDB.Sql.Type
open RyakDB.Storage.Type
open RyakDB.Storage.File
open RyakDB.Storage.Log
open RyakDB.Storage.Record
open RyakDB.Storage.Index
open RyakDB.Storage.Catalog

type LogRecordOp =
    | Start = -42
    | Commit = -43
    | Rollback = -44
    | Checkpoint = -41
    | LogicalStart = -61
    | LogicalAbort = -77
    | RecordFileInsertEnd = -71
    | RecordFileDeleteEnd = -72
    | IndexFileInsertEnd = -73
    | IndexFileDeleteEnd = -74
    | IndexPageInsert = -75
    | IndexPageInsertClr = -79
    | IndexPageDelete = -76
    | IndexPageDeleteClr = -80
    | SetValue = -62
    | SetValueClr = -78

type LogRecord =
    | StartRecord of txNo: int64 * lsn: LogSeqNo option
    | CommitRecord of txNo: int64 * lsn: LogSeqNo option
    | RollbackRecord of txNo: int64 * lsn: LogSeqNo option
    | CheckpointRecord of txNos: int64 list * lsn: LogSeqNo option
    | LogicalStartRecord of txNo: int64 * lsn: LogSeqNo option
    | LogicalAbortRecord of txNo: int64 * logicalStartLSN: LogSeqNo * lsn: LogSeqNo option
    | RecordFileInsertEndRecord of txNo: int64 * tblName: string * blockNo: int64 * slotId: int32 * logicalStartLSN: LogSeqNo * lsn: LogSeqNo option
    | RecordFileDeleteEndRecord of txNo: int64 * tblName: string * blockNo: int64 * slotId: int32 * logicalStartLSN: LogSeqNo * lsn: LogSeqNo option
    | IndexInsertEndRecord of txNo: int64 * indexName: string * searchKey: SearchKey * recordBlockNo: int64 * recordSlotId: int32 * logicalStartLSN: LogSeqNo * lsn: LogSeqNo option
    | IndexDeleteEndRecord of txNo: int64 * indexName: string * searchKey: SearchKey * recordBlockNo: int64 * recordSlotId: int32 * logicalStartLSN: LogSeqNo * lsn: LogSeqNo option
    | IndexPageInsertRecord of txNo: int64 * indexBlkId: BlockId * isDirPage: bool * keyType: SearchKeyType * slotId: int32 * lsn: LogSeqNo option
    | IndexPageInsertClr of compTxNo: int64 * indexBlkId: BlockId * isDirPage: bool * keyType: SearchKeyType * slotId: int32 * undoNextLSN: LogSeqNo * lsn: LogSeqNo option
    | IndexPageDeleteRecord of txNo: int64 * indexBlkId: BlockId * isDirPage: bool * keyType: SearchKeyType * slotId: int32 * lsn: LogSeqNo option
    | IndexPageDeleteClr of compTxNo: int64 * indexBlkId: BlockId * isDirPage: bool * keyType: SearchKeyType * slotId: int32 * undoNextLSN: LogSeqNo * lsn: LogSeqNo option
    | SetValueRecord of txNo: int64 * blkId: BlockId * offset: int32 * sqlType: SqlType * value: SqlConstant * newValue: SqlConstant * lsn: LogSeqNo option
    | SetValueClr of compTxNo: int64 * blkId: BlockId * offset: int32 * sqlType: SqlType * value: SqlConstant * newValue: SqlConstant * undoNextLSN: LogSeqNo * lsn: LogSeqNo option

module LogRecord =
    let newStartRecord txNo = StartRecord(txNo, None)

    let newStartRecordByBasicLogRecord (record: BasicLogRecord) =
        StartRecord(record.NextVal BigIntSqlType |> SqlConstant.toLong, Some record.Lsn)

    let newCommitRecord txNo = CommitRecord(txNo, None)

    let newCommitRecordByBasicLogRecord (record: BasicLogRecord) =
        CommitRecord(record.NextVal BigIntSqlType |> SqlConstant.toLong, Some record.Lsn)

    let newRollbackRecord txNo = RollbackRecord(txNo, None)

    let newRollbackRecordByBasicLogRecord (record: BasicLogRecord) =
        RollbackRecord(record.NextVal BigIntSqlType |> SqlConstant.toLong, Some record.Lsn)

    let newCheckpointRecord txNos = CheckpointRecord(txNos, None)

    let newCheckpointRecordByBasicLogRecord (record: BasicLogRecord) =
        let count =
            record.NextVal IntSqlType |> SqlConstant.toInt

        let txNos =
            [ 1 .. count ]
            |> List.map (fun _ -> record.NextVal BigIntSqlType |> SqlConstant.toLong)

        CheckpointRecord(txNos, Some record.Lsn)

    let newLogicalStartRecord txNo = LogicalStartRecord(txNo, None)

    let newLogicalStartRecordByBasicLogRecord (record: BasicLogRecord) =
        LogicalStartRecord(record.NextVal BigIntSqlType |> SqlConstant.toLong, Some record.Lsn)

    let newLogicalAbortRecord txNo logicalStartLSN =
        LogicalAbortRecord(txNo, logicalStartLSN, None)

    let newLogicalAbortRecordByBasicLogRecord (record: BasicLogRecord) =
        let txNo =
            record.NextVal BigIntSqlType |> SqlConstant.toLong

        let logicalStartLSN =
            LogSeqNo.newLogSeqNo (record.NextVal BigIntSqlType |> SqlConstant.toLong)
                (record.NextVal BigIntSqlType |> SqlConstant.toLong)

        LogicalAbortRecord(txNo, logicalStartLSN, Some record.Lsn)

    let newRecordFileInsertEndRecord txNo tblName blockNo slotId logicalStartLSN =
        RecordFileInsertEndRecord(txNo, tblName, blockNo, slotId, logicalStartLSN, None)

    let newRecordFileInsertEndRecordByBasicLogRecord (record: BasicLogRecord) =
        let txNo =
            record.NextVal BigIntSqlType |> SqlConstant.toLong

        let tblName =
            record.NextVal(VarcharSqlType 0)
            |> SqlConstant.toString

        let blockNo =
            record.NextVal BigIntSqlType |> SqlConstant.toLong

        let slotId =
            record.NextVal IntSqlType |> SqlConstant.toInt

        let logicalStartLSN =
            LogSeqNo.newLogSeqNo (record.NextVal BigIntSqlType |> SqlConstant.toLong)
                (record.NextVal BigIntSqlType |> SqlConstant.toLong)

        RecordFileInsertEndRecord(txNo, tblName, blockNo, slotId, logicalStartLSN, Some record.Lsn)

    let newRecordFileDeleteEndRecord txNo tblName blockNo slotId logicalStartLSN =
        RecordFileDeleteEndRecord(txNo, tblName, blockNo, slotId, logicalStartLSN, None)

    let newRecordFileDeleteEndRecordByBasicLogRecord (record: BasicLogRecord) =
        let txNo =
            record.NextVal BigIntSqlType |> SqlConstant.toLong

        let tblName =
            record.NextVal(VarcharSqlType 0)
            |> SqlConstant.toString

        let blockNo =
            record.NextVal BigIntSqlType |> SqlConstant.toLong

        let slotId =
            record.NextVal IntSqlType |> SqlConstant.toInt

        let logicalStartLSN =
            LogSeqNo.newLogSeqNo (record.NextVal BigIntSqlType |> SqlConstant.toLong)
                (record.NextVal BigIntSqlType |> SqlConstant.toLong)

        RecordFileDeleteEndRecord(txNo, tblName, blockNo, slotId, logicalStartLSN, Some record.Lsn)

    let newIndexInsertEndRecord txNo indexName searchKey recordBlockNo recordSlotId logicalStartLSN =
        IndexInsertEndRecord(txNo, indexName, searchKey, recordBlockNo, recordSlotId, logicalStartLSN, None)

    let newIndexInsertEndRecordByBasicLogRecord (record: BasicLogRecord) =
        let txNo =
            record.NextVal BigIntSqlType |> SqlConstant.toLong

        let indexName =
            record.NextVal(VarcharSqlType 0)
            |> SqlConstant.toString

        let count =
            record.NextVal IntSqlType |> SqlConstant.toInt

        let searchKey =
            [ 1 .. count ]
            |> List.map (fun _ ->
                SqlType.fromInt (record.NextVal IntSqlType |> SqlConstant.toInt)
                    (record.NextVal IntSqlType |> SqlConstant.toInt)
                |> record.NextVal)
            |> SearchKey.newSearchKey

        let recordBlockNo =
            record.NextVal BigIntSqlType |> SqlConstant.toLong

        let recordSlotId =
            record.NextVal IntSqlType |> SqlConstant.toInt

        let logicalStartLSN =
            LogSeqNo.newLogSeqNo (record.NextVal BigIntSqlType |> SqlConstant.toLong)
                (record.NextVal BigIntSqlType |> SqlConstant.toLong)

        IndexInsertEndRecord(txNo, indexName, searchKey, recordBlockNo, recordSlotId, logicalStartLSN, Some record.Lsn)

    let newIndexDeleteEndRecord txNo indexName searchKey recordBlockNo recordSlotId logicalStartLSN =
        IndexDeleteEndRecord(txNo, indexName, searchKey, recordBlockNo, recordSlotId, logicalStartLSN, None)

    let newIndexDeleteEndRecordByBasicLogRecord (record: BasicLogRecord) =
        let txNo =
            record.NextVal BigIntSqlType |> SqlConstant.toLong

        let indexName =
            record.NextVal(VarcharSqlType 0)
            |> SqlConstant.toString

        let count =
            record.NextVal IntSqlType |> SqlConstant.toInt

        let searchKey =
            [ 1 .. count ]
            |> List.map (fun _ ->
                SqlType.fromInt (record.NextVal IntSqlType |> SqlConstant.toInt)
                    (record.NextVal IntSqlType |> SqlConstant.toInt)
                |> record.NextVal)
            |> SearchKey.newSearchKey

        let recordBlockNo =
            record.NextVal BigIntSqlType |> SqlConstant.toLong

        let recordSlotId =
            record.NextVal IntSqlType |> SqlConstant.toInt

        let logicalStartLSN =
            LogSeqNo.newLogSeqNo (record.NextVal BigIntSqlType |> SqlConstant.toLong)
                (record.NextVal BigIntSqlType |> SqlConstant.toLong)

        IndexDeleteEndRecord(txNo, indexName, searchKey, recordBlockNo, recordSlotId, logicalStartLSN, Some record.Lsn)

    let newIndexPageInsertRecord isDirPage txNo indexBlkId keyType slotId =
        IndexPageInsertRecord(txNo, indexBlkId, isDirPage, keyType, slotId, None)

    let newIndexPageInsertRecordByBasicLogRecord (record: BasicLogRecord) =
        let txNo =
            record.NextVal BigIntSqlType |> SqlConstant.toLong

        let isDirPage =
            (record.NextVal IntSqlType |> SqlConstant.toInt) = 1

        let count =
            record.NextVal IntSqlType |> SqlConstant.toInt

        let keyType =
            [ 1 .. count ]
            |> List.map (fun _ ->
                SqlType.fromInt (record.NextVal IntSqlType |> SqlConstant.toInt)
                    (record.NextVal IntSqlType |> SqlConstant.toInt))
            |> SearchKeyType.newSearchKeyTypeByTypes

        let indexBlkId =
            BlockId.newBlockId
                (record.NextVal(VarcharSqlType 0)
                 |> SqlConstant.toString) (record.NextVal BigIntSqlType |> SqlConstant.toLong)

        let slotId =
            record.NextVal IntSqlType |> SqlConstant.toInt

        IndexPageInsertRecord(txNo, indexBlkId, isDirPage, keyType, slotId, Some record.Lsn)

    let newIndexPageInsertClr isDirPage compTxNo indexBlkId keyType slotId undoNextLSN =
        IndexPageInsertClr(compTxNo, indexBlkId, isDirPage, keyType, slotId, undoNextLSN, None)

    let newIndexPageInsertClrByBasicLogRecord (record: BasicLogRecord) =
        let compTxNo =
            record.NextVal BigIntSqlType |> SqlConstant.toLong

        let isDirPage =
            (record.NextVal IntSqlType |> SqlConstant.toInt) = 1

        let count =
            record.NextVal IntSqlType |> SqlConstant.toInt

        let keyType =
            [ 1 .. count ]
            |> List.map (fun _ ->
                SqlType.fromInt (record.NextVal IntSqlType |> SqlConstant.toInt)
                    (record.NextVal IntSqlType |> SqlConstant.toInt))
            |> SearchKeyType.newSearchKeyTypeByTypes

        let indexBlkId =
            BlockId.newBlockId
                (record.NextVal(VarcharSqlType 0)
                 |> SqlConstant.toString) (record.NextVal BigIntSqlType |> SqlConstant.toLong)

        let slotId =
            record.NextVal IntSqlType |> SqlConstant.toInt

        let undoNextLSN =
            LogSeqNo.newLogSeqNo (record.NextVal BigIntSqlType |> SqlConstant.toLong)
                (record.NextVal BigIntSqlType |> SqlConstant.toLong)

        IndexPageInsertClr(compTxNo, indexBlkId, isDirPage, keyType, slotId, undoNextLSN, Some record.Lsn)

    let newIndexPageDeleteRecord isDirPage txNo indexBlkId keyType slotId =
        IndexPageDeleteRecord(txNo, indexBlkId, isDirPage, keyType, slotId, None)

    let newIndexPageDeleteRecordByBasicLogRecord (record: BasicLogRecord) =
        let txNo =
            record.NextVal BigIntSqlType |> SqlConstant.toLong

        let isDirPage =
            (record.NextVal IntSqlType |> SqlConstant.toInt) = 1

        let count =
            record.NextVal IntSqlType |> SqlConstant.toInt

        let keyType =
            [ 1 .. count ]
            |> List.map (fun _ ->
                SqlType.fromInt (record.NextVal IntSqlType |> SqlConstant.toInt)
                    (record.NextVal IntSqlType |> SqlConstant.toInt))
            |> SearchKeyType.newSearchKeyTypeByTypes

        let indexBlkId =
            BlockId.newBlockId
                (record.NextVal(VarcharSqlType 0)
                 |> SqlConstant.toString) (record.NextVal BigIntSqlType |> SqlConstant.toLong)

        let slotId =
            record.NextVal IntSqlType |> SqlConstant.toInt

        IndexPageDeleteRecord(txNo, indexBlkId, isDirPage, keyType, slotId, Some record.Lsn)

    let newIndexPageDeleteClr isDirPage compTxNo indexBlkId keyType slotId undoNextLSN =
        IndexPageDeleteClr(compTxNo, indexBlkId, isDirPage, keyType, slotId, undoNextLSN, None)

    let newIndexPageDeleteClrByBasicLogRecord (record: BasicLogRecord) =
        let compTxNo =
            record.NextVal BigIntSqlType |> SqlConstant.toLong

        let isDirPage =
            (record.NextVal IntSqlType |> SqlConstant.toInt) = 1

        let count =
            record.NextVal IntSqlType |> SqlConstant.toInt

        let keyType =
            [ 1 .. count ]
            |> List.map (fun _ ->
                SqlType.fromInt (record.NextVal IntSqlType |> SqlConstant.toInt)
                    (record.NextVal IntSqlType |> SqlConstant.toInt))
            |> SearchKeyType.newSearchKeyTypeByTypes

        let indexBlkId =
            BlockId.newBlockId
                (record.NextVal(VarcharSqlType 0)
                 |> SqlConstant.toString) (record.NextVal BigIntSqlType |> SqlConstant.toLong)

        let slotId =
            record.NextVal IntSqlType |> SqlConstant.toInt

        let undoNextLSN =
            LogSeqNo.newLogSeqNo (record.NextVal BigIntSqlType |> SqlConstant.toLong)
                (record.NextVal BigIntSqlType |> SqlConstant.toLong)

        IndexPageDeleteClr(compTxNo, indexBlkId, isDirPage, keyType, slotId, undoNextLSN, Some record.Lsn)

    let newSetValueRecord txNo blkId offset value newValue =
        SetValueRecord(txNo, blkId, offset, SqlConstant.sqlType value, value, newValue, None)

    let newSetValueRecordByBasicLogRecord (record: BasicLogRecord) =
        let txNo =
            record.NextVal BigIntSqlType |> SqlConstant.toLong

        let blkId =
            BlockId.newBlockId
                (record.NextVal(VarcharSqlType 0)
                 |> SqlConstant.toString) (record.NextVal BigIntSqlType |> SqlConstant.toLong)

        let offset =
            record.NextVal IntSqlType |> SqlConstant.toInt

        let sqlType =
            SqlType.fromInt (record.NextVal IntSqlType |> SqlConstant.toInt)
                (record.NextVal IntSqlType |> SqlConstant.toInt)

        let value = record.NextVal sqlType
        let newValue = record.NextVal sqlType
        SetValueRecord(txNo, blkId, offset, sqlType, value, newValue, Some record.Lsn)

    let newSetValueClr compTxNo blkId offset value newValue undoNextLSN =
        SetValueClr(compTxNo, blkId, offset, SqlConstant.sqlType value, value, newValue, undoNextLSN, None)

    let newSetValueClrByBasicLogRecord (record: BasicLogRecord) =
        let txNo =
            record.NextVal BigIntSqlType |> SqlConstant.toLong

        let blkId =
            BlockId.newBlockId
                (record.NextVal(VarcharSqlType 0)
                 |> SqlConstant.toString) (record.NextVal BigIntSqlType |> SqlConstant.toLong)

        let offset =
            record.NextVal IntSqlType |> SqlConstant.toInt

        let sqlType =
            SqlType.fromInt (record.NextVal IntSqlType |> SqlConstant.toInt)
                (record.NextVal IntSqlType |> SqlConstant.toInt)

        let value = record.NextVal sqlType
        let newValue = record.NextVal sqlType

        let undoNextLSN =
            LogSeqNo.newLogSeqNo (record.NextVal BigIntSqlType |> SqlConstant.toLong)
                (record.NextVal BigIntSqlType |> SqlConstant.toLong)

        SetValueClr(txNo, blkId, offset, sqlType, value, newValue, undoNextLSN, Some record.Lsn)

    let transactionNumber r =
        match r with
        | StartRecord(txNo = n) -> n
        | CommitRecord(txNo = n) -> n
        | RollbackRecord(txNo = n) -> n
        | CheckpointRecord (_) -> -1L
        | LogicalStartRecord(txNo = n) -> n
        | LogicalAbortRecord(txNo = n) -> n
        | RecordFileInsertEndRecord(txNo = n) -> n
        | RecordFileDeleteEndRecord(txNo = n) -> n
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
        | RecordFileInsertEndRecord(lsn = n) -> n
        | RecordFileDeleteEndRecord(lsn = n) -> n
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
        | StartRecord (_) -> LogRecordOp.Start
        | CommitRecord (_) -> LogRecordOp.Commit
        | RollbackRecord (_) -> LogRecordOp.Rollback
        | CheckpointRecord (_) -> LogRecordOp.Checkpoint
        | LogicalStartRecord (_) -> LogRecordOp.LogicalStart
        | LogicalAbortRecord (_) -> LogRecordOp.LogicalAbort
        | RecordFileInsertEndRecord (_) -> LogRecordOp.RecordFileInsertEnd
        | RecordFileDeleteEndRecord (_) -> LogRecordOp.RecordFileDeleteEnd
        | IndexInsertEndRecord (_) -> LogRecordOp.IndexFileInsertEnd
        | IndexDeleteEndRecord (_) -> LogRecordOp.IndexFileDeleteEnd
        | IndexPageInsertRecord (_) -> LogRecordOp.IndexPageInsert
        | IndexPageInsertClr (_) -> LogRecordOp.IndexPageInsertClr
        | IndexPageDeleteRecord (_) -> LogRecordOp.IndexPageDelete
        | IndexPageDeleteClr (_) -> LogRecordOp.IndexPageDeleteClr
        | SetValueRecord (_) -> LogRecordOp.SetValue
        | SetValueClr (_) -> LogRecordOp.SetValueClr

    let buildRecord r =
        match r with
        | CheckpointRecord(txNos = nums) ->
            let list =
                nums |> List.map (fun n -> BigIntSqlConstant(n))

            IntSqlConstant(int (op r))
            :: IntSqlConstant(List.length nums)
            :: list
        | LogicalAbortRecord (n, start, _) ->
            let (LogSeqNo (blockNo, offset)) = start
            [ IntSqlConstant(int (op r))
              BigIntSqlConstant(n)
              BigIntSqlConstant(blockNo)
              BigIntSqlConstant(offset) ]
        | RecordFileInsertEndRecord (n, tn, bn, sid, start, _) ->
            let (LogSeqNo (blockNo, offset)) = start
            [ IntSqlConstant(int (op r))
              BigIntSqlConstant(n)
              SqlConstant.newVarchar tn
              BigIntSqlConstant(bn)
              IntSqlConstant(sid)
              BigIntSqlConstant(blockNo)
              BigIntSqlConstant(offset) ]
        | RecordFileDeleteEndRecord (n, tn, bn, sid, start, _) ->
            let (LogSeqNo (blockNo, offset)) = start
            [ IntSqlConstant(int (op r))
              BigIntSqlConstant(n)
              SqlConstant.newVarchar tn
              BigIntSqlConstant(bn)
              IntSqlConstant(sid)
              BigIntSqlConstant(blockNo)
              BigIntSqlConstant(offset) ]
        | IndexInsertEndRecord (n, inm, sk, bn, sid, start, _) ->
            let (LogSeqNo (blockNo, offset)) = start
            let (SearchKey (key)) = sk
            IntSqlConstant(int (op r))
            :: BigIntSqlConstant(n)
            :: SqlConstant.newVarchar inm
            :: BigIntSqlConstant(bn)
            :: IntSqlConstant(sid)
            :: BigIntSqlConstant(blockNo)
            :: BigIntSqlConstant(offset)
            :: IntSqlConstant(List.length key)
            :: key
            |> List.collect (fun v ->
                let t = SqlConstant.sqlType v
                [ IntSqlConstant(SqlType.toInt t)
                  IntSqlConstant(SqlType.argument t)
                  v ])
        | IndexDeleteEndRecord (n, inm, sk, bn, sid, start, _) ->
            let (LogSeqNo (blockNo, offset)) = start
            let (SearchKey (key)) = sk
            IntSqlConstant(int (op r))
            :: BigIntSqlConstant(n)
            :: SqlConstant.newVarchar inm
            :: BigIntSqlConstant(bn)
            :: IntSqlConstant(sid)
            :: BigIntSqlConstant(blockNo)
            :: BigIntSqlConstant(offset)
            :: IntSqlConstant(List.length key)
            :: key
            |> List.collect (fun v ->
                let t = SqlConstant.sqlType v
                [ IntSqlConstant(SqlType.toInt t)
                  IntSqlConstant(SqlType.argument t)
                  v ])
        | IndexPageInsertRecord (n, ibid, dir, kt, sid, _) ->
            let (BlockId (fileName, blockNo)) = ibid
            let (SearchKeyType (keyType)) = kt
            IntSqlConstant(int (op r))
            :: BigIntSqlConstant(n)
            :: IntSqlConstant(if dir then 1 else 0)
            :: SqlConstant.newVarchar fileName
            :: BigIntSqlConstant(blockNo)
            :: IntSqlConstant(sid)
            :: IntSqlConstant(List.length keyType)
            :: (keyType
                |> List.collect (fun t ->
                    [ IntSqlConstant(SqlType.toInt t)
                      IntSqlConstant(SqlType.argument t) ]))
        | IndexPageInsertClr (n, ibid, dir, kt, sid, undo, _) ->
            let (BlockId (fileName, blockNo)) = ibid
            let (LogSeqNo (undoBlockNo, undoOffset)) = undo
            let (SearchKeyType (keyType)) = kt
            IntSqlConstant(int (op r))
            :: BigIntSqlConstant(n)
            :: IntSqlConstant(if dir then 1 else 0)
            :: SqlConstant.newVarchar fileName
            :: BigIntSqlConstant(blockNo)
            :: IntSqlConstant(sid)
            :: BigIntSqlConstant(undoBlockNo)
            :: BigIntSqlConstant(undoOffset)
            :: IntSqlConstant(List.length keyType)
            :: (keyType
                |> List.collect (fun t ->
                    [ IntSqlConstant(SqlType.toInt t)
                      IntSqlConstant(SqlType.argument t) ]))
        | IndexPageDeleteRecord (n, ibid, dir, kt, sid, _) ->
            let (BlockId (fileName, blockNo)) = ibid
            let (SearchKeyType (keyType)) = kt
            IntSqlConstant(int (op r))
            :: BigIntSqlConstant(n)
            :: IntSqlConstant(if dir then 1 else 0)
            :: SqlConstant.newVarchar fileName
            :: BigIntSqlConstant(blockNo)
            :: IntSqlConstant(sid)
            :: IntSqlConstant(List.length keyType)
            :: (keyType
                |> List.collect (fun t ->
                    [ IntSqlConstant(SqlType.toInt t)
                      IntSqlConstant(SqlType.argument t) ]))
        | IndexPageDeleteClr (n, ibid, dir, kt, sid, undo, _) ->
            let (BlockId (fileName, blockNo)) = ibid
            let (LogSeqNo (undoBlockNo, undoOffset)) = undo
            let (SearchKeyType (keyType)) = kt
            IntSqlConstant(int (op r))
            :: BigIntSqlConstant(n)
            :: IntSqlConstant(if dir then 1 else 0)
            :: SqlConstant.newVarchar fileName
            :: BigIntSqlConstant(blockNo)
            :: IntSqlConstant(sid)
            :: BigIntSqlConstant(undoBlockNo)
            :: BigIntSqlConstant(undoOffset)
            :: IntSqlConstant(List.length keyType)
            :: (keyType
                |> List.collect (fun t ->
                    [ IntSqlConstant(SqlType.toInt t)
                      IntSqlConstant(SqlType.argument t) ]))
        | SetValueRecord (n, bid, off, t, v, nv, _) ->
            let (BlockId (fileName, blockNo)) = bid
            [ IntSqlConstant(int (op r))
              BigIntSqlConstant(n)
              SqlConstant.newVarchar fileName
              BigIntSqlConstant(blockNo)
              IntSqlConstant(off)
              IntSqlConstant(SqlType.toInt t)
              IntSqlConstant(SqlType.argument t)
              v
              nv ]
        | SetValueClr (n, bid, off, t, v, nv, undo, _) ->
            let (BlockId (fileName, blockNo)) = bid
            let (LogSeqNo (undoBlockNo, undoOffset)) = undo
            [ IntSqlConstant(int (op r))
              BigIntSqlConstant(n)
              SqlConstant.newVarchar fileName
              BigIntSqlConstant(blockNo)
              IntSqlConstant(off)
              IntSqlConstant(SqlType.toInt t)
              IntSqlConstant(SqlType.argument t)
              v
              nv
              BigIntSqlConstant(undoBlockNo)
              BigIntSqlConstant(undoOffset) ]
        | _ ->
            [ IntSqlConstant(int (op r))
              BigIntSqlConstant(transactionNumber r) ]

    let writeToLog (logMgr: LogManager) r = buildRecord r |> logMgr.Append

    let fromBasicLogRecord (record: BasicLogRecord) =
        let op =
            record.NextVal IntSqlType
            |> SqlConstant.toInt
            |> enum<LogRecordOp>

        match op with
        | LogRecordOp.Start -> newStartRecordByBasicLogRecord record
        | LogRecordOp.Commit -> newCommitRecordByBasicLogRecord record
        | LogRecordOp.Rollback -> newRollbackRecordByBasicLogRecord record
        | LogRecordOp.Checkpoint -> newCheckpointRecordByBasicLogRecord record
        | LogRecordOp.LogicalStart -> newLogicalStartRecordByBasicLogRecord record
        | LogRecordOp.LogicalAbort -> newLogicalAbortRecordByBasicLogRecord record
        | LogRecordOp.RecordFileInsertEnd -> newRecordFileInsertEndRecordByBasicLogRecord record
        | LogRecordOp.RecordFileDeleteEnd -> newRecordFileDeleteEndRecordByBasicLogRecord record
        | LogRecordOp.IndexFileInsertEnd -> newIndexInsertEndRecordByBasicLogRecord record
        | LogRecordOp.IndexFileDeleteEnd -> newIndexDeleteEndRecordByBasicLogRecord record
        | LogRecordOp.IndexPageInsert -> newIndexPageInsertRecordByBasicLogRecord record
        | LogRecordOp.IndexPageInsertClr -> newIndexPageInsertClrByBasicLogRecord record
        | LogRecordOp.IndexPageDelete -> newIndexPageDeleteRecordByBasicLogRecord record
        | LogRecordOp.IndexPageDeleteClr -> newIndexPageDeleteClrByBasicLogRecord record
        | LogRecordOp.SetValue -> newSetValueRecordByBasicLogRecord record
        | LogRecordOp.SetValueClr -> newSetValueClrByBasicLogRecord record

    let undo (fileMgr: FileManager) (logMgr: LogManager) (catalogMgr: CatalogManager) (tx: Transaction) r =
        match r with
        | LogicalStartRecord (n, l) ->
            l
            |> Option.bind (tx.RecoveryMgr.LogLogicalAbort n)
            |> Option.iter logMgr.Flush
        | RecordFileInsertEndRecord (n, tn, bn, sid, start, _) ->
            let (Some ti) = catalogMgr.GetTableInfo tx tn

            let rf =
                RecordFile.newRecordFile fileMgr tx true ti

            let rid =
                RecordId.newBlockRecordId sid ti.FileName bn

            rf.DeleteByRecordId rid
            tx.RecoveryMgr.LogLogicalAbort n start
            |> Option.iter logMgr.Flush
        | RecordFileDeleteEndRecord (n, tn, bn, sid, start, _) ->
            let (Some ti) = catalogMgr.GetTableInfo tx tn

            let rf =
                RecordFile.newRecordFile fileMgr tx true ti

            let rid =
                RecordId.newBlockRecordId sid ti.FileName bn

            rf.InsertByRecordId rid
            tx.RecoveryMgr.LogLogicalAbort n start
            |> Option.iter logMgr.Flush
        | IndexInsertEndRecord (n, inm, sk, bn, sid, start, _) ->
            let (Some ii) = catalogMgr.GetIndexInfoByName tx inm
            let idx = ii.OpenIndex tx

            let rid =
                RecordId.newBlockRecordId sid ii.FileName bn

            idx.Delete false sk rid
            idx.Close()
            tx.RecoveryMgr.LogLogicalAbort n start
            |> Option.iter logMgr.Flush
        | IndexDeleteEndRecord (n, inm, sk, bn, sid, start, _) ->
            let (Some ii) = catalogMgr.GetIndexInfoByName tx inm
            let idx = ii.OpenIndex tx

            let rid =
                RecordId.newBlockRecordId sid ii.FileName bn

            idx.Insert false sk rid
            idx.Close()
            tx.RecoveryMgr.LogLogicalAbort n start
            |> Option.iter logMgr.Flush
        | IndexPageInsertRecord (n, ibid, dir, kt, sid, optlsn) ->
            let buffer = tx.BufferMgr.Pin ibid
            match optlsn with
            | Some lsn when lsn < buffer.LastLsn() ->
                if dir then BTreeDir.deleteASlot tx ibid kt sid else BTreeLeaf.deleteASlot tx ibid kt sid
                tx.RecoveryMgr.LogIndexPageDeletionClr dir n ibid kt sid lsn
                |> Option.iter logMgr.Flush
            | _ -> ()
            tx.BufferMgr.Unpin buffer
        | IndexPageDeleteRecord (n, ibid, dir, kt, sid, optlsn) ->
            let buffer = tx.BufferMgr.Pin ibid
            match optlsn with
            | Some lsn when lsn < buffer.LastLsn() ->
                if dir then BTreeDir.insertASlot tx ibid kt sid else BTreeLeaf.insertASlot tx ibid kt sid
                tx.RecoveryMgr.LogIndexPageInsertionClr dir n ibid kt sid lsn
                |> Option.iter logMgr.Flush
            | _ -> ()
            tx.BufferMgr.Unpin buffer
        | SetValueRecord (n, bid, off, _, v, _, optlsn) ->
            let buffer = tx.BufferMgr.Pin bid
            optlsn
            |> Option.bind (tx.RecoveryMgr.LogSetValClr n buffer off v)
            |> Option.iter logMgr.Flush
            buffer.SetVal off v None
            tx.BufferMgr.Unpin buffer
        | _ -> ()

    let redo (tx: Transaction) r =
        match r with
        | IndexPageInsertRecord (n, ibid, dir, kt, sid, optlsn) ->
            let buffer = tx.BufferMgr.Pin ibid
            match optlsn with
            | Some l when l > buffer.LastLsn() ->
                if dir then BTreeDir.insertASlot tx ibid kt sid else BTreeLeaf.insertASlot tx ibid kt sid
            | _ -> ()
            tx.BufferMgr.Unpin buffer
        | IndexPageInsertClr (n, ibid, dir, kt, sid, _, optlsn) ->
            let buffer = tx.BufferMgr.Pin ibid
            match optlsn with
            | Some l when l > buffer.LastLsn() ->
                if dir then BTreeDir.insertASlot tx ibid kt sid else BTreeLeaf.insertASlot tx ibid kt sid
            | _ -> ()
            tx.BufferMgr.Unpin buffer
        | IndexPageDeleteRecord (n, ibid, dir, kt, sid, optlsn) ->
            let buffer = tx.BufferMgr.Pin ibid
            match optlsn with
            | Some l when l > buffer.LastLsn() ->
                if dir then BTreeDir.deleteASlot tx ibid kt sid else BTreeLeaf.deleteASlot tx ibid kt sid
            | _ -> ()
            tx.BufferMgr.Unpin buffer
        | IndexPageDeleteClr (n, ibid, dir, kt, sid, _, optlsn) ->
            let buffer = tx.BufferMgr.Pin ibid
            match optlsn with
            | Some l when l > buffer.LastLsn() ->
                if dir then BTreeDir.deleteASlot tx ibid kt sid else BTreeLeaf.deleteASlot tx ibid kt sid
            | _ -> ()
            tx.BufferMgr.Unpin buffer
        | SetValueRecord (_, bid, off, _, _, nv, _) ->
            let buffer = tx.BufferMgr.Pin bid
            buffer.SetVal off nv None
            tx.BufferMgr.Unpin buffer
        | SetValueClr (_, bid, off, _, _, nv, _, _) ->
            let buffer = tx.BufferMgr.Pin bid
            buffer.SetVal off nv None
            tx.BufferMgr.Unpin buffer
        | _ -> ()

module RecoveryManager =
    type RecoveryManagerState =
        { TxNo: int64
          mutable LogicalStartLSN: LogSeqNo option }

    let rollback
        (fileMgr: FileManager)
        (logMgr: LogManager)
        (catalogMgr: CatalogManager)
        (state: RecoveryManagerState)
        (tx: Transaction)
        =
        let undoLogRecord =
            LogRecord.undo fileMgr logMgr catalogMgr tx

        let mutable txUnDoNextLSN = None
        let mutable inStart = false
        logMgr.Records()
        |> Seq.map LogRecord.fromBasicLogRecord
        |> Seq.filter (fun r -> LogRecord.transactionNumber r = state.TxNo)
        |> Seq.iter (fun r ->
            if not (inStart) then
                match txUnDoNextLSN, LogRecord.getLSN r with
                | None, _ ->
                    match r with
                    | StartRecord (_) -> inStart <- true
                    | LogicalAbortRecord(logicalStartLSN = lsn) ->
                        undoLogRecord r
                        txUnDoNextLSN <- Some lsn
                    | RecordFileInsertEndRecord(logicalStartLSN = lsn) ->
                        undoLogRecord r
                        txUnDoNextLSN <- Some lsn
                    | RecordFileDeleteEndRecord(logicalStartLSN = lsn) ->
                        undoLogRecord r
                        txUnDoNextLSN <- Some lsn
                    | IndexInsertEndRecord(logicalStartLSN = lsn) ->
                        undoLogRecord r
                        txUnDoNextLSN <- Some lsn
                    | IndexDeleteEndRecord(logicalStartLSN = lsn) ->
                        undoLogRecord r
                        txUnDoNextLSN <- Some lsn
                    | _ -> undoLogRecord r
                | Some (lsn1), Some (lsn2) when lsn1 > lsn2 ->
                    match r with
                    | StartRecord (_) -> inStart <- true
                    | LogicalAbortRecord(logicalStartLSN = lsn) ->
                        undoLogRecord r
                        txUnDoNextLSN <- Some lsn
                    | RecordFileInsertEndRecord(logicalStartLSN = lsn) ->
                        undoLogRecord r
                        txUnDoNextLSN <- Some lsn
                    | RecordFileDeleteEndRecord(logicalStartLSN = lsn) ->
                        undoLogRecord r
                        txUnDoNextLSN <- Some lsn
                    | IndexInsertEndRecord(logicalStartLSN = lsn) ->
                        undoLogRecord r
                        txUnDoNextLSN <- Some lsn
                    | IndexDeleteEndRecord(logicalStartLSN = lsn) ->
                        undoLogRecord r
                        txUnDoNextLSN <- Some lsn
                    | _ -> undoLogRecord r
                | _, _ -> ())

    let recoverSystem (fileMgr: FileManager) (logMgr: LogManager) (catalogMgr: CatalogManager) (tx: Transaction) =
        let undoLogRecord =
            LogRecord.undo fileMgr logMgr catalogMgr tx

        let mutable unCompletedTxs = Set.empty
        let mutable finishedTxs = Set.empty
        let mutable redoRecords = []
        let mutable inCheckpoint = false
        logMgr.Records()
        |> Seq.map LogRecord.fromBasicLogRecord
        |> Seq.iter (fun r ->
            if not (inCheckpoint) then
                match r with
                | CheckpointRecord(txNos = txNos) ->
                    txNos
                    |> List.filter (finishedTxs.Contains >> not)
                    |> List.iter (fun txNo -> unCompletedTxs <- unCompletedTxs.Add txNo)
                    inCheckpoint <- true
                | CommitRecord(txNo = txNo) ->
                    finishedTxs <- finishedTxs.Add txNo
                    redoRecords <- r :: redoRecords
                | RollbackRecord(txNo = txNo) ->
                    finishedTxs <- finishedTxs.Add txNo
                    redoRecords <- r :: redoRecords
                | StartRecord(txNo = txNo) when not (finishedTxs.Contains txNo) ->
                    unCompletedTxs <- unCompletedTxs.Add txNo
                    redoRecords <- r :: redoRecords
                | _ -> redoRecords <- r :: redoRecords)
        redoRecords |> List.iter (LogRecord.redo tx)
        unCompletedTxs <- unCompletedTxs.Remove tx.TransactionNumber
        let mutable txUnDoNextLSN = Map.empty
        logMgr.Records()
        |> Seq.map LogRecord.fromBasicLogRecord
        |> Seq.iter (fun r ->
            if not (unCompletedTxs.IsEmpty) then
                let txNo = LogRecord.transactionNumber r
                LogRecord.getLSN r
                |> Option.iter (fun lsn ->
                    if unCompletedTxs.Contains txNo
                       && txUnDoNextLSN.ContainsKey txNo
                       && txUnDoNextLSN.[txNo] > lsn then
                        match r with
                        | CommitRecord (_) -> ()
                        | RollbackRecord (_) -> ()
                        | StartRecord (_) -> unCompletedTxs <- unCompletedTxs.Remove txNo
                        | LogicalAbortRecord(logicalStartLSN = lsn) ->
                            undoLogRecord r
                            txUnDoNextLSN <- txUnDoNextLSN.Add(txNo, lsn)
                        | RecordFileInsertEndRecord(logicalStartLSN = lsn) ->
                            undoLogRecord r
                            txUnDoNextLSN <- txUnDoNextLSN.Add(txNo, lsn)
                        | RecordFileDeleteEndRecord(logicalStartLSN = lsn) ->
                            undoLogRecord r
                            txUnDoNextLSN <- txUnDoNextLSN.Add(txNo, lsn)
                        | IndexInsertEndRecord(logicalStartLSN = lsn) ->
                            undoLogRecord r
                            txUnDoNextLSN <- txUnDoNextLSN.Add(txNo, lsn)
                        | IndexDeleteEndRecord(logicalStartLSN = lsn) ->
                            undoLogRecord r
                            txUnDoNextLSN <- txUnDoNextLSN.Add(txNo, lsn)
                        | IndexPageInsertClr(undoNextLSN = lsn) -> txUnDoNextLSN <- txUnDoNextLSN.Add(txNo, lsn)
                        | IndexPageDeleteClr(undoNextLSN = lsn) -> txUnDoNextLSN <- txUnDoNextLSN.Add(txNo, lsn)
                        | SetValueClr(undoNextLSN = lsn) -> txUnDoNextLSN <- txUnDoNextLSN.Add(txNo, lsn)
                        | _ -> undoLogRecord r))

    let onTxCommit (logMgr: LogManager) (state: RecoveryManagerState) (tx: Transaction) =
        if not (tx.ReadOnly) then
            LogRecord.newCommitRecord state.TxNo
            |> LogRecord.writeToLog logMgr
            |> logMgr.Flush

    let onTxRollback
        (fileMgr: FileManager)
        (logMgr: LogManager)
        (catalogMgr: CatalogManager)
        (state: RecoveryManagerState)
        (tx: Transaction)
        =
        if not (tx.ReadOnly) then
            rollback fileMgr logMgr catalogMgr state tx
            LogRecord.newRollbackRecord state.TxNo
            |> LogRecord.writeToLog logMgr
            |> logMgr.Flush

    let checkpoint (logMgr: LogManager) txNos =
        LogRecord.newCheckpointRecord txNos
        |> LogRecord.writeToLog logMgr

    let logSetVal (logMgr: LogManager) (state: RecoveryManagerState) (buffer: Buffer) offset newVal =
        let blk = buffer.BlockId()
        let (BlockId (fileName, _)) = blk
        if FileManager.isTempFile fileName then
            None
        else
            let record =
                LogRecord.newSetValueRecord state.TxNo blk offset (buffer.GetVal offset (SqlConstant.sqlType newVal))
                    newVal

            let lsn = LogRecord.writeToLog logMgr record
            Some(lsn)

    let logLogicalStart (logMgr: LogManager) (state: RecoveryManagerState) =
        let record =
            LogRecord.newLogicalStartRecord state.TxNo

        let lsn = LogRecord.writeToLog logMgr record
        state.LogicalStartLSN <- Some(lsn)
        state.LogicalStartLSN

    let logLogicalAbort (logMgr: LogManager) txNo undoNextLSN =
        let record =
            LogRecord.newLogicalAbortRecord txNo undoNextLSN

        let lsn = LogRecord.writeToLog logMgr record
        Some(lsn)

    let logRecordFileInsertionEnd (logMgr: LogManager) (state: RecoveryManagerState) tableName blockNo slotId =
        match state.LogicalStartLSN with
        | Some (startLsn) ->
            let record =
                LogRecord.newRecordFileInsertEndRecord state.TxNo tableName blockNo slotId startLsn

            let lsn = LogRecord.writeToLog logMgr record
            state.LogicalStartLSN <- None
            Some(lsn)
        | _ -> failwith "Logical start LSN is null (in logRecordFileInsertionEnd)"

    let logRecordFileDeletionEnd (logMgr: LogManager) (state: RecoveryManagerState) tableName blockNo slotId =
        match state.LogicalStartLSN with
        | Some (startLsn) ->
            let record =
                LogRecord.newRecordFileDeleteEndRecord state.TxNo tableName blockNo slotId startLsn

            let lsn = LogRecord.writeToLog logMgr record
            state.LogicalStartLSN <- None
            Some(lsn)
        | _ -> failwith "Logical start LSN is null (in logRecordFileDeletionEnd)"

    let logIndexInsertionEnd
        (logMgr: LogManager)
        (state: RecoveryManagerState)
        indexName
        searchKey
        recordBlockNo
        recordSlotId
        =
        match state.LogicalStartLSN with
        | Some (startLsn) ->
            let record =
                LogRecord.newIndexInsertEndRecord state.TxNo indexName searchKey recordBlockNo recordSlotId startLsn

            let lsn = LogRecord.writeToLog logMgr record
            state.LogicalStartLSN <- None
            Some(lsn)
        | _ -> failwith "Logical start LSN is null (in logIndexInsertionEnd)"

    let logIndexDeletionEnd
        (logMgr: LogManager)
        (state: RecoveryManagerState)
        indexName
        searchKey
        recordBlockNo
        recordSlotId
        =
        match state.LogicalStartLSN with
        | Some (startLsn) ->
            let record =
                LogRecord.newIndexDeleteEndRecord state.TxNo indexName searchKey recordBlockNo recordSlotId startLsn

            let lsn = LogRecord.writeToLog logMgr record
            state.LogicalStartLSN <- None
            Some(lsn)
        | _ -> failwith "Logical start LSN is null (in logIndexDeletionEnd)"

    let logIndexPageInsertion (logMgr: LogManager) (state: RecoveryManagerState) isDirPage indexBlkId keyType slotId =
        let record =
            LogRecord.newIndexPageInsertRecord isDirPage state.TxNo indexBlkId keyType slotId

        let lsn = LogRecord.writeToLog logMgr record
        Some(lsn)

    let logIndexPageDeletion (logMgr: LogManager) (state: RecoveryManagerState) isDirPage indexBlkId keyType slotId =
        let record =
            LogRecord.newIndexPageDeleteRecord isDirPage state.TxNo indexBlkId keyType slotId

        let lsn = LogRecord.writeToLog logMgr record
        Some(lsn)

    let logIndexPageInsertionClr (logMgr: LogManager) isDirPage compTxNo indexBlkId keyType slotId undoNextLSN =
        let record =
            LogRecord.newIndexPageInsertClr isDirPage compTxNo indexBlkId keyType slotId undoNextLSN

        let lsn = LogRecord.writeToLog logMgr record
        Some(lsn)

    let logIndexPageDeletionClr (logMgr: LogManager) isDirPage compTxNo indexBlkId keyType slotId undoNextLSN =
        let record =
            LogRecord.newIndexPageDeleteClr isDirPage compTxNo indexBlkId keyType slotId undoNextLSN

        let lsn = LogRecord.writeToLog logMgr record
        Some(lsn)

    let logSetValClr (logMgr: LogManager) compTxNo (buffer: Buffer) offset newVal undoNextLSN =
        let blk = buffer.BlockId()
        let (BlockId (fileName, _)) = blk
        if FileManager.isTempFile fileName then
            None
        else
            let record =
                LogRecord.newSetValueClr compTxNo blk offset (buffer.GetVal offset (SqlConstant.sqlType newVal)) newVal
                    undoNextLSN

            let lsn = LogRecord.writeToLog logMgr record
            Some(lsn)

    let newRecoveryManager (fileMgr: FileManager) (logMgr: LogManager) (catalogMgr: CatalogManager) txNo isReadOnly =
        let state = { TxNo = txNo; LogicalStartLSN = None }

        if not (isReadOnly) then
            LogRecord.newStartRecord txNo
            |> LogRecord.writeToLog logMgr
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
          LogSetValClr = logSetValClr logMgr },
        { OnTxCommit = onTxCommit logMgr state
          OnTxRollback = onTxRollback fileMgr logMgr catalogMgr state
          OnTxEndStatement = fun _ -> () }

    let initializeSystem (fileMgr: FileManager) (logMgr: LogManager) (catalogMgr: CatalogManager) (tx: Transaction) =
        recoverSystem fileMgr logMgr catalogMgr tx
        tx.BufferMgr.FlushAll()
        logMgr.RemoveAndCreateNewLog()
        tx.TransactionNumber
        |> LogRecord.newStartRecord
        |> LogRecord.writeToLog logMgr
        |> ignore

module CheckpointTask =
    let createPeriodicCheckpoint (txMgr: TransactionManager) =
        let tx = txMgr.NewTransaction false Serializable
        txMgr.CreateCheckpoint tx
        tx.Commit()

    let createMonitorCheckpoint (txMgr: TransactionManager) txCount lastTxNo =
        let txNo = txMgr.GetNextTxNo()
        if txNo - lastTxNo > int64 txCount then
            let tx = txMgr.NewTransaction false Serializable
            txMgr.CreateCheckpoint tx
            tx.Commit()
            txNo
        else
            lastTxNo

    let newPeriodicCheckpointTask (txMgr: TransactionManager) (period: int32) =
        fun () ->
            while true do
                createPeriodicCheckpoint txMgr
                System.Threading.Thread.Sleep(period)

    let newMonitorCheckpointTask (txMgr: TransactionManager) (period: int32) txCount =
        let mutable lastTxNo = 0L
        fun () ->
            while true do
                lastTxNo <- createMonitorCheckpoint txMgr txCount lastTxNo
                System.Threading.Thread.Sleep(period)
