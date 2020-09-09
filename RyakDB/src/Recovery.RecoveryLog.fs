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

type RecoveryLog =
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

module RecoveryLog =
    let newStartRecord txNo = StartRecord(txNo, None)

    let newStartRecordByLogRecord record =
        StartRecord(record.NextVal BigIntSqlType |> SqlConstant.toLong, Some record.Lsn)

    let newCommitRecord txNo = CommitRecord(txNo, None)

    let newCommitRecordByLogRecord record =
        CommitRecord(record.NextVal BigIntSqlType |> SqlConstant.toLong, Some record.Lsn)

    let newRollbackRecord txNo = RollbackRecord(txNo, None)

    let newRollbackRecordByLogRecord record =
        RollbackRecord(record.NextVal BigIntSqlType |> SqlConstant.toLong, Some record.Lsn)

    let newCheckpointRecord txNos = CheckpointRecord(txNos, None)

    let newCheckpointRecordByLogRecord record =
        let count =
            record.NextVal IntSqlType |> SqlConstant.toInt

        let txNos =
            [ 1 .. count ]
            |> List.map (fun _ -> record.NextVal BigIntSqlType |> SqlConstant.toLong)

        CheckpointRecord(txNos, Some record.Lsn)

    let newLogicalStartRecord txNo = LogicalStartRecord(txNo, None)

    let newLogicalStartRecordByLogRecord record =
        LogicalStartRecord(record.NextVal BigIntSqlType |> SqlConstant.toLong, Some record.Lsn)

    let newLogicalAbortRecord txNo logicalStartLSN =
        LogicalAbortRecord(txNo, logicalStartLSN, None)

    let newLogicalAbortRecordByLogRecord record =
        let txNo =
            record.NextVal BigIntSqlType |> SqlConstant.toLong

        let logicalStartLSN =
            LogSeqNo.newLogSeqNo
                (record.NextVal BigIntSqlType |> SqlConstant.toLong)
                (record.NextVal BigIntSqlType |> SqlConstant.toLong)

        LogicalAbortRecord(txNo, logicalStartLSN, Some record.Lsn)

    let newRecordFileInsertEndRecord txNo tblName blockNo slotId logicalStartLSN =
        RecordFileInsertEndRecord(txNo, tblName, blockNo, slotId, logicalStartLSN, None)

    let newRecordFileInsertEndRecordByLogRecord record =
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
            LogSeqNo.newLogSeqNo
                (record.NextVal BigIntSqlType |> SqlConstant.toLong)
                (record.NextVal BigIntSqlType |> SqlConstant.toLong)

        RecordFileInsertEndRecord(txNo, tblName, blockNo, slotId, logicalStartLSN, Some record.Lsn)

    let newRecordFileDeleteEndRecord txNo tblName blockNo slotId logicalStartLSN =
        RecordFileDeleteEndRecord(txNo, tblName, blockNo, slotId, logicalStartLSN, None)

    let newRecordFileDeleteEndRecordByLogRecord record =
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
            LogSeqNo.newLogSeqNo
                (record.NextVal BigIntSqlType |> SqlConstant.toLong)
                (record.NextVal BigIntSqlType |> SqlConstant.toLong)

        RecordFileDeleteEndRecord(txNo, tblName, blockNo, slotId, logicalStartLSN, Some record.Lsn)

    let newIndexInsertEndRecord txNo indexName searchKey recordBlockNo recordSlotId logicalStartLSN =
        IndexInsertEndRecord(txNo, indexName, searchKey, recordBlockNo, recordSlotId, logicalStartLSN, None)

    let newIndexInsertEndRecordByLogRecord record =
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
                SqlType.fromInt
                    (record.NextVal IntSqlType |> SqlConstant.toInt)
                    (record.NextVal IntSqlType |> SqlConstant.toInt)
                |> record.NextVal)
            |> SearchKey.newSearchKey

        let recordBlockNo =
            record.NextVal BigIntSqlType |> SqlConstant.toLong

        let recordSlotId =
            record.NextVal IntSqlType |> SqlConstant.toInt

        let logicalStartLSN =
            LogSeqNo.newLogSeqNo
                (record.NextVal BigIntSqlType |> SqlConstant.toLong)
                (record.NextVal BigIntSqlType |> SqlConstant.toLong)

        IndexInsertEndRecord(txNo, indexName, searchKey, recordBlockNo, recordSlotId, logicalStartLSN, Some record.Lsn)

    let newIndexDeleteEndRecord txNo indexName searchKey recordBlockNo recordSlotId logicalStartLSN =
        IndexDeleteEndRecord(txNo, indexName, searchKey, recordBlockNo, recordSlotId, logicalStartLSN, None)

    let newIndexDeleteEndRecordByLogRecord record =
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
                SqlType.fromInt
                    (record.NextVal IntSqlType |> SqlConstant.toInt)
                    (record.NextVal IntSqlType |> SqlConstant.toInt)
                |> record.NextVal)
            |> SearchKey.newSearchKey

        let recordBlockNo =
            record.NextVal BigIntSqlType |> SqlConstant.toLong

        let recordSlotId =
            record.NextVal IntSqlType |> SqlConstant.toInt

        let logicalStartLSN =
            LogSeqNo.newLogSeqNo
                (record.NextVal BigIntSqlType |> SqlConstant.toLong)
                (record.NextVal BigIntSqlType |> SqlConstant.toLong)

        IndexDeleteEndRecord(txNo, indexName, searchKey, recordBlockNo, recordSlotId, logicalStartLSN, Some record.Lsn)

    let newIndexPageInsertRecord isDirPage txNo indexBlkId keyType slotId =
        IndexPageInsertRecord(txNo, indexBlkId, isDirPage, keyType, slotId, None)

    let newIndexPageInsertRecordByLogRecord record =
        let txNo =
            record.NextVal BigIntSqlType |> SqlConstant.toLong

        let isDirPage =
            (record.NextVal IntSqlType |> SqlConstant.toInt) = 1

        let count =
            record.NextVal IntSqlType |> SqlConstant.toInt

        let keyType =
            [ 1 .. count ]
            |> List.map (fun _ ->
                SqlType.fromInt
                    (record.NextVal IntSqlType |> SqlConstant.toInt)
                    (record.NextVal IntSqlType |> SqlConstant.toInt))
            |> SearchKeyType.newSearchKeyTypeByTypes

        let indexBlkId =
            BlockId.newBlockId
                (record.NextVal(VarcharSqlType 0)
                 |> SqlConstant.toString)
                (record.NextVal BigIntSqlType |> SqlConstant.toLong)

        let slotId =
            record.NextVal IntSqlType |> SqlConstant.toInt

        IndexPageInsertRecord(txNo, indexBlkId, isDirPage, keyType, slotId, Some record.Lsn)

    let newIndexPageInsertClr isDirPage compTxNo indexBlkId keyType slotId undoNextLSN =
        IndexPageInsertClr(compTxNo, indexBlkId, isDirPage, keyType, slotId, undoNextLSN, None)

    let newIndexPageInsertClrByLogRecord record =
        let compTxNo =
            record.NextVal BigIntSqlType |> SqlConstant.toLong

        let isDirPage =
            (record.NextVal IntSqlType |> SqlConstant.toInt) = 1

        let count =
            record.NextVal IntSqlType |> SqlConstant.toInt

        let keyType =
            [ 1 .. count ]
            |> List.map (fun _ ->
                SqlType.fromInt
                    (record.NextVal IntSqlType |> SqlConstant.toInt)
                    (record.NextVal IntSqlType |> SqlConstant.toInt))
            |> SearchKeyType.newSearchKeyTypeByTypes

        let indexBlkId =
            BlockId.newBlockId
                (record.NextVal(VarcharSqlType 0)
                 |> SqlConstant.toString)
                (record.NextVal BigIntSqlType |> SqlConstant.toLong)

        let slotId =
            record.NextVal IntSqlType |> SqlConstant.toInt

        let undoNextLSN =
            LogSeqNo.newLogSeqNo
                (record.NextVal BigIntSqlType |> SqlConstant.toLong)
                (record.NextVal BigIntSqlType |> SqlConstant.toLong)

        IndexPageInsertClr(compTxNo, indexBlkId, isDirPage, keyType, slotId, undoNextLSN, Some record.Lsn)

    let newIndexPageDeleteRecord isDirPage txNo indexBlkId keyType slotId =
        IndexPageDeleteRecord(txNo, indexBlkId, isDirPage, keyType, slotId, None)

    let newIndexPageDeleteRecordByLogRecord record =
        let txNo =
            record.NextVal BigIntSqlType |> SqlConstant.toLong

        let isDirPage =
            (record.NextVal IntSqlType |> SqlConstant.toInt) = 1

        let count =
            record.NextVal IntSqlType |> SqlConstant.toInt

        let keyType =
            [ 1 .. count ]
            |> List.map (fun _ ->
                SqlType.fromInt
                    (record.NextVal IntSqlType |> SqlConstant.toInt)
                    (record.NextVal IntSqlType |> SqlConstant.toInt))
            |> SearchKeyType.newSearchKeyTypeByTypes

        let indexBlkId =
            BlockId.newBlockId
                (record.NextVal(VarcharSqlType 0)
                 |> SqlConstant.toString)
                (record.NextVal BigIntSqlType |> SqlConstant.toLong)

        let slotId =
            record.NextVal IntSqlType |> SqlConstant.toInt

        IndexPageDeleteRecord(txNo, indexBlkId, isDirPage, keyType, slotId, Some record.Lsn)

    let newIndexPageDeleteClr isDirPage compTxNo indexBlkId keyType slotId undoNextLSN =
        IndexPageDeleteClr(compTxNo, indexBlkId, isDirPage, keyType, slotId, undoNextLSN, None)

    let newIndexPageDeleteClrByLogRecord record =
        let compTxNo =
            record.NextVal BigIntSqlType |> SqlConstant.toLong

        let isDirPage =
            (record.NextVal IntSqlType |> SqlConstant.toInt) = 1

        let count =
            record.NextVal IntSqlType |> SqlConstant.toInt

        let keyType =
            [ 1 .. count ]
            |> List.map (fun _ ->
                SqlType.fromInt
                    (record.NextVal IntSqlType |> SqlConstant.toInt)
                    (record.NextVal IntSqlType |> SqlConstant.toInt))
            |> SearchKeyType.newSearchKeyTypeByTypes

        let indexBlkId =
            BlockId.newBlockId
                (record.NextVal(VarcharSqlType 0)
                 |> SqlConstant.toString)
                (record.NextVal BigIntSqlType |> SqlConstant.toLong)

        let slotId =
            record.NextVal IntSqlType |> SqlConstant.toInt

        let undoNextLSN =
            LogSeqNo.newLogSeqNo
                (record.NextVal BigIntSqlType |> SqlConstant.toLong)
                (record.NextVal BigIntSqlType |> SqlConstant.toLong)

        IndexPageDeleteClr(compTxNo, indexBlkId, isDirPage, keyType, slotId, undoNextLSN, Some record.Lsn)

    let newSetValueRecord txNo blkId offset value newValue =
        SetValueRecord(txNo, blkId, offset, SqlConstant.sqlType value, value, newValue, None)

    let newSetValueRecordByLogRecord record =
        let txNo =
            record.NextVal BigIntSqlType |> SqlConstant.toLong

        let blkId =
            BlockId.newBlockId
                (record.NextVal(VarcharSqlType 0)
                 |> SqlConstant.toString)
                (record.NextVal BigIntSqlType |> SqlConstant.toLong)

        let offset =
            record.NextVal IntSqlType |> SqlConstant.toInt

        let sqlType =
            SqlType.fromInt
                (record.NextVal IntSqlType |> SqlConstant.toInt)
                (record.NextVal IntSqlType |> SqlConstant.toInt)

        let value = record.NextVal sqlType
        let newValue = record.NextVal sqlType
        SetValueRecord(txNo, blkId, offset, sqlType, value, newValue, Some record.Lsn)

    let newSetValueClr compTxNo blkId offset value newValue undoNextLSN =
        SetValueClr(compTxNo, blkId, offset, SqlConstant.sqlType value, value, newValue, undoNextLSN, None)

    let newSetValueClrByLogRecord record =
        let txNo =
            record.NextVal BigIntSqlType |> SqlConstant.toLong

        let blkId =
            BlockId.newBlockId
                (record.NextVal(VarcharSqlType 0)
                 |> SqlConstant.toString)
                (record.NextVal BigIntSqlType |> SqlConstant.toLong)

        let offset =
            record.NextVal IntSqlType |> SqlConstant.toInt

        let sqlType =
            SqlType.fromInt
                (record.NextVal IntSqlType |> SqlConstant.toInt)
                (record.NextVal IntSqlType |> SqlConstant.toInt)

        let value = record.NextVal sqlType
        let newValue = record.NextVal sqlType

        let undoNextLSN =
            LogSeqNo.newLogSeqNo
                (record.NextVal BigIntSqlType |> SqlConstant.toLong)
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
        | StartRecord (_) -> RecoveryLogOp.Start
        | CommitRecord (_) -> RecoveryLogOp.Commit
        | RollbackRecord (_) -> RecoveryLogOp.Rollback
        | CheckpointRecord (_) -> RecoveryLogOp.Checkpoint
        | LogicalStartRecord (_) -> RecoveryLogOp.LogicalStart
        | LogicalAbortRecord (_) -> RecoveryLogOp.LogicalAbort
        | RecordFileInsertEndRecord (_) -> RecoveryLogOp.RecordFileInsertEnd
        | RecordFileDeleteEndRecord (_) -> RecoveryLogOp.RecordFileDeleteEnd
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

    let writeToLog logMgr r = buildRecord r |> logMgr.Append

    let fromLogRecord record =
        let op =
            record.NextVal IntSqlType
            |> SqlConstant.toInt
            |> enum<RecoveryLogOp>

        match op with
        | RecoveryLogOp.Start -> newStartRecordByLogRecord record
        | RecoveryLogOp.Commit -> newCommitRecordByLogRecord record
        | RecoveryLogOp.Rollback -> newRollbackRecordByLogRecord record
        | RecoveryLogOp.Checkpoint -> newCheckpointRecordByLogRecord record
        | RecoveryLogOp.LogicalStart -> newLogicalStartRecordByLogRecord record
        | RecoveryLogOp.LogicalAbort -> newLogicalAbortRecordByLogRecord record
        | RecoveryLogOp.RecordFileInsertEnd -> newRecordFileInsertEndRecordByLogRecord record
        | RecoveryLogOp.RecordFileDeleteEnd -> newRecordFileDeleteEndRecordByLogRecord record
        | RecoveryLogOp.IndexFileInsertEnd -> newIndexInsertEndRecordByLogRecord record
        | RecoveryLogOp.IndexFileDeleteEnd -> newIndexDeleteEndRecordByLogRecord record
        | RecoveryLogOp.IndexPageInsert -> newIndexPageInsertRecordByLogRecord record
        | RecoveryLogOp.IndexPageInsertClr -> newIndexPageInsertClrByLogRecord record
        | RecoveryLogOp.IndexPageDelete -> newIndexPageDeleteRecordByLogRecord record
        | RecoveryLogOp.IndexPageDeleteClr -> newIndexPageDeleteClrByLogRecord record
        | RecoveryLogOp.SetValue -> newSetValueRecordByLogRecord record
        | RecoveryLogOp.SetValueClr -> newSetValueClrByLogRecord record
        | _ -> failwith "Not supported"
