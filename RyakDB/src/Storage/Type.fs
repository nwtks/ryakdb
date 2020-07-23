namespace RyakDB.Storage.Type

open RyakDB.Sql.Type
open RyakDB.Storage.File
open RyakDB.Storage.Log

[<AutoOpen>]
module Buffer =
    type BufferFormatter = Buffer -> unit

    and Buffer =
        { BufferSize: int32
          GetVal: int32 -> SqlType -> SqlConstant
          SetVal: int32 -> SqlConstant -> LogSeqNo option -> unit
          LastLsn: unit -> LogSeqNo
          BlockId: unit -> BlockId
          SetValue: int32 -> SqlConstant -> unit
          Pin: unit -> unit
          Unpin: unit -> unit
          IsPinned: unit -> bool
          Flush: unit -> unit
          AssignToBlock: BlockId -> unit
          AssignToNew: string -> BufferFormatter -> unit }

    type BufferManager =
        { Pin: BlockId -> Buffer
          PinNew: string -> BufferFormatter -> Buffer
          Unpin: Buffer -> unit
          UnpinAll: unit -> unit
          FlushAll: unit -> unit
          FlushBuffers: unit -> unit
          Available: unit -> int32 }

[<AutoOpen>]
module Record =
    type RecordId = RecordId of id: int32 * blockId: BlockId

    type RecordFile =
        { GetVal: string -> SqlConstant option
          SetVal: string -> SqlConstant -> unit
          CurrentRecordId: unit -> RecordId option
          BeforeFirst: unit -> unit
          Next: unit -> bool
          MoveToRecordId: RecordId -> unit
          Insert: unit -> unit
          InsertByRecordId: RecordId -> unit
          Delete: unit -> unit
          DeleteByRecordId: RecordId -> unit
          FileSize: unit -> int64
          Close: unit -> unit
          Remove: unit -> unit }

[<AutoOpen>]
module Index =
    type SearchKey = SearchKey of constants: SqlConstant list

    type SearchKeyType = SearchKeyType of types: SqlType list

    type SearchRange =
        { Size: unit -> int32
          Get: int32 -> SqlConstantRange
          IsValid: unit -> bool
          GetMin: unit -> SearchKey
          GetMax: unit -> SearchKey
          MatchsKey: SearchKey -> bool
          BetweenMinAndMax: SearchKey -> bool
          IsSingleValue: unit -> bool
          ToSearchKey: unit -> SearchKey }

    type Index =
        { BeforeFirst: SearchRange -> unit
          Next: unit -> bool
          GetDataRecordId: unit -> RecordId
          Insert: bool -> SearchKey -> RecordId -> unit
          Delete: bool -> SearchKey -> RecordId -> unit
          Close: unit -> unit
          PreLoadToMemory: unit -> unit }

[<AutoOpen>]
module Concurrency =
    type ConcurrencyManager =
        { ModifyFile: string -> unit
          ReadFile: string -> unit
          InsertBlock: BlockId -> unit
          ModifyBlock: BlockId -> unit
          ReadBlock: BlockId -> unit
          ModifyRecord: RecordId -> unit
          ReadRecord: RecordId -> unit
          ModifyIndex: string -> unit
          ReadIndex: string -> unit
          ModifyLeafBlock: BlockId -> unit
          ReadLeafBlock: BlockId -> unit
          CrabDownDirBlockForModification: BlockId -> unit
          CrabDownDirBlockForRead: BlockId -> unit
          CrabBackDirBlockForModification: BlockId -> unit
          CrabBackDirBlockForRead: BlockId -> unit
          LockRecordFileHeader: BlockId -> unit
          ReleaseRecordFileHeader: BlockId -> unit }

[<AutoOpen>]
module Recovery =
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

[<AutoOpen>]
module Transaction =
    type IsolationLevel =
        | Serializable
        | RepeatableRead
        | ReadCommitted

    type Transaction =
        { RecoveryMgr: RecoveryManager
          ConcurMgr: ConcurrencyManager
          BufferMgr: BufferManager
          TransactionNumber: int64
          ReadOnly: bool
          Commit: unit -> unit
          Rollback: unit -> unit
          EndStatement: unit -> unit }

    type TransactionLifecycleListener =
        { OnTxCommit: Transaction -> unit
          OnTxRollback: Transaction -> unit
          OnTxEndStatement: Transaction -> unit }

    type TransactionManager =
        { NewTransaction: bool -> IsolationLevel -> Transaction
          GetNextTxNo: unit -> int64
          GetActiveTxCount: unit -> int32
          CreateCheckpoint: Transaction -> unit
          LifecycleListener: TransactionLifecycleListener }

[<AutoOpen>]
module Catalog =
    type TableInfo =
        { TableName: string
          Schema: Schema
          FileName: string
          OpenFile: Transaction -> bool -> RecordFile }

    type IndexInfo =
        { IndexName: string
          IndexType: IndexType
          TableName: string
          FieldNames: string list
          FileName: string
          OpenIndex : Transaction -> Index}
