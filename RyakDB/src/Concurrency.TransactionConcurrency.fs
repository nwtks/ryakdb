module RyakDB.Concurrency.TransactionConcurrency

open RyakDB.Storage
open RyakDB.Table
open RyakDB.Concurrency.LockTable

type TransactionConcurrency =
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
      CrabDownBranchBlockForModification: BlockId -> unit
      CrabDownBranchBlockForRead: BlockId -> unit
      CrabBackBranchBlockForModification: BlockId -> unit
      CrabBackBranchBlockForRead: BlockId -> unit
      LockTableFileHeader: BlockId -> unit
      ReleaseTableFileHeader: BlockId -> unit
      OnTxCommit: unit -> unit
      OnTxRollback: unit -> unit
      OnTxEndStatement: unit -> unit }

let newReadCommitted txNo lockTable =
    let mutable toReleaseSLockAtEndStatement: LockerKey list = []

    { ModifyFile = fun fileName -> FileNameLockerKey fileName |> lockTable.XLock txNo
      ReadFile =
          fun fileName ->
              FileNameLockerKey fileName
              |> lockTable.ISLock txNo
              FileNameLockerKey fileName
              |> lockTable.ReleaseISLock txNo
      InsertBlock =
          fun blockId ->
              let (BlockId (fileName, _)) = blockId
              FileNameLockerKey fileName |> lockTable.XLock txNo
              BlockIdLockerKey blockId |> lockTable.XLock txNo
      ModifyBlock =
          fun blockId ->
              let (BlockId (fileName, _)) = blockId
              FileNameLockerKey fileName
              |> lockTable.IXLock txNo
              BlockIdLockerKey blockId |> lockTable.XLock txNo
      ReadBlock =
          fun blockId ->
              let (BlockId (fileName, _)) = blockId
              FileNameLockerKey fileName
              |> lockTable.ISLock txNo
              FileNameLockerKey fileName
              |> lockTable.ReleaseISLock txNo
              BlockIdLockerKey blockId |> lockTable.SLock txNo
              toReleaseSLockAtEndStatement <-
                  (BlockIdLockerKey blockId)
                  :: toReleaseSLockAtEndStatement
      ModifyRecord =
          fun recordId ->
              let (RecordId (_, blockId)) = recordId
              let (BlockId (fileName, _)) = blockId
              FileNameLockerKey fileName
              |> lockTable.IXLock txNo
              BlockIdLockerKey blockId |> lockTable.IXLock txNo
              RecordIdLockerKey recordId |> lockTable.XLock txNo
      ReadRecord =
          fun recordId ->
              let (RecordId (_, blockId)) = recordId
              let (BlockId (fileName, _)) = blockId
              FileNameLockerKey fileName
              |> lockTable.ISLock txNo
              FileNameLockerKey fileName
              |> lockTable.ReleaseISLock txNo
              BlockIdLockerKey blockId |> lockTable.ISLock txNo
              BlockIdLockerKey blockId
              |> lockTable.ReleaseISLock txNo
              RecordIdLockerKey recordId |> lockTable.SLock txNo
              toReleaseSLockAtEndStatement <-
                  (RecordIdLockerKey recordId)
                  :: toReleaseSLockAtEndStatement
      ModifyIndex = fun fileName -> FileNameLockerKey fileName |> lockTable.XLock txNo
      ReadIndex =
          fun fileName ->
              FileNameLockerKey fileName
              |> lockTable.ISLock txNo
              FileNameLockerKey fileName
              |> lockTable.ReleaseISLock txNo
      ModifyLeafBlock = fun blockId -> BlockIdLockerKey blockId |> lockTable.XLock txNo
      ReadLeafBlock =
          fun blockId ->
              toReleaseSLockAtEndStatement <-
                  (BlockIdLockerKey blockId)
                  :: toReleaseSLockAtEndStatement
      CrabDownBranchBlockForModification = fun blockId -> BlockIdLockerKey blockId |> lockTable.XLock txNo
      CrabDownBranchBlockForRead = fun blockId -> BlockIdLockerKey blockId |> lockTable.SLock txNo
      CrabBackBranchBlockForModification =
          fun blockId ->
              BlockIdLockerKey blockId
              |> lockTable.ReleaseXLock txNo
      CrabBackBranchBlockForRead =
          fun blockId ->
              BlockIdLockerKey blockId
              |> lockTable.ReleaseSLock txNo
      LockTableFileHeader = fun blockId -> BlockIdLockerKey blockId |> lockTable.XLock txNo
      ReleaseTableFileHeader =
          fun blockId ->
              BlockIdLockerKey blockId
              |> lockTable.ReleaseXLock txNo
      OnTxCommit = fun () -> lockTable.ReleaseAll txNo false
      OnTxRollback = fun () -> lockTable.ReleaseAll txNo false
      OnTxEndStatement =
          fun () ->
              List.rev toReleaseSLockAtEndStatement
              |> List.iter (lockTable.ReleaseSLock txNo)
              toReleaseSLockAtEndStatement <- [] }

let newRepeatableRead txNo lockTable =
    let mutable toReleaseSLockAtEndStatement: LockerKey list = []

    { ModifyFile = fun fileName -> FileNameLockerKey fileName |> lockTable.XLock txNo
      ReadFile =
          fun fileName ->
              FileNameLockerKey fileName
              |> lockTable.ISLock txNo
              FileNameLockerKey fileName
              |> lockTable.ReleaseISLock txNo
      InsertBlock =
          fun blockId ->
              let (BlockId (fileName, _)) = blockId
              FileNameLockerKey fileName |> lockTable.XLock txNo
              BlockIdLockerKey blockId |> lockTable.XLock txNo
      ModifyBlock =
          fun blockId ->
              let (BlockId (fileName, _)) = blockId
              FileNameLockerKey fileName
              |> lockTable.IXLock txNo
              BlockIdLockerKey blockId |> lockTable.XLock txNo
      ReadBlock =
          fun blockId ->
              let (BlockId (fileName, _)) = blockId
              FileNameLockerKey fileName
              |> lockTable.ISLock txNo
              FileNameLockerKey fileName
              |> lockTable.ReleaseISLock txNo
              BlockIdLockerKey blockId |> lockTable.SLock txNo
      ModifyRecord =
          fun recordId ->
              let (RecordId (_, blockId)) = recordId
              let (BlockId (fileName, _)) = blockId
              FileNameLockerKey fileName
              |> lockTable.IXLock txNo
              BlockIdLockerKey blockId |> lockTable.IXLock txNo
              RecordIdLockerKey recordId |> lockTable.XLock txNo
      ReadRecord =
          fun recordId ->
              let (RecordId (_, blockId)) = recordId
              let (BlockId (fileName, _)) = blockId
              FileNameLockerKey fileName
              |> lockTable.ISLock txNo
              FileNameLockerKey fileName
              |> lockTable.ReleaseISLock txNo
              BlockIdLockerKey blockId |> lockTable.ISLock txNo
              BlockIdLockerKey blockId
              |> lockTable.ReleaseISLock txNo
              RecordIdLockerKey recordId |> lockTable.SLock txNo
      ModifyIndex = fun fileName -> FileNameLockerKey fileName |> lockTable.XLock txNo
      ReadIndex =
          fun fileName ->
              FileNameLockerKey fileName
              |> lockTable.ISLock txNo
              FileNameLockerKey fileName
              |> lockTable.ReleaseISLock txNo
      ModifyLeafBlock = fun blockId -> BlockIdLockerKey blockId |> lockTable.XLock txNo
      ReadLeafBlock =
          fun blockId ->
              BlockIdLockerKey blockId |> lockTable.SLock txNo
              toReleaseSLockAtEndStatement <-
                  (BlockIdLockerKey blockId)
                  :: toReleaseSLockAtEndStatement
      CrabDownBranchBlockForModification = fun blockId -> BlockIdLockerKey blockId |> lockTable.XLock txNo
      CrabDownBranchBlockForRead = fun blockId -> BlockIdLockerKey blockId |> lockTable.SLock txNo
      CrabBackBranchBlockForModification =
          fun blockId ->
              BlockIdLockerKey blockId
              |> lockTable.ReleaseXLock txNo
      CrabBackBranchBlockForRead =
          fun blockId ->
              BlockIdLockerKey blockId
              |> lockTable.ReleaseSLock txNo
      LockTableFileHeader = fun blockId -> BlockIdLockerKey blockId |> lockTable.XLock txNo
      ReleaseTableFileHeader =
          fun blockId ->
              BlockIdLockerKey blockId
              |> lockTable.ReleaseXLock txNo
      OnTxCommit = fun () -> lockTable.ReleaseAll txNo false
      OnTxRollback = fun () -> lockTable.ReleaseAll txNo false
      OnTxEndStatement =
          fun () ->
              List.rev toReleaseSLockAtEndStatement
              |> List.iter (lockTable.ReleaseSLock txNo)
              toReleaseSLockAtEndStatement <- [] }

let newSerializable txNo lockTable =
    { ModifyFile = fun fileName -> FileNameLockerKey fileName |> lockTable.XLock txNo
      ReadFile =
          fun fileName ->
              FileNameLockerKey fileName
              |> lockTable.ISLock txNo
      InsertBlock =
          fun blockId ->
              let (BlockId (fileName, _)) = blockId
              FileNameLockerKey fileName |> lockTable.XLock txNo
              BlockIdLockerKey blockId |> lockTable.XLock txNo
      ModifyBlock =
          fun blockId ->
              let (BlockId (fileName, _)) = blockId
              FileNameLockerKey fileName
              |> lockTable.IXLock txNo
              BlockIdLockerKey blockId |> lockTable.XLock txNo
      ReadBlock =
          fun blockId ->
              let (BlockId (fileName, _)) = blockId
              FileNameLockerKey fileName
              |> lockTable.ISLock txNo
              BlockIdLockerKey blockId |> lockTable.SLock txNo
      ModifyRecord =
          fun recordId ->
              let (RecordId (_, blockId)) = recordId
              let (BlockId (fileName, _)) = blockId
              FileNameLockerKey fileName
              |> lockTable.IXLock txNo
              BlockIdLockerKey blockId |> lockTable.IXLock txNo
              RecordIdLockerKey recordId |> lockTable.XLock txNo
      ReadRecord =
          fun recordId ->
              let (RecordId (_, blockId)) = recordId
              let (BlockId (fileName, _)) = blockId
              FileNameLockerKey fileName
              |> lockTable.ISLock txNo
              BlockIdLockerKey blockId |> lockTable.ISLock txNo
              RecordIdLockerKey recordId |> lockTable.SLock txNo
      ModifyIndex = fun fileName -> FileNameLockerKey fileName |> lockTable.XLock txNo
      ReadIndex =
          fun fileName ->
              FileNameLockerKey fileName
              |> lockTable.ISLock txNo
      ModifyLeafBlock = fun blockId -> BlockIdLockerKey blockId |> lockTable.XLock txNo
      ReadLeafBlock = fun blockId -> BlockIdLockerKey blockId |> lockTable.SLock txNo
      CrabDownBranchBlockForModification = fun blockId -> BlockIdLockerKey blockId |> lockTable.XLock txNo
      CrabDownBranchBlockForRead = fun blockId -> BlockIdLockerKey blockId |> lockTable.SLock txNo
      CrabBackBranchBlockForModification =
          fun blockId ->
              BlockIdLockerKey blockId
              |> lockTable.ReleaseXLock txNo
      CrabBackBranchBlockForRead =
          fun blockId ->
              BlockIdLockerKey blockId
              |> lockTable.ReleaseSLock txNo
      LockTableFileHeader = fun blockId -> BlockIdLockerKey blockId |> lockTable.XLock txNo
      ReleaseTableFileHeader =
          fun blockId ->
              BlockIdLockerKey blockId
              |> lockTable.ReleaseXLock txNo
      OnTxCommit = fun () -> lockTable.ReleaseAll txNo false
      OnTxRollback = fun () -> lockTable.ReleaseAll txNo false
      OnTxEndStatement = fun () -> () }
