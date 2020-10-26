module RyakDB.Concurrency.TransactionConcurrency

open RyakDB.Storage
open RyakDB.Table
open RyakDB.Concurrency.LockService

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

let newReadCommitted txNo lockService =
    let mutable toReleaseSLockAtEndStatement: LockerKey list = []

    { ModifyFile =
          fun fileName ->
              FileNameLockerKey fileName
              |> lockService.XLock txNo
      ReadFile =
          fun fileName ->
              FileNameLockerKey fileName
              |> lockService.ISLock txNo
              FileNameLockerKey fileName
              |> lockService.ReleaseISLock txNo
      InsertBlock =
          fun blockId ->
              BlockId.fileName blockId
              |> FileNameLockerKey
              |> lockService.XLock txNo
              BlockIdLockerKey blockId |> lockService.XLock txNo
      ModifyBlock =
          fun blockId ->
              BlockId.fileName blockId
              |> FileNameLockerKey
              |> lockService.IXLock txNo
              BlockIdLockerKey blockId |> lockService.XLock txNo
      ReadBlock =
          fun blockId ->
              BlockId.fileName blockId
              |> FileNameLockerKey
              |> lockService.ISLock txNo
              BlockId.fileName blockId
              |> FileNameLockerKey
              |> lockService.ReleaseISLock txNo
              BlockIdLockerKey blockId |> lockService.SLock txNo
              toReleaseSLockAtEndStatement <-
                  (BlockIdLockerKey blockId)
                  :: toReleaseSLockAtEndStatement
      ModifyRecord =
          fun recordId ->
              RecordId.fileName recordId
              |> FileNameLockerKey
              |> lockService.IXLock txNo
              RecordId.blockId recordId
              |> BlockIdLockerKey
              |> lockService.IXLock txNo
              RecordIdLockerKey recordId
              |> lockService.XLock txNo
      ReadRecord =
          fun recordId ->
              RecordId.fileName recordId
              |> FileNameLockerKey
              |> lockService.ISLock txNo
              RecordId.fileName recordId
              |> FileNameLockerKey
              |> lockService.ReleaseISLock txNo
              RecordId.blockId recordId
              |> BlockIdLockerKey
              |> lockService.ISLock txNo
              RecordId.blockId recordId
              |> BlockIdLockerKey
              |> lockService.ReleaseISLock txNo
              RecordIdLockerKey recordId
              |> lockService.SLock txNo
              toReleaseSLockAtEndStatement <-
                  (RecordIdLockerKey recordId)
                  :: toReleaseSLockAtEndStatement
      ModifyIndex =
          fun fileName ->
              FileNameLockerKey fileName
              |> lockService.XLock txNo
      ReadIndex =
          fun fileName ->
              FileNameLockerKey fileName
              |> lockService.ISLock txNo
              FileNameLockerKey fileName
              |> lockService.ReleaseISLock txNo
      ModifyLeafBlock = fun blockId -> BlockIdLockerKey blockId |> lockService.XLock txNo
      ReadLeafBlock =
          fun blockId ->
              toReleaseSLockAtEndStatement <-
                  (BlockIdLockerKey blockId)
                  :: toReleaseSLockAtEndStatement
      CrabDownBranchBlockForModification = fun blockId -> BlockIdLockerKey blockId |> lockService.XLock txNo
      CrabDownBranchBlockForRead = fun blockId -> BlockIdLockerKey blockId |> lockService.SLock txNo
      CrabBackBranchBlockForModification =
          fun blockId ->
              BlockIdLockerKey blockId
              |> lockService.ReleaseXLock txNo
      CrabBackBranchBlockForRead =
          fun blockId ->
              BlockIdLockerKey blockId
              |> lockService.ReleaseSLock txNo
      LockTableFileHeader = fun blockId -> BlockIdLockerKey blockId |> lockService.XLock txNo
      ReleaseTableFileHeader =
          fun blockId ->
              BlockIdLockerKey blockId
              |> lockService.ReleaseXLock txNo
      OnTxCommit = fun () -> lockService.ReleaseAll txNo false
      OnTxRollback = fun () -> lockService.ReleaseAll txNo false
      OnTxEndStatement =
          fun () ->
              toReleaseSLockAtEndStatement
              |> List.rev
              |> List.iter (lockService.ReleaseSLock txNo)
              toReleaseSLockAtEndStatement <- [] }

let newRepeatableRead txNo lockService =
    let mutable toReleaseSLockAtEndStatement: LockerKey list = []

    { ModifyFile =
          fun fileName ->
              FileNameLockerKey fileName
              |> lockService.XLock txNo
      ReadFile =
          fun fileName ->
              FileNameLockerKey fileName
              |> lockService.ISLock txNo
              FileNameLockerKey fileName
              |> lockService.ReleaseISLock txNo
      InsertBlock =
          fun blockId ->
              BlockId.fileName blockId
              |> FileNameLockerKey
              |> lockService.XLock txNo
              BlockIdLockerKey blockId |> lockService.XLock txNo
      ModifyBlock =
          fun blockId ->
              BlockId.fileName blockId
              |> FileNameLockerKey
              |> lockService.IXLock txNo
              BlockIdLockerKey blockId |> lockService.XLock txNo
      ReadBlock =
          fun blockId ->
              BlockId.fileName blockId
              |> FileNameLockerKey
              |> lockService.ISLock txNo
              BlockId.fileName blockId
              |> FileNameLockerKey
              |> lockService.ReleaseISLock txNo
              BlockIdLockerKey blockId |> lockService.SLock txNo
      ModifyRecord =
          fun recordId ->
              RecordId.fileName recordId
              |> FileNameLockerKey
              |> lockService.IXLock txNo
              RecordId.blockId recordId
              |> BlockIdLockerKey
              |> lockService.IXLock txNo
              RecordIdLockerKey recordId
              |> lockService.XLock txNo
      ReadRecord =
          fun recordId ->
              RecordId.fileName recordId
              |> FileNameLockerKey
              |> lockService.ISLock txNo
              RecordId.fileName recordId
              |> FileNameLockerKey
              |> lockService.ReleaseISLock txNo
              RecordId.blockId recordId
              |> BlockIdLockerKey
              |> lockService.ISLock txNo
              RecordId.blockId recordId
              |> BlockIdLockerKey
              |> lockService.ReleaseISLock txNo
              RecordIdLockerKey recordId
              |> lockService.SLock txNo
      ModifyIndex =
          fun fileName ->
              FileNameLockerKey fileName
              |> lockService.XLock txNo
      ReadIndex =
          fun fileName ->
              FileNameLockerKey fileName
              |> lockService.ISLock txNo
              FileNameLockerKey fileName
              |> lockService.ReleaseISLock txNo
      ModifyLeafBlock = fun blockId -> BlockIdLockerKey blockId |> lockService.XLock txNo
      ReadLeafBlock =
          fun blockId ->
              BlockIdLockerKey blockId |> lockService.SLock txNo
              toReleaseSLockAtEndStatement <-
                  (BlockIdLockerKey blockId)
                  :: toReleaseSLockAtEndStatement
      CrabDownBranchBlockForModification = fun blockId -> BlockIdLockerKey blockId |> lockService.XLock txNo
      CrabDownBranchBlockForRead = fun blockId -> BlockIdLockerKey blockId |> lockService.SLock txNo
      CrabBackBranchBlockForModification =
          fun blockId ->
              BlockIdLockerKey blockId
              |> lockService.ReleaseXLock txNo
      CrabBackBranchBlockForRead =
          fun blockId ->
              BlockIdLockerKey blockId
              |> lockService.ReleaseSLock txNo
      LockTableFileHeader = fun blockId -> BlockIdLockerKey blockId |> lockService.XLock txNo
      ReleaseTableFileHeader =
          fun blockId ->
              BlockIdLockerKey blockId
              |> lockService.ReleaseXLock txNo
      OnTxCommit = fun () -> lockService.ReleaseAll txNo false
      OnTxRollback = fun () -> lockService.ReleaseAll txNo false
      OnTxEndStatement =
          fun () ->
              toReleaseSLockAtEndStatement
              |> List.rev
              |> List.iter (lockService.ReleaseSLock txNo)
              toReleaseSLockAtEndStatement <- [] }

let newSerializable txNo lockService =
    { ModifyFile =
          fun fileName ->
              FileNameLockerKey fileName
              |> lockService.XLock txNo
      ReadFile =
          fun fileName ->
              FileNameLockerKey fileName
              |> lockService.ISLock txNo
      InsertBlock =
          fun blockId ->
              BlockId.fileName blockId
              |> FileNameLockerKey
              |> lockService.XLock txNo
              BlockIdLockerKey blockId |> lockService.XLock txNo
      ModifyBlock =
          fun blockId ->
              BlockId.fileName blockId
              |> FileNameLockerKey
              |> lockService.IXLock txNo
              BlockIdLockerKey blockId |> lockService.XLock txNo
      ReadBlock =
          fun blockId ->
              BlockId.fileName blockId
              |> FileNameLockerKey
              |> lockService.ISLock txNo
              BlockIdLockerKey blockId |> lockService.SLock txNo
      ModifyRecord =
          fun recordId ->
              RecordId.fileName recordId
              |> FileNameLockerKey
              |> lockService.IXLock txNo
              RecordId.blockId recordId
              |> BlockIdLockerKey
              |> lockService.IXLock txNo
              RecordIdLockerKey recordId
              |> lockService.XLock txNo
      ReadRecord =
          fun recordId ->
              RecordId.fileName recordId
              |> FileNameLockerKey
              |> lockService.ISLock txNo
              RecordId.blockId recordId
              |> BlockIdLockerKey
              |> lockService.ISLock txNo
              RecordIdLockerKey recordId
              |> lockService.SLock txNo
      ModifyIndex =
          fun fileName ->
              FileNameLockerKey fileName
              |> lockService.XLock txNo
      ReadIndex =
          fun fileName ->
              FileNameLockerKey fileName
              |> lockService.ISLock txNo
      ModifyLeafBlock = fun blockId -> BlockIdLockerKey blockId |> lockService.XLock txNo
      ReadLeafBlock = fun blockId -> BlockIdLockerKey blockId |> lockService.SLock txNo
      CrabDownBranchBlockForModification = fun blockId -> BlockIdLockerKey blockId |> lockService.XLock txNo
      CrabDownBranchBlockForRead = fun blockId -> BlockIdLockerKey blockId |> lockService.SLock txNo
      CrabBackBranchBlockForModification =
          fun blockId ->
              BlockIdLockerKey blockId
              |> lockService.ReleaseXLock txNo
      CrabBackBranchBlockForRead =
          fun blockId ->
              BlockIdLockerKey blockId
              |> lockService.ReleaseSLock txNo
      LockTableFileHeader = fun blockId -> BlockIdLockerKey blockId |> lockService.XLock txNo
      ReleaseTableFileHeader =
          fun blockId ->
              BlockIdLockerKey blockId
              |> lockService.ReleaseXLock txNo
      OnTxCommit = fun () -> lockService.ReleaseAll txNo false
      OnTxRollback = fun () -> lockService.ReleaseAll txNo false
      OnTxEndStatement = fun () -> () }
