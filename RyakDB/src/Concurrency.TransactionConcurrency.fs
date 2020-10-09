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
              let (BlockId (fileName, _)) = blockId
              FileNameLockerKey fileName
              |> lockService.XLock txNo
              BlockIdLockerKey blockId |> lockService.XLock txNo
      ModifyBlock =
          fun blockId ->
              let (BlockId (fileName, _)) = blockId
              FileNameLockerKey fileName
              |> lockService.IXLock txNo
              BlockIdLockerKey blockId |> lockService.XLock txNo
      ReadBlock =
          fun blockId ->
              let (BlockId (fileName, _)) = blockId
              FileNameLockerKey fileName
              |> lockService.ISLock txNo
              FileNameLockerKey fileName
              |> lockService.ReleaseISLock txNo
              BlockIdLockerKey blockId |> lockService.SLock txNo
              toReleaseSLockAtEndStatement <-
                  (BlockIdLockerKey blockId)
                  :: toReleaseSLockAtEndStatement
      ModifyRecord =
          fun recordId ->
              let (RecordId (_, blockId)) = recordId
              let (BlockId (fileName, _)) = blockId
              FileNameLockerKey fileName
              |> lockService.IXLock txNo
              BlockIdLockerKey blockId
              |> lockService.IXLock txNo
              RecordIdLockerKey recordId
              |> lockService.XLock txNo
      ReadRecord =
          fun recordId ->
              let (RecordId (_, blockId)) = recordId
              let (BlockId (fileName, _)) = blockId
              FileNameLockerKey fileName
              |> lockService.ISLock txNo
              FileNameLockerKey fileName
              |> lockService.ReleaseISLock txNo
              BlockIdLockerKey blockId
              |> lockService.ISLock txNo
              BlockIdLockerKey blockId
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
              List.rev toReleaseSLockAtEndStatement
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
              let (BlockId (fileName, _)) = blockId
              FileNameLockerKey fileName
              |> lockService.XLock txNo
              BlockIdLockerKey blockId |> lockService.XLock txNo
      ModifyBlock =
          fun blockId ->
              let (BlockId (fileName, _)) = blockId
              FileNameLockerKey fileName
              |> lockService.IXLock txNo
              BlockIdLockerKey blockId |> lockService.XLock txNo
      ReadBlock =
          fun blockId ->
              let (BlockId (fileName, _)) = blockId
              FileNameLockerKey fileName
              |> lockService.ISLock txNo
              FileNameLockerKey fileName
              |> lockService.ReleaseISLock txNo
              BlockIdLockerKey blockId |> lockService.SLock txNo
      ModifyRecord =
          fun recordId ->
              let (RecordId (_, blockId)) = recordId
              let (BlockId (fileName, _)) = blockId
              FileNameLockerKey fileName
              |> lockService.IXLock txNo
              BlockIdLockerKey blockId
              |> lockService.IXLock txNo
              RecordIdLockerKey recordId
              |> lockService.XLock txNo
      ReadRecord =
          fun recordId ->
              let (RecordId (_, blockId)) = recordId
              let (BlockId (fileName, _)) = blockId
              FileNameLockerKey fileName
              |> lockService.ISLock txNo
              FileNameLockerKey fileName
              |> lockService.ReleaseISLock txNo
              BlockIdLockerKey blockId
              |> lockService.ISLock txNo
              BlockIdLockerKey blockId
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
              List.rev toReleaseSLockAtEndStatement
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
              let (BlockId (fileName, _)) = blockId
              FileNameLockerKey fileName
              |> lockService.XLock txNo
              BlockIdLockerKey blockId |> lockService.XLock txNo
      ModifyBlock =
          fun blockId ->
              let (BlockId (fileName, _)) = blockId
              FileNameLockerKey fileName
              |> lockService.IXLock txNo
              BlockIdLockerKey blockId |> lockService.XLock txNo
      ReadBlock =
          fun blockId ->
              let (BlockId (fileName, _)) = blockId
              FileNameLockerKey fileName
              |> lockService.ISLock txNo
              BlockIdLockerKey blockId |> lockService.SLock txNo
      ModifyRecord =
          fun recordId ->
              let (RecordId (_, blockId)) = recordId
              let (BlockId (fileName, _)) = blockId
              FileNameLockerKey fileName
              |> lockService.IXLock txNo
              BlockIdLockerKey blockId
              |> lockService.IXLock txNo
              RecordIdLockerKey recordId
              |> lockService.XLock txNo
      ReadRecord =
          fun recordId ->
              let (RecordId (_, blockId)) = recordId
              let (BlockId (fileName, _)) = blockId
              FileNameLockerKey fileName
              |> lockService.ISLock txNo
              BlockIdLockerKey blockId
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
