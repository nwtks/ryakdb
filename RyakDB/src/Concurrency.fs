namespace RyakDB.Concurrency

open RyakDB.Storage
open RyakDB.Table
open RyakDB.Concurrency.LockTable

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
      ReleaseRecordFileHeader: BlockId -> unit
      OnTxCommit: unit -> unit
      OnTxRollback: unit -> unit
      OnTxEndStatement: unit -> unit }

module ConcurrencyManager =
    let newReadCommitted txNo lockTable =
        let mutable toReleaseSLockAtEndStatement: LockerKey list = []

        { ModifyFile = fun fileName -> lockTable.XLock txNo (FileNameLockerKey fileName)
          ReadFile =
              fun fileName ->
                  lockTable.ISLock txNo (FileNameLockerKey fileName)
                  lockTable.ReleaseISLock txNo (FileNameLockerKey fileName)
          InsertBlock =
              fun blockId ->
                  let (BlockId (fileName, _)) = blockId
                  lockTable.XLock txNo (FileNameLockerKey fileName)
                  lockTable.XLock txNo (BlockIdLockerKey blockId)
          ModifyBlock =
              fun blockId ->
                  let (BlockId (fileName, _)) = blockId
                  lockTable.IXLock txNo (FileNameLockerKey fileName)
                  lockTable.XLock txNo (BlockIdLockerKey blockId)
          ReadBlock =
              fun blockId ->
                  let (BlockId (fileName, _)) = blockId
                  lockTable.ISLock txNo (FileNameLockerKey fileName)
                  lockTable.ReleaseISLock txNo (FileNameLockerKey fileName)
                  lockTable.SLock txNo (BlockIdLockerKey blockId)
                  toReleaseSLockAtEndStatement <-
                      (BlockIdLockerKey blockId)
                      :: toReleaseSLockAtEndStatement
          ModifyRecord =
              fun recordId ->
                  let (RecordId (_, blockId)) = recordId
                  let (BlockId (fileName, _)) = blockId
                  lockTable.IXLock txNo (FileNameLockerKey fileName)
                  lockTable.IXLock txNo (BlockIdLockerKey blockId)
                  lockTable.XLock txNo (RecordIdLockerKey recordId)
          ReadRecord =
              fun recordId ->
                  let (RecordId (_, blockId)) = recordId
                  let (BlockId (fileName, _)) = blockId
                  lockTable.ISLock txNo (FileNameLockerKey fileName)
                  lockTable.ReleaseISLock txNo (FileNameLockerKey fileName)
                  lockTable.ISLock txNo (BlockIdLockerKey blockId)
                  lockTable.ReleaseISLock txNo (BlockIdLockerKey blockId)
                  lockTable.SLock txNo (RecordIdLockerKey recordId)
                  toReleaseSLockAtEndStatement <-
                      (RecordIdLockerKey recordId)
                      :: toReleaseSLockAtEndStatement
          ModifyIndex = fun fileName -> lockTable.XLock txNo (FileNameLockerKey fileName)
          ReadIndex =
              fun fileName ->
                  lockTable.ISLock txNo (FileNameLockerKey fileName)
                  lockTable.ReleaseISLock txNo (FileNameLockerKey fileName)
          ModifyLeafBlock = fun blockId -> lockTable.XLock txNo (BlockIdLockerKey blockId)
          ReadLeafBlock =
              fun blockId ->
                  toReleaseSLockAtEndStatement <-
                      (BlockIdLockerKey blockId)
                      :: toReleaseSLockAtEndStatement
          CrabDownDirBlockForModification = fun blockId -> lockTable.XLock txNo (BlockIdLockerKey blockId)
          CrabDownDirBlockForRead = fun blockId -> lockTable.SLock txNo (BlockIdLockerKey blockId)
          CrabBackDirBlockForModification = fun blockId -> lockTable.ReleaseXLock txNo (BlockIdLockerKey blockId)
          CrabBackDirBlockForRead = fun blockId -> lockTable.ReleaseSLock txNo (BlockIdLockerKey blockId)
          LockRecordFileHeader = fun blockId -> lockTable.XLock txNo (BlockIdLockerKey blockId)
          ReleaseRecordFileHeader = fun blockId -> lockTable.ReleaseXLock txNo (BlockIdLockerKey blockId)
          OnTxCommit = fun () -> lockTable.ReleaseAll txNo false
          OnTxRollback = fun () -> lockTable.ReleaseAll txNo false
          OnTxEndStatement =
              fun () ->
                  List.rev toReleaseSLockAtEndStatement
                  |> List.iter (fun key -> lockTable.ReleaseSLock txNo key)
                  toReleaseSLockAtEndStatement <- [] }

    let newRepeatableRead txNo lockTable =
        let mutable toReleaseSLockAtEndStatement: LockerKey list = []

        { ModifyFile = fun fileName -> lockTable.XLock txNo (FileNameLockerKey fileName)
          ReadFile =
              fun fileName ->
                  lockTable.ISLock txNo (FileNameLockerKey fileName)
                  lockTable.ReleaseISLock txNo (FileNameLockerKey fileName)
          InsertBlock =
              fun blockId ->
                  let (BlockId (fileName, _)) = blockId
                  lockTable.XLock txNo (FileNameLockerKey fileName)
                  lockTable.XLock txNo (BlockIdLockerKey blockId)
          ModifyBlock =
              fun blockId ->
                  let (BlockId (fileName, _)) = blockId
                  lockTable.IXLock txNo (FileNameLockerKey fileName)
                  lockTable.XLock txNo (BlockIdLockerKey blockId)
          ReadBlock =
              fun blockId ->
                  let (BlockId (fileName, _)) = blockId
                  lockTable.ISLock txNo (FileNameLockerKey fileName)
                  lockTable.ReleaseISLock txNo (FileNameLockerKey fileName)
                  lockTable.SLock txNo (BlockIdLockerKey blockId)
          ModifyRecord =
              fun recordId ->
                  let (RecordId (_, blockId)) = recordId
                  let (BlockId (fileName, _)) = blockId
                  lockTable.IXLock txNo (FileNameLockerKey fileName)
                  lockTable.IXLock txNo (BlockIdLockerKey blockId)
                  lockTable.XLock txNo (RecordIdLockerKey recordId)
          ReadRecord =
              fun recordId ->
                  let (RecordId (_, blockId)) = recordId
                  let (BlockId (fileName, _)) = blockId
                  lockTable.ISLock txNo (FileNameLockerKey fileName)
                  lockTable.ReleaseISLock txNo (FileNameLockerKey fileName)
                  lockTable.ISLock txNo (BlockIdLockerKey blockId)
                  lockTable.ReleaseISLock txNo (BlockIdLockerKey blockId)
                  lockTable.SLock txNo (RecordIdLockerKey recordId)
          ModifyIndex = fun fileName -> lockTable.XLock txNo (FileNameLockerKey fileName)
          ReadIndex =
              fun fileName ->
                  lockTable.ISLock txNo (FileNameLockerKey fileName)
                  lockTable.ReleaseISLock txNo (FileNameLockerKey fileName)
          ModifyLeafBlock = fun blockId -> lockTable.XLock txNo (BlockIdLockerKey blockId)
          ReadLeafBlock =
              fun blockId ->
                  lockTable.SLock txNo (BlockIdLockerKey blockId)
                  toReleaseSLockAtEndStatement <-
                      (BlockIdLockerKey blockId)
                      :: toReleaseSLockAtEndStatement
          CrabDownDirBlockForModification = fun blockId -> lockTable.XLock txNo (BlockIdLockerKey blockId)
          CrabDownDirBlockForRead = fun blockId -> lockTable.SLock txNo (BlockIdLockerKey blockId)
          CrabBackDirBlockForModification = fun blockId -> lockTable.ReleaseXLock txNo (BlockIdLockerKey blockId)
          CrabBackDirBlockForRead = fun blockId -> lockTable.ReleaseSLock txNo (BlockIdLockerKey blockId)
          LockRecordFileHeader = fun blockId -> lockTable.XLock txNo (BlockIdLockerKey blockId)
          ReleaseRecordFileHeader = fun blockId -> lockTable.ReleaseXLock txNo (BlockIdLockerKey blockId)
          OnTxCommit = fun () -> lockTable.ReleaseAll txNo false
          OnTxRollback = fun () -> lockTable.ReleaseAll txNo false
          OnTxEndStatement =
              fun () ->
                  List.rev toReleaseSLockAtEndStatement
                  |> List.iter (fun key -> lockTable.ReleaseSLock txNo key)
                  toReleaseSLockAtEndStatement <- [] }

    let newSerializable txNo lockTable =
        { ModifyFile = fun fileName -> lockTable.XLock txNo (FileNameLockerKey fileName)
          ReadFile = fun fileName -> lockTable.ISLock txNo (FileNameLockerKey fileName)
          InsertBlock =
              fun blockId ->
                  let (BlockId (fileName, _)) = blockId
                  lockTable.XLock txNo (FileNameLockerKey fileName)
                  lockTable.XLock txNo (BlockIdLockerKey blockId)
          ModifyBlock =
              fun blockId ->
                  let (BlockId (fileName, _)) = blockId
                  lockTable.IXLock txNo (FileNameLockerKey fileName)
                  lockTable.XLock txNo (BlockIdLockerKey blockId)
          ReadBlock =
              fun blockId ->
                  let (BlockId (fileName, _)) = blockId
                  lockTable.ISLock txNo (FileNameLockerKey fileName)
                  lockTable.SLock txNo (BlockIdLockerKey blockId)
          ModifyRecord =
              fun recordId ->
                  let (RecordId (_, blockId)) = recordId
                  let (BlockId (fileName, _)) = blockId
                  lockTable.IXLock txNo (FileNameLockerKey fileName)
                  lockTable.IXLock txNo (BlockIdLockerKey blockId)
                  lockTable.XLock txNo (RecordIdLockerKey recordId)
          ReadRecord =
              fun recordId ->
                  let (RecordId (_, blockId)) = recordId
                  let (BlockId (fileName, _)) = blockId
                  lockTable.ISLock txNo (FileNameLockerKey fileName)
                  lockTable.ISLock txNo (BlockIdLockerKey blockId)
                  lockTable.SLock txNo (RecordIdLockerKey recordId)
          ModifyIndex = fun fileName -> lockTable.XLock txNo (FileNameLockerKey fileName)
          ReadIndex = fun fileName -> lockTable.ISLock txNo (FileNameLockerKey fileName)
          ModifyLeafBlock = fun blockId -> lockTable.XLock txNo (BlockIdLockerKey blockId)
          ReadLeafBlock = fun blockId -> lockTable.SLock txNo (BlockIdLockerKey blockId)
          CrabDownDirBlockForModification = fun blockId -> lockTable.XLock txNo (BlockIdLockerKey blockId)
          CrabDownDirBlockForRead = fun blockId -> lockTable.SLock txNo (BlockIdLockerKey blockId)
          CrabBackDirBlockForModification = fun blockId -> lockTable.ReleaseXLock txNo (BlockIdLockerKey blockId)
          CrabBackDirBlockForRead = fun blockId -> lockTable.ReleaseSLock txNo (BlockIdLockerKey blockId)
          LockRecordFileHeader = fun blockId -> lockTable.XLock txNo (BlockIdLockerKey blockId)
          ReleaseRecordFileHeader = fun blockId -> lockTable.ReleaseXLock txNo (BlockIdLockerKey blockId)
          OnTxCommit = fun () -> lockTable.ReleaseAll txNo false
          OnTxRollback = fun () -> lockTable.ReleaseAll txNo false
          OnTxEndStatement = fun () -> () }
