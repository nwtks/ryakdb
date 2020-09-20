module RyakDB.Database

open RyakDB.Storage.File
open RyakDB.Storage.Log
open RyakDB.Buffer.BufferPool
open RyakDB.Concurrency.LockTable
open RyakDB.Transaction
open RyakDB.TransactionManager
open RyakDB.Recovery
open RyakDB.Recovery.CheckpointTask
open RyakDB.Recovery.SystemRecovery
open RyakDB.Catalog.CatalogManager
open RyakDB.Execution.Planner
open RyakDB.Task

type Database =
    { FileMgr: FileManager
      LogMgr: LogManager
      TaskMgr: TaskManager
      TxMgr: TransactionManager
      CatalogMgr: CatalogManager
      NewPlanner: unit -> Planner }

type DatabaseConfig =
    { InMemory: bool
      BlockSize: int32
      BufferPoolSize: int32
      BufferPoolWaitTime: int32
      LockTableWaitTime: int32
      CheckpointPeriod: int32
      CheckpointTxCount: int32 }

module Database =
    let defaultConfig () =
        { InMemory = false
          BlockSize = 8192
          BufferPoolSize = 1024
          BufferPoolWaitTime = 10000
          LockTableWaitTime = 10000
          CheckpointPeriod = 300000
          CheckpointTxCount = 1000 }

    let createPlanner fileMgr catalogMgr =
        let queryPlanner = newQueryPlanner fileMgr catalogMgr
        let updatePlanner = newUpdatePlanner fileMgr catalogMgr
        newPlanner queryPlanner updatePlanner

let createDatabase dbPath config =
    let fileMgr =
        newFileManager dbPath config.BlockSize config.InMemory

    let logMgr = newLogManager fileMgr "database.log"

    let bufferPool =
        newBufferPool fileMgr logMgr config.BufferPoolSize config.BufferPoolWaitTime

    let catalogMgr = newCatalogManager fileMgr

    let lockTable = newLockTable config.LockTableWaitTime

    let txMgr =
        newTransactionManager fileMgr logMgr bufferPool lockTable catalogMgr

    let initTx = txMgr.NewTransaction false Serializable
    let isDbNew = fileMgr.IsNew
    if isDbNew then catalogMgr.InitCatalogManager initTx
    if not (isDbNew)
    then SystemRecovery.recover fileMgr logMgr catalogMgr initTx
    txMgr.CreateCheckpoint initTx
    initTx.Commit()

    let taskMgr = newTaskManager ()
    lockTable.LocktableNotifier() |> taskMgr.RunTask
    if config.CheckpointTxCount > 0
    then newMonitorCheckpointTask txMgr config.CheckpointPeriod config.CheckpointTxCount
    else newPeriodicCheckpointTask txMgr config.CheckpointPeriod
    |> taskMgr.RunTask

    { FileMgr = fileMgr
      LogMgr = logMgr
      TaskMgr = taskMgr
      TxMgr = txMgr
      CatalogMgr = catalogMgr
      NewPlanner = fun () -> Database.createPlanner fileMgr catalogMgr }

let newDatabase dbPath =
    Database.defaultConfig () |> createDatabase dbPath
