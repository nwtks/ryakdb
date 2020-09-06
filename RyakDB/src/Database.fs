namespace RyakDB.Database

open RyakDB.Storage.File
open RyakDB.Storage.Log
open RyakDB.Buffer.BufferPool
open RyakDB.Concurrency
open RyakDB.Concurrency.LockTable
open RyakDB.Transaction
open RyakDB.Transaction.TransactionManager
open RyakDB.Recovery
open RyakDB.Recovery.CheckpointTask
open RyakDB.Recovery.SystemRecovery
open RyakDB.Catalog
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

    let newPlanner (fileMgr: FileManager) (catalogMgr: CatalogManager) =
        let queryPlanner =
            QueryPlanner.newQueryPlanner fileMgr catalogMgr

        let updatePlanner =
            UpdatePlanner.newUpdatePlanner fileMgr catalogMgr

        Planner.newPlanner queryPlanner updatePlanner

    let createDatabase dbPath config =
        let fileMgr =
            FileManager.newFileManager dbPath config.BlockSize config.InMemory

        let logMgr =
            LogManager.newLogManager fileMgr "database.log"

        let bufferPool =
            BufferPool.newBufferPool fileMgr logMgr config.BufferPoolSize config.BufferPoolWaitTime

        let catalogMgr = CatalogManager.newCatalogManager fileMgr

        let lockTable =
            LockTable.newLockTable config.LockTableWaitTime

        let txMgr =
            TransactionManager.newTransactionManager fileMgr logMgr bufferPool lockTable catalogMgr

        let initTx = txMgr.NewTransaction false Serializable
        let isDbNew = fileMgr.IsNew
        if isDbNew then catalogMgr.InitCatalogManager initTx
        if not (isDbNew)
        then SystemRecovery.recover fileMgr logMgr catalogMgr initTx
        txMgr.CreateCheckpoint initTx
        initTx.Commit()

        let taskMgr = TaskManager.newTaskManager ()
        lockTable.LocktableNotifier() |> taskMgr.RunTask
        if config.CheckpointTxCount > 0
        then CheckpointTask.newMonitorCheckpointTask txMgr config.CheckpointPeriod config.CheckpointTxCount
        else CheckpointTask.newPeriodicCheckpointTask txMgr config.CheckpointPeriod
        |> taskMgr.RunTask

        { FileMgr = fileMgr
          LogMgr = logMgr
          TaskMgr = taskMgr
          TxMgr = txMgr
          CatalogMgr = catalogMgr
          NewPlanner = fun () -> newPlanner fileMgr catalogMgr }

    let newDatabase dbPath =
        defaultConfig () |> createDatabase dbPath
