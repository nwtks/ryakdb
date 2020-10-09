module RyakDB.Database

open RyakDB.Task
open RyakDB.Storage.File
open RyakDB.Storage.Log
open RyakDB.Buffer.BufferPool
open RyakDB.Concurrency.LockService
open RyakDB.Transaction
open RyakDB.TransactionService
open RyakDB.Catalog.CatalogService
open RyakDB.Recovery.RecoveryService
open RyakDB.Recovery.CheckpointTask
open RyakDB.Execution.Planner

type Database =
    { File: FileService
      BufferPool: BufferPool
      Log: LogService
      Task: TaskService
      Transaction: TransactionService
      Catalog: CatalogService
      Recovery: RecoveryService
      NewPlanner: unit -> Planner
      Shutdown: unit -> unit }
    interface System.IDisposable with
        member this.Dispose() = this.Shutdown()

type DatabaseConfig =
    { InMemory: bool
      BlockSize: int32
      BufferPoolSize: int32
      BufferPoolWaitTime: int32
      LockWaitTime: int32
      CheckpointPeriod: int32
      CheckpointTxCount: int32 }

module Database =
    let defaultConfig () =
        { InMemory = false
          BlockSize = 8192
          BufferPoolSize = 4096
          BufferPoolWaitTime = 10000
          LockWaitTime = 10000
          CheckpointPeriod = 300000
          CheckpointTxCount = 1000 }

    let createPlanner fileService bufferPool catalogService =
        let queryPlanner =
            newQueryPlanner fileService bufferPool catalogService

        let updatePlanner =
            newUpdatePlanner fileService catalogService

        newPlanner queryPlanner updatePlanner

    let shutdown fileService bufferPool transactionService taskService =
        taskService.CancelTasks()
        transactionService.RollbackAll()
        bufferPool.FlushAll()
        fileService.CloseAll()

let newDatabase dbPath config =
    let fileService =
        newFileService dbPath config.BlockSize config.InMemory

    let logService = newLogService fileService "database.log"

    let bufferPool =
        newBufferPool fileService logService config.BufferPoolSize config.BufferPoolWaitTime

    let catalogService = newCatalogService fileService

    let lockService = newLockService config.LockWaitTime

    let recoveryService =
        newRecoveryService fileService logService bufferPool catalogService

    let transactionService =
        newTransactionService logService bufferPool lockService recoveryService

    let initTx =
        transactionService.NewTransaction false Serializable

    if fileService.IsNew
    then catalogService.InitCatalogService initTx
    else recoveryService.RecoverSystem initTx

    transactionService.CreateCheckpoint()

    initTx.Commit()

    let taskService = newTaskService ()

    lockService.LockNotifier() |> taskService.RunTask

    if config.CheckpointTxCount > 0
    then newMonitorCheckpointTask transactionService config.CheckpointPeriod config.CheckpointTxCount
    else newPeriodicCheckpointTask transactionService config.CheckpointPeriod
    |> taskService.RunTask

    { File = fileService
      BufferPool = bufferPool
      Log = logService
      Task = taskService
      Transaction = transactionService
      Catalog = catalogService
      Recovery = recoveryService
      NewPlanner = fun () -> Database.createPlanner fileService bufferPool catalogService
      Shutdown = fun () -> Database.shutdown fileService bufferPool transactionService taskService }
