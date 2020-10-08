module RyakDB.TransactionManager

open RyakDB.Buffer.BufferPool
open RyakDB.Buffer.TransactionBuffer
open RyakDB.Concurrency.TransactionConcurrency
open RyakDB.Recovery.TransactionRecovery
open RyakDB.Transaction
open RyakDB.Recovery.SystemRecovery

type TransactionManager =
    { NewTransaction: bool -> IsolationLevel -> Transaction
      GetNextTxNo: unit -> int64
      GetActiveTxCount: unit -> int32
      CreateCheckpoint: unit -> unit
      RollbackAll: unit -> unit }

module TransactionManager =
    type TransactionManagerState =
        { ActiveTxs: Transaction list
          NextTxNo: int64 }

    let onTxCommit state tx =
        { state with
              ActiveTxs =
                  state.ActiveTxs
                  |> List.filter (fun atx -> atx.TransactionNo <> tx.TransactionNo) }

    let onTxRollback state tx =
        { state with
              ActiveTxs =
                  state.ActiveTxs
                  |> List.filter (fun atx -> atx.TransactionNo <> tx.TransactionNo) }

    let createTransaction logMgr bufferPool lockTable systemRecovery txCommitListener txRollbackListener state =
        fun readOnly isolationLevel ->
            let txBuffer = newTransactionBuffer bufferPool

            let txConcurrency =
                match isolationLevel with
                | ReadCommitted -> newReadCommitted state.NextTxNo lockTable
                | RepeatableRead -> newRepeatableRead state.NextTxNo lockTable
                | Serializable -> newSerializable state.NextTxNo lockTable

            let txRecovery =
                newTransactionRecovery logMgr state.NextTxNo readOnly

            let tx =
                newTransaction
                    txCommitListener
                    txRollbackListener
                    txRecovery
                    systemRecovery.OnCommit
                    systemRecovery.OnRollback
                    txConcurrency
                    txBuffer
                    state.NextTxNo
                    readOnly

            { state with
                  ActiveTxs = tx :: state.ActiveTxs
                  NextTxNo = state.NextTxNo + 1L },
            tx

    let createCheckpoint bufferPool systemRecovery state =
        bufferPool.FlushAll()
        state.ActiveTxs
        |> List.map (fun atx -> atx.TransactionNo)
        |> systemRecovery.Checkpoint

    let rollbackAll state =
        state.ActiveTxs
        |> List.iter (fun tx -> tx.Rollback())

let newTransactionManager logMgr bufferPool lockTable systemRecovery =
    let mutable state: TransactionManager.TransactionManagerState = { ActiveTxs = []; NextTxNo = 0L }
    let txNoLock = obj ()

    { NewTransaction =
          fun readOnly isolationLevel ->
              lock txNoLock (fun () ->
                  let nextstate, tx =
                      TransactionManager.createTransaction logMgr bufferPool lockTable systemRecovery (fun tx ->
                          lock txNoLock (fun () -> state <- TransactionManager.onTxCommit state tx)) (fun tx ->
                          lock txNoLock (fun () -> state <- TransactionManager.onTxRollback state tx)) state readOnly
                          isolationLevel

                  state <- nextstate
                  tx)
      GetNextTxNo = fun () -> lock txNoLock (fun () -> state.NextTxNo)
      GetActiveTxCount = fun () -> lock txNoLock (fun () -> state.ActiveTxs.Length)
      CreateCheckpoint =
          fun () -> lock txNoLock (fun () -> TransactionManager.createCheckpoint bufferPool systemRecovery state)
      RollbackAll = fun () -> lock txNoLock (fun () -> TransactionManager.rollbackAll state) }
