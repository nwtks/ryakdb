module RyakDB.TransactionService

open RyakDB.Buffer.BufferPool
open RyakDB.Buffer.TransactionBuffer
open RyakDB.Concurrency.TransactionConcurrency
open RyakDB.Recovery.TransactionRecovery
open RyakDB.Transaction
open RyakDB.Recovery.RecoveryService

type TransactionService =
    { NewTransaction: bool -> IsolationLevel -> Transaction
      GetNextTxNo: unit -> int64
      GetActiveTxCount: unit -> int32
      CreateCheckpoint: unit -> unit
      RollbackAll: unit -> unit }

module TransactionService =
    type TransactionServiceState =
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

    let createTransaction logService bufferPool lockService recoveryService txCommitListener txRollbackListener state =
        fun readOnly isolationLevel ->
            let txBuffer = newTransactionBuffer bufferPool

            let txConcurrency =
                match isolationLevel with
                | ReadCommitted -> newReadCommitted state.NextTxNo lockService
                | RepeatableRead -> newRepeatableRead state.NextTxNo lockService
                | Serializable -> newSerializable state.NextTxNo lockService

            let txRecovery =
                newTransactionRecovery logService state.NextTxNo readOnly

            let tx =
                newTransaction
                    txCommitListener
                    txRollbackListener
                    txRecovery
                    recoveryService.OnCommit
                    recoveryService.OnRollback
                    txConcurrency
                    txBuffer
                    state.NextTxNo
                    readOnly

            { state with
                  ActiveTxs = tx :: state.ActiveTxs
                  NextTxNo = state.NextTxNo + 1L },
            tx

    let createCheckpoint bufferPool recoveryService state =
        bufferPool.FlushAll()
        state.ActiveTxs
        |> List.map (fun atx -> atx.TransactionNo)
        |> recoveryService.Checkpoint

    let rollbackAll state =
        state.ActiveTxs
        |> List.iter (fun tx -> tx.Rollback())

let newTransactionService logService bufferPool lockService recoveryService =
    let mutable state: TransactionService.TransactionServiceState = { ActiveTxs = []; NextTxNo = 0L }
    let txNoLock = obj ()

    { NewTransaction =
          fun readOnly isolationLevel ->
              lock txNoLock (fun () ->
                  let nextstate, tx =
                      TransactionService.createTransaction logService bufferPool lockService recoveryService (fun tx ->
                          lock txNoLock (fun () -> state <- TransactionService.onTxCommit state tx)) (fun tx ->
                          lock txNoLock (fun () -> state <- TransactionService.onTxRollback state tx)) state readOnly
                          isolationLevel

                  state <- nextstate
                  tx)
      GetNextTxNo = fun () -> lock txNoLock (fun () -> state.NextTxNo)
      GetActiveTxCount = fun () -> lock txNoLock (fun () -> List.length state.ActiveTxs)
      CreateCheckpoint =
          fun () -> lock txNoLock (fun () -> TransactionService.createCheckpoint bufferPool recoveryService state)
      RollbackAll = fun () -> lock txNoLock (fun () -> TransactionService.rollbackAll state) }
