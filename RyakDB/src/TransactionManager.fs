module RyakDB.TransactionManager

open RyakDB.Storage.Log
open RyakDB.Buffer.BufferPool
open RyakDB.Buffer.TransactionBuffer
open RyakDB.Concurrency.TransactionConcurrency
open RyakDB.Recovery.TransactionRecovery
open RyakDB.Recovery
open RyakDB.Transaction

type TransactionManager =
    { NewTransaction: bool -> IsolationLevel -> Transaction
      GetNextTxNo: unit -> int64
      GetActiveTxCount: unit -> int32
      CreateCheckpoint: Transaction -> unit }

module TransactionManager =
    type TransactionManagerState =
        { ActiveTxs: Set<int64>
          NextTxNo: int64 }

    let onTxCommit state tx =
        { state with
              ActiveTxs = state.ActiveTxs.Remove tx.TransactionNo }

    let onTxRollback state tx =
        { state with
              ActiveTxs = state.ActiveTxs.Remove tx.TransactionNo }

    let createTransaction fileMgr logMgr bufferPool lockTable catalogMgr txCommitListener txRollbackListener state =
        fun readOnly isolationLevel ->
            let txRecovery =
                newTransactionRecovery logMgr state.NextTxNo readOnly

            let txBuffer = newTransactionBuffer bufferPool

            let txConcurrency =
                match isolationLevel with
                | ReadCommitted -> newReadCommitted state.NextTxNo lockTable
                | RepeatableRead -> newRepeatableRead state.NextTxNo lockTable
                | Serializable -> newSerializable state.NextTxNo lockTable

            let tx =
                newTransaction
                    txCommitListener
                    txRollbackListener
                    txRecovery
                    (SystemRecovery.onCommit logMgr)
                    (SystemRecovery.onRollback fileMgr logMgr catalogMgr)
                    txConcurrency
                    txBuffer
                    state.NextTxNo
                    readOnly

            { state with
                  ActiveTxs = state.ActiveTxs.Add tx.TransactionNo
                  NextTxNo = state.NextTxNo + 1L },
            tx

    let createCheckpoint logMgr bufferPool state =
        fun tx ->
            bufferPool.FlushAll()
            state.ActiveTxs
            |> Set.toList
            |> tx.Recovery.Checkpoint
            |> logMgr.Flush

let newTransactionManager fileMgr logMgr bufferPool lockTable catalogMgr =
    let mutable state: TransactionManager.TransactionManagerState = { ActiveTxs = Set.empty; NextTxNo = 0L }
    let txNoLock = obj ()

    { NewTransaction =
          fun readOnly isolationLevel ->
              lock txNoLock (fun () ->
                  let nextstate, tx =
                      TransactionManager.createTransaction fileMgr logMgr bufferPool lockTable catalogMgr (fun tx ->
                          lock txNoLock (fun () -> state <- TransactionManager.onTxCommit state tx)) (fun tx ->
                          lock txNoLock (fun () -> state <- TransactionManager.onTxRollback state tx)) state readOnly
                          isolationLevel

                  state <- nextstate
                  tx)
      GetNextTxNo = fun () -> state.NextTxNo
      GetActiveTxCount = fun () -> state.ActiveTxs.Count
      CreateCheckpoint = fun tx -> TransactionManager.createCheckpoint logMgr bufferPool state tx }
