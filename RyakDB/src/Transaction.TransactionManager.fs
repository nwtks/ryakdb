module RyakDB.Transaction.TransactionManager

open RyakDB.Storage.Log
open RyakDB.Buffer
open RyakDB.Buffer.BufferManager
open RyakDB.Concurrency
open RyakDB.Recovery
open RyakDB.Recovery.TransactionRecoveryFinalize
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

    let getNextTxNo state = state.NextTxNo

    let getActiveTxCount state = state.ActiveTxs.Count

    let onTxCommit state tx =
        { state with
              ActiveTxs = state.ActiveTxs.Remove tx.TransactionNumber }

    let onTxRollback state tx =
        { state with
              ActiveTxs = state.ActiveTxs.Remove tx.TransactionNumber }

    let createTransaction fileMgr logMgr bufferPoolMgr lockTable catalogMgr txCommitListener txRollbackListener state =
        fun readOnly isolationLevel ->
            let recoveryMgr =
                RecoveryManager.newRecoveryManager logMgr state.NextTxNo readOnly

            let bufferMgr =
                BufferManager.newBufferManager bufferPoolMgr state.NextTxNo

            let concurMgr =
                match isolationLevel with
                | ReadCommitted -> ConcurrencyManager.newReadCommitted state.NextTxNo lockTable
                | RepeatableRead -> ConcurrencyManager.newRepeatableRead state.NextTxNo lockTable
                | Serializable -> ConcurrencyManager.newSerializable state.NextTxNo lockTable

            let tx =
                Transaction.newTransaction
                    txCommitListener
                    txRollbackListener
                    recoveryMgr
                    (TransactionRecoveryFinalize.onCommit logMgr)
                    (TransactionRecoveryFinalize.onRollback fileMgr logMgr catalogMgr)
                    concurMgr
                    bufferMgr
                    state.NextTxNo
                    readOnly

            { state with
                  ActiveTxs = state.ActiveTxs.Add tx.TransactionNumber
                  NextTxNo = state.NextTxNo + 1L },
            tx

    let createCheckpoint (logMgr: LogManager) state =
        fun tx ->
            tx.BufferMgr.FlushAll()
            state.ActiveTxs
            |> Set.toList
            |> tx.RecoveryMgr.Checkpoint
            |> logMgr.Flush

    let newTransactionManager fileMgr logMgr bufferPoolMgr lockTable catalogMgr =
        let mutable state = { ActiveTxs = Set.empty; NextTxNo = 0L }

        let txNoLock = obj ()

        { NewTransaction =
              fun readOnly isolationLevel ->
                  lock txNoLock (fun () ->
                      let nextstate, tx =
                          createTransaction fileMgr logMgr bufferPoolMgr lockTable catalogMgr (fun tx ->
                              lock txNoLock (fun () -> state <- onTxCommit state tx)) (fun tx ->
                              lock txNoLock (fun () -> state <- onTxRollback state tx)) state readOnly isolationLevel

                      state <- nextstate
                      tx)
          GetNextTxNo = fun () -> getNextTxNo state
          GetActiveTxCount = fun () -> getActiveTxCount state
          CreateCheckpoint = fun tx -> createCheckpoint logMgr state tx }
