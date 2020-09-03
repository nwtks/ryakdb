namespace RyakDB.Storage.Transaction

open RyakDB.Server.Task
open RyakDB.Storage.Type
open RyakDB.Storage.File
open RyakDB.Storage.Log
open RyakDB.Storage.Buffer
open RyakDB.Storage.Catalog
open RyakDB.Storage.Concurrency
open RyakDB.Storage.Recovery

module Transaction =
    let commit lifecycleListeners tx =
        lifecycleListeners
        |> List.iter (fun l -> l.OnTxCommit(tx))

    let rollback lifecycleListeners tx =
        lifecycleListeners
        |> List.iter (fun l -> l.OnTxRollback(tx))

    let endStatement lifecycleListeners tx =
        lifecycleListeners
        |> List.iter (fun l -> l.OnTxEndStatement(tx))

    let newTransaction txListener
                       recoveryMgr
                       recoveryListener
                       concurMgr
                       concurListener
                       bufferMgr
                       bufferListener
                       txNo
                       readOnly
                       =
        let lifecycleListeners =
            [ txListener
              recoveryListener
              concurListener
              bufferListener ]

        let mutable callbackTx = None

        let tx =
            { RecoveryMgr = recoveryMgr
              ConcurMgr = concurMgr
              BufferMgr = bufferMgr
              TransactionNumber = txNo
              ReadOnly = readOnly
              Commit =
                  fun () ->
                      callbackTx
                      |> Option.get
                      |> commit lifecycleListeners
              Rollback =
                  fun () ->
                      callbackTx
                      |> Option.get
                      |> rollback lifecycleListeners
              EndStatement =
                  fun () ->
                      callbackTx
                      |> Option.get
                      |> endStatement lifecycleListeners }

        callbackTx <- Some(tx)
        tx

module TransactionManager =
    type TransactionManagerState =
        { ActiveTxs: Set<int64>
          NextTxNo: int64 }

    let newTransaction (fileMgr: FileManager)
                       (logMgr: LogManager)
                       (catalogMgr: CatalogManager)
                       (bufferPoolMgr: BufferPoolManager)
                       (lockTable: LockTable)
                       (lifecycleListener: TransactionLifecycleListener)
                       (state: TransactionManagerState)
                       =
        fun readOnly isolationLevel ->
            let recoveryMgr, recoveryListener =
                RecoveryManager.newRecoveryManager fileMgr logMgr catalogMgr state.NextTxNo readOnly

            let bufferMgr, bufferListener =
                BufferManager.newBufferManager bufferPoolMgr state.NextTxNo

            let concurMgr, concurListener =
                ConcurrencyManager.newConcurrencyManager lockTable state.NextTxNo isolationLevel

            let tx =
                Transaction.newTransaction
                    lifecycleListener
                    recoveryMgr
                    recoveryListener
                    concurMgr
                    concurListener
                    bufferMgr
                    bufferListener
                    state.NextTxNo
                    readOnly

            { state with
                  ActiveTxs = state.ActiveTxs.Add tx.TransactionNumber
                  NextTxNo = state.NextTxNo + 1L },
            tx

    let createCheckpoint (logMgr: LogManager) (state: TransactionManagerState) =
        fun tx ->
            tx.BufferMgr.FlushAll()
            state.ActiveTxs
            |> Set.toList
            |> tx.RecoveryMgr.Checkpoint
            |> logMgr.Flush

    let getNextTxNo (state: TransactionManagerState) = state.NextTxNo

    let getActiveTxCount (state: TransactionManagerState) = state.ActiveTxs.Count

    let onTxCommit (state: TransactionManagerState) (tx: Transaction) =
        { state with
              ActiveTxs = state.ActiveTxs.Remove tx.TransactionNumber }

    let onTxRollback (state: TransactionManagerState) (tx: Transaction) =
        { state with
              ActiveTxs = state.ActiveTxs.Remove tx.TransactionNumber }

    let newTransactionManager (fileMgr: FileManager)
                              (logMgr: LogManager)
                              (catalogMgr: CatalogManager)
                              (bufferPoolMgr: BufferPoolManager)
                              (lockTable: LockTable)
                              =
        let mutable state = { ActiveTxs = Set.empty; NextTxNo = 0L }

        let txNoLock = obj ()

        let lifecycleListener =
            { OnTxCommit = fun tx -> lock txNoLock (fun () -> state <- onTxCommit state tx)
              OnTxRollback = fun tx -> lock txNoLock (fun () -> state <- onTxRollback state tx)
              OnTxEndStatement = fun _ -> () }

        { NewTransaction =
              fun readOnly isolationLevel ->
                  lock txNoLock (fun () ->
                      let nextstate, tx =
                          newTransaction
                              fileMgr
                              logMgr
                              catalogMgr
                              bufferPoolMgr
                              lockTable
                              lifecycleListener
                              state
                              readOnly
                              isolationLevel

                      state <- nextstate
                      tx)
          GetNextTxNo = fun () -> getNextTxNo state
          GetActiveTxCount = fun () -> getActiveTxCount state
          CreateCheckpoint = fun tx -> createCheckpoint logMgr state tx
          LifecycleListener = lifecycleListener }
