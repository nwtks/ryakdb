namespace RyakDB.Transaction

open RyakDB.Buffer.BufferManager
open RyakDB.Concurrency
open RyakDB.Recovery

type Transaction =
    { RecoveryMgr: RecoveryManager
      ConcurMgr: ConcurrencyManager
      BufferMgr: BufferManager
      TransactionNumber: int64
      ReadOnly: bool
      Commit: unit -> unit
      Rollback: unit -> unit
      EndStatement: unit -> unit }

module Transaction =
    let commit commitListeners tx =
        commitListeners |> List.iter (fun f -> f (tx))

    let rollback rollbackListeners tx =
        rollbackListeners |> List.iter (fun f -> f (tx))

    let endStatement endStatementListeners tx =
        endStatementListeners
        |> List.iter (fun f -> f (tx))

    let newTransaction txCommitListener
                       txRollbackListener
                       recoveryMgr
                       recoveryCommitListener
                       recoveryRollbackListener
                       concurMgr
                       bufferMgr
                       txNo
                       readOnly
                       =
        let commitListeners =
            [ txCommitListener
              recoveryCommitListener
              (fun _ -> concurMgr.OnTxCommit())
              (fun _ -> bufferMgr.UnpinAll()) ]

        let rollbackListeners =
            [ txRollbackListener
              recoveryRollbackListener
              (fun _ -> concurMgr.OnTxRollback())
              (fun _ -> bufferMgr.UnpinAll()) ]

        let endStatementListeners =
            [ (fun _ -> concurMgr.OnTxEndStatement()) ]

        let mutable callbackTx = None

        let tx =
            { RecoveryMgr = recoveryMgr
              ConcurMgr = concurMgr
              BufferMgr = bufferMgr
              TransactionNumber = txNo
              ReadOnly = readOnly
              Commit = fun () -> callbackTx |> Option.get |> commit commitListeners
              Rollback =
                  fun () ->
                      callbackTx
                      |> Option.get
                      |> rollback rollbackListeners
              EndStatement =
                  fun () ->
                      callbackTx
                      |> Option.get
                      |> endStatement endStatementListeners }

        callbackTx <- Some(tx)
        tx
