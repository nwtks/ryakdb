module RyakDB.Transaction

open RyakDB.Buffer.TransactionBuffer
open RyakDB.Concurrency.TransactionConcurrency
open RyakDB.Recovery.TransactionRecovery

type Transaction =
    { Recovery: TransactionRecovery
      Concurrency: TransactionConcurrency
      Buffer: TransactionBuffer
      TransactionNumber: int64
      ReadOnly: bool
      Commit: unit -> unit
      Rollback: unit -> unit
      EndStatement: unit -> unit }

type IsolationLevel =
    | Serializable
    | RepeatableRead
    | ReadCommitted

module Transaction =
    let inline commit commitListeners tx =
        commitListeners |> List.iter (fun f -> f (tx))

    let inline rollback rollbackListeners tx =
        rollbackListeners |> List.iter (fun f -> f (tx))

    let inline endStatement endStatementListeners tx =
        endStatementListeners
        |> List.iter (fun f -> f (tx))

let newTransaction txCommitListener
                   txRollbackListener
                   txRecovery
                   recoveryCommitListener
                   recoveryRollbackListener
                   txConcurrency
                   txBuffer
                   txNo
                   readOnly
                   =
    let commitListeners =
        [ txCommitListener
          recoveryCommitListener
          (fun _ -> txConcurrency.OnTxCommit())
          (fun _ -> txBuffer.UnpinAll()) ]

    let rollbackListeners =
        [ txRollbackListener
          recoveryRollbackListener
          (fun _ -> txConcurrency.OnTxRollback())
          (fun _ -> txBuffer.UnpinAll()) ]

    let endStatementListeners =
        [ (fun _ -> txConcurrency.OnTxEndStatement()) ]

    let mutable callbackTx = None

    let tx =
        { Recovery = txRecovery
          Concurrency = txConcurrency
          Buffer = txBuffer
          TransactionNumber = txNo
          ReadOnly = readOnly
          Commit =
              fun () ->
                  callbackTx
                  |> Option.get
                  |> Transaction.commit commitListeners
          Rollback =
              fun () ->
                  callbackTx
                  |> Option.get
                  |> Transaction.rollback rollbackListeners
          EndStatement =
              fun () ->
                  callbackTx
                  |> Option.get
                  |> Transaction.endStatement endStatementListeners }

    callbackTx <- Some(tx)
    tx
