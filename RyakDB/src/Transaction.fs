module RyakDB.Transaction

open RyakDB.Buffer.TransactionBuffer
open RyakDB.Concurrency.TransactionConcurrency
open RyakDB.Recovery.TransactionRecovery

type Transaction =
    { Recovery: TransactionRecovery
      Concurrency: TransactionConcurrency
      Buffer: TransactionBuffer
      TransactionNo: int64
      ReadOnly: bool
      Commit: unit -> unit
      Rollback: unit -> unit
      EndStatement: unit -> unit }

type IsolationLevel =
    | Serializable
    | RepeatableRead
    | ReadCommitted

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
    let commit tx =
        txCommitListener tx
        recoveryCommitListener tx
        txConcurrency.OnTxCommit()
        txBuffer.UnpinAll()

    let rollback tx =
        txRollbackListener tx
        recoveryRollbackListener tx
        txConcurrency.OnTxRollback()
        txBuffer.UnpinAll()

    let endStatement () = txConcurrency.OnTxEndStatement()

    let rec tx =
        { Recovery = txRecovery
          Concurrency = txConcurrency
          Buffer = txBuffer
          TransactionNo = txNo
          ReadOnly = readOnly
          Commit = fun () -> commit tx
          Rollback = fun () -> rollback tx
          EndStatement = endStatement }

    tx
