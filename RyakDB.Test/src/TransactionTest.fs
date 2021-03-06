module RyakDB.Test.TransactionTest

open Xunit
open FsUnit.Xunit
open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Transaction
open RyakDB.Database

[<Fact>]
let commit () =
    let filename = "test_commit"

    use db =
        { Database.defaultConfig () with
              BlockSize = 1024
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let blk = BlockId.newBlockId filename 13L
    let c = IntDbConstant(123457)

    let tx1 =
        db.Transaction.NewTransaction false Serializable

    let buff1 = tx1.Buffer.Pin blk
    tx1.Concurrency.ModifyBlock blk
    tx1.Recovery.LogSetVal buff1 0 c
    |> buff1.SetVal 0 c
    tx1.Commit()

    let tx2 =
        db.Transaction.NewTransaction false Serializable

    let buff2 = tx2.Buffer.Pin blk
    tx2.Concurrency.ReadBlock blk
    buff2.GetVal 0 IntDbType |> should equal c
    tx2.Commit()

[<Fact>]
let rollback () =
    let filename = "test_rollback"

    use db =
        { Database.defaultConfig () with
              BlockSize = 1024
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let blk = BlockId.newBlockId filename 17L
    let c1 = IntDbConstant(55555)
    let c2 = IntDbConstant(999)

    let tx1 =
        db.Transaction.NewTransaction false Serializable

    let buff1 = tx1.Buffer.Pin blk
    tx1.Concurrency.ModifyBlock blk
    tx1.Recovery.LogSetVal buff1 0 c1
    |> buff1.SetVal 0 c1
    tx1.Commit()

    let tx2 =
        db.Transaction.NewTransaction false Serializable

    let buff2 = tx2.Buffer.Pin blk
    tx2.Concurrency.ModifyBlock blk
    tx2.Recovery.LogSetVal buff2 0 c2
    |> buff2.SetVal 0 c2
    tx2.Rollback()

    let tx3 =
        db.Transaction.NewTransaction false Serializable

    let buff3 = tx3.Buffer.Pin blk
    tx3.Concurrency.ReadBlock blk
    buff3.GetVal 0 IntDbType |> should equal c1
    tx3.Commit()

[<Fact>]
let ``end statement`` () =
    let filename = "test_end_statement"

    use db =
        { Database.defaultConfig () with
              BlockSize = 1024
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let blk = BlockId.newBlockId filename 19L

    let tx1 =
        db.Transaction.NewTransaction false ReadCommitted

    let buff1 = tx1.Buffer.Pin blk
    tx1.Concurrency.ReadBlock blk
    buff1.GetVal 0 IntDbType |> ignore
    tx1.EndStatement()

    let tx2 =
        db.Transaction.NewTransaction false Serializable

    tx1.Buffer.Pin blk |> ignore
    tx2.Concurrency.ModifyBlock blk

    tx2.Commit()
    tx1.Commit()
