module RyakDB.Test.TransactionTest

open Xunit
open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Concurrency
open RyakDB.Transaction
open RyakDB.Database

[<Fact>]
let commit () =
    let filename = "test_commit"

    let db =
        Database.newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let blk = BlockId.newBlockId filename 13L
    let c = IntSqlConstant(123457)

    let tx1 =
        db.TxMgr.NewTransaction false Serializable

    let buff1 = tx1.BufferMgr.Pin blk
    tx1.ConcurMgr.ModifyBlock blk
    tx1.RecoveryMgr.LogSetVal buff1 0 c
    |> buff1.SetVal 0 c
    tx1.Commit()

    let tx2 =
        db.TxMgr.NewTransaction false Serializable

    let buff2 = tx2.BufferMgr.Pin blk
    tx2.ConcurMgr.ReadBlock blk
    Assert.Equal(c, buff2.GetVal 0 IntSqlType)
    tx2.Commit()

[<Fact>]
let rollback () =
    let filename = "test_rollback"

    let db =
        Database.newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let blk = BlockId.newBlockId filename 17L
    let c1 = IntSqlConstant(55555)
    let c2 = IntSqlConstant(999)

    let tx1 =
        db.TxMgr.NewTransaction false Serializable

    let buff1 = tx1.BufferMgr.Pin blk
    tx1.ConcurMgr.ModifyBlock blk
    tx1.RecoveryMgr.LogSetVal buff1 0 c1
    |> buff1.SetVal 0 c1
    tx1.Commit()

    let tx2 =
        db.TxMgr.NewTransaction false Serializable

    let buff2 = tx2.BufferMgr.Pin blk
    tx2.ConcurMgr.ModifyBlock blk
    tx2.RecoveryMgr.LogSetVal buff2 0 c2
    |> buff2.SetVal 0 c2
    tx2.Rollback()

    let tx3 =
        db.TxMgr.NewTransaction false Serializable

    let buff3 = tx3.BufferMgr.Pin blk
    tx3.ConcurMgr.ReadBlock blk
    Assert.Equal(c1, buff3.GetVal 0 IntSqlType)
    tx3.Commit()

[<Fact>]
let ``end statement`` () =
    let filename = "test_end_statement"

    let db =
        Database.newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let blk = BlockId.newBlockId filename 19L

    let tx1 =
        db.TxMgr.NewTransaction false ReadCommitted

    let buff1 = tx1.BufferMgr.Pin blk
    tx1.ConcurMgr.ReadBlock blk
    buff1.GetVal 0 IntSqlType |> ignore
    tx1.EndStatement()

    let tx2 =
        db.TxMgr.NewTransaction false Serializable

    tx1.BufferMgr.Pin blk |> ignore
    tx2.ConcurMgr.ModifyBlock blk

    tx2.Commit()
    tx1.Commit()
