module RyakDB.Test.Recovery.SystemRecoveryTest

open Xunit
open FsUnit.Xunit
open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Transaction
open RyakDB.Database
open RyakDB.Recovery.SystemRecovery

let blockId = BlockId.newBlockId "_test_recover" 13L

let initBuffer db =
    let tx =
        db.TxMgr.NewTransaction false Serializable

    let buff = tx.Buffer.Pin blockId
    buff.SetVal 4 (IntDbConstant 9876) None
    buff.SetVal 20 (DbConstant.newVarchar "abcdefg") None
    buff.SetVal 104 (IntDbConstant 9999) None
    buff.SetVal 120 (DbConstant.newVarchar "gfedcba") None
    buff.SetVal 140 (DbConstant.newVarchar "kjih") None
    tx.Buffer.Unpin buff
    tx.Commit()

[<Fact>]
let rollback () =
    use db =
        Database.defaultConfig ()
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    initBuffer db

    let tx =
        db.TxMgr.NewTransaction false Serializable

    let buff = tx.Buffer.Pin blockId
    tx.Recovery.LogSetVal buff 4 (IntDbConstant 1234)
    |> buff.SetVal 4 (IntDbConstant 1234)
    tx.Recovery.LogSetVal buff 20 (DbConstant.newVarchar "xyz")
    |> buff.SetVal 20 (DbConstant.newVarchar "xyz")
    tx.Buffer.Unpin buff

    let buff = tx.Buffer.Pin blockId
    buff.GetVal 4 IntDbType
    |> should equal (IntDbConstant 1234)
    buff.GetVal 20 (VarcharDbType 0)
    |> should equal (DbConstant.newVarchar "xyz")
    tx.Buffer.Unpin buff

    tx.Rollback()

    let buff = tx.Buffer.Pin blockId
    buff.GetVal 4 IntDbType
    |> should equal (IntDbConstant 9876)
    buff.GetVal 20 (VarcharDbType 0)
    |> should equal (DbConstant.newVarchar "abcdefg")
    tx.Buffer.Unpin buff

[<Fact>]
let recover () =
    use db =
        Database.defaultConfig ()
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    initBuffer db

    let tx1 =
        db.TxMgr.NewTransaction false Serializable

    let buff = tx1.Buffer.Pin blockId
    tx1.Recovery.LogSetVal buff 104 (IntDbConstant 1234)
    |> buff.SetVal 104 (IntDbConstant 1234)
    tx1.Buffer.Unpin buff

    let tx2 =
        db.TxMgr.NewTransaction false Serializable

    let buff = tx2.Buffer.Pin blockId
    tx2.Recovery.LogSetVal buff 120 (DbConstant.newVarchar "xyz")
    |> buff.SetVal 120 (DbConstant.newVarchar "xyz")
    tx2.Buffer.Unpin buff
    tx2.Commit()

    let tx3 =
        db.TxMgr.NewTransaction false Serializable

    let buff = tx3.Buffer.Pin blockId
    tx3.Recovery.LogSetVal buff 140 (DbConstant.newVarchar "rst")
    |> buff.SetVal 140 (DbConstant.newVarchar "rst")
    tx3.Buffer.Unpin buff

    let tx4 =
        db.TxMgr.NewTransaction false Serializable

    let buff = tx4.Buffer.Pin blockId
    buff.GetVal 104 IntDbType
    |> should equal (IntDbConstant 1234)
    buff.GetVal 120 (VarcharDbType 0)
    |> should equal (DbConstant.newVarchar "xyz")
    buff.GetVal 140 (VarcharDbType 0)
    |> should equal (DbConstant.newVarchar "rst")
    tx4.Buffer.Unpin buff

    let tx5 =
        db.TxMgr.NewTransaction false Serializable

    recoverSystem db.FileMgr db.LogMgr db.BufferPool db.CatalogMgr tx5

    let buff = tx5.Buffer.Pin blockId
    buff.GetVal 104 IntDbType
    |> should equal (IntDbConstant 9999)
    buff.GetVal 120 (VarcharDbType 0)
    |> should equal (DbConstant.newVarchar "xyz")
    buff.GetVal 140 (VarcharDbType 0)
    |> should equal (DbConstant.newVarchar "kjih")
    tx5.Buffer.Unpin buff
