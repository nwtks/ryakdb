module RyakDB.Test.Recovery.RecoveryServiceTest

open Xunit
open FsUnit.Xunit
open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Table
open RyakDB.Index
open RyakDB.Table.TableFile
open RyakDB.Transaction
open RyakDB.Database

let blockId = BlockId.newBlockId "_test_recover" 13L

let initBuffer db =
    let tx =
        db.Transaction.NewTransaction false Serializable

    let buff = tx.Buffer.Pin blockId
    buff.SetVal 4 (IntDbConstant 9876) None
    buff.SetVal 20 (DbConstant.newVarchar "abcdefg") None
    buff.SetVal 40 (DbConstant.newVarchar "hijk") None
    buff.SetVal 104 (IntDbConstant 9999) None
    buff.SetVal 120 (DbConstant.newVarchar "gfedcba") None
    buff.SetVal 140 (DbConstant.newVarchar "kjih") None
    buff.SetVal 204 (IntDbConstant 1415) None
    buff.SetVal 220 (DbConstant.newVarchar "pifo") None
    buff.SetVal 240 (DbConstant.newVarchar "urth") None
    buff.SetVal 304 (IntDbConstant 9265) None
    buff.SetVal 320 (DbConstant.newVarchar "piei") None
    buff.SetVal 340 (DbConstant.newVarchar "ghth") None
    buff.SetVal 404 (IntDbConstant 0) None
    buff.SetVal 420 (DbConstant.newVarchar "aaaa") None
    buff.SetVal 440 (DbConstant.newVarchar "AAAA") None
    buff.SetVal 504 (IntDbConstant 0) None
    buff.SetVal 520 (DbConstant.newVarchar "aaaa") None
    buff.SetVal 540 (DbConstant.newVarchar "AAAA") None
    tx.Buffer.Unpin buff
    tx.Commit()

let createTable db =
    let tx =
        db.Transaction.NewTransaction false Serializable

    Schema.newSchema ()
    |> (fun sch ->
        sch.AddField "cid" IntDbType
        sch.AddField "title" (VarcharDbType 100)
        sch.AddField "majorid" BigIntDbType
        db.Catalog.CreateTable tx "RecoveryTest" sch)
    tx.Commit()

let createIndex db =
    let tx =
        db.Transaction.NewTransaction false Serializable

    db.Catalog.CreateIndex tx "RecoveryTest_I1" BTree "RecoveryTest" [ "cid" ]
    tx.Commit()

[<Fact>]
let rollback () =
    use db =
        Database.defaultConfig ()
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    initBuffer db

    let tx =
        db.Transaction.NewTransaction false Serializable

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

    tx.Commit()

[<Fact>]
let recover () =
    use db =
        Database.defaultConfig ()
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    initBuffer db

    let tx1 =
        db.Transaction.NewTransaction false Serializable

    let buff = tx1.Buffer.Pin blockId
    tx1.Recovery.LogSetVal buff 104 (IntDbConstant 1234)
    |> buff.SetVal 104 (IntDbConstant 1234)
    tx1.Buffer.Unpin buff

    let tx2 =
        db.Transaction.NewTransaction false Serializable

    let buff = tx2.Buffer.Pin blockId
    tx2.Recovery.LogSetVal buff 120 (DbConstant.newVarchar "xyz")
    |> buff.SetVal 120 (DbConstant.newVarchar "xyz")
    tx2.Buffer.Unpin buff

    let tx3 =
        db.Transaction.NewTransaction false Serializable

    let buff = tx3.Buffer.Pin blockId
    tx3.Recovery.LogSetVal buff 140 (DbConstant.newVarchar "rst")
    |> buff.SetVal 140 (DbConstant.newVarchar "rst")
    tx3.Buffer.Unpin buff

    tx2.Commit()

    let tx4 =
        db.Transaction.NewTransaction false Serializable

    let buff = tx4.Buffer.Pin blockId
    buff.GetVal 104 IntDbType
    |> should equal (IntDbConstant 1234)
    buff.GetVal 120 (VarcharDbType 0)
    |> should equal (DbConstant.newVarchar "xyz")
    buff.GetVal 140 (VarcharDbType 0)
    |> should equal (DbConstant.newVarchar "rst")
    tx4.Buffer.Unpin buff

    let tx5 =
        db.Transaction.NewTransaction false Serializable

    db.Recovery.RecoverSystem tx5

    let buff = tx5.Buffer.Pin blockId
    buff.GetVal 104 IntDbType
    |> should equal (IntDbConstant 9999)
    buff.GetVal 120 (VarcharDbType 0)
    |> should equal (DbConstant.newVarchar "xyz")
    buff.GetVal 140 (VarcharDbType 0)
    |> should equal (DbConstant.newVarchar "kjih")
    tx5.Buffer.Unpin buff

    tx5.Commit()

[<Fact>]
let checkpoint () =
    use db =
        Database.defaultConfig ()
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    initBuffer db

    let tx1 =
        db.Transaction.NewTransaction false Serializable

    let buff = tx1.Buffer.Pin blockId
    tx1.Recovery.LogSetVal buff 204 (IntDbConstant 3538)
    |> buff.SetVal 204 (IntDbConstant 3538)
    tx1.Buffer.Unpin buff

    let tx2 =
        db.Transaction.NewTransaction false Serializable

    let buff = tx2.Buffer.Pin blockId
    tx2.Recovery.LogSetVal buff 220 (DbConstant.newVarchar "twel")
    |> buff.SetVal 220 (DbConstant.newVarchar "twel")
    tx2.Buffer.Unpin buff

    let tx3 =
        db.Transaction.NewTransaction false Serializable

    let buff = tx3.Buffer.Pin blockId
    tx3.Recovery.LogSetVal buff 240 (DbConstant.newVarchar "tfth")
    |> buff.SetVal 240 (DbConstant.newVarchar "tfth")
    tx3.Buffer.Unpin buff

    let tx4 =
        db.Transaction.NewTransaction false Serializable

    let buff = tx4.Buffer.Pin blockId
    tx4.Recovery.LogSetVal buff 304 (IntDbConstant 9323)
    |> buff.SetVal 304 (IntDbConstant 9323)
    tx4.Buffer.Unpin buff

    tx2.Commit()
    tx3.Rollback()

    db.Transaction.CreateCheckpoint()

    tx1.Commit()

    let tx5 =
        db.Transaction.NewTransaction false Serializable

    let buff = tx5.Buffer.Pin blockId
    tx5.Recovery.LogSetVal buff 320 (DbConstant.newVarchar "sixt")
    |> buff.SetVal 320 (DbConstant.newVarchar "sixt")
    tx5.Buffer.Unpin buff

    let tx6 =
        db.Transaction.NewTransaction false Serializable

    let buff = tx6.Buffer.Pin blockId
    tx6.Recovery.LogSetVal buff 240 (DbConstant.newVarchar "eenth")
    |> buff.SetVal 240 (DbConstant.newVarchar "eenth")
    tx6.Buffer.Unpin buff

    tx5.Commit()

    let tx7 =
        db.Transaction.NewTransaction false Serializable

    db.Recovery.RecoverSystem tx7

    let buff = tx7.Buffer.Pin blockId
    buff.GetVal 204 IntDbType
    |> should equal (IntDbConstant 3538)
    buff.GetVal 220 (VarcharDbType 0)
    |> should equal (DbConstant.newVarchar "twel")
    buff.GetVal 240 (VarcharDbType 0)
    |> should equal (DbConstant.newVarchar "urth")
    buff.GetVal 304 IntDbType
    |> should equal (IntDbConstant 9265)
    buff.GetVal 320 (VarcharDbType 0)
    |> should equal (DbConstant.newVarchar "sixt")
    buff.GetVal 340 (VarcharDbType 0)
    |> should equal (DbConstant.newVarchar "ghth")
    tx7.Buffer.Unpin buff

    tx7.Commit()

[<Fact>]
let ``table record`` () =
    use db =
        Database.defaultConfig ()
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    createTable db

    let tx1 =
        db.Transaction.NewTransaction false Serializable

    use tf1 =
        db.Catalog.GetTableInfo tx1 "RecoveryTest"
        |> Option.get
        |> newTableFile db.File tx1.Buffer tx1.Concurrency tx1.Recovery tx1.ReadOnly true

    tf1.Insert()
    tf1.SetVal "cid" (IntDbConstant 1)
    tf1.SetVal "title" (DbConstant.newVarchar "course1")
    tf1.SetVal "majorid" (BigIntDbConstant 1001L)

    tx1.Commit()

    let tx2 =
        db.Transaction.NewTransaction false Serializable

    use tf2 =
        db.Catalog.GetTableInfo tx2 "RecoveryTest"
        |> Option.get
        |> newTableFile db.File tx2.Buffer tx2.Concurrency tx2.Recovery tx2.ReadOnly true

    tf2.BeforeFirst()
    tf2.Next() |> should be True
    tf2.GetVal "title"
    |> DbConstant.toString
    |> should equal "course1"
    tf2.Delete()

    tx2.Rollback()

    let tx3 =
        db.Transaction.NewTransaction false Serializable

    use tf3 =
        db.Catalog.GetTableInfo tx3 "RecoveryTest"
        |> Option.get
        |> newTableFile db.File tx3.Buffer tx3.Concurrency tx3.Recovery tx3.ReadOnly true

    tf3.BeforeFirst()
    tf3.Next() |> should be True
    tf3.GetVal "title"
    |> DbConstant.toString
    |> should equal "course1"
    tf3.Next() |> should be False

    tf3.Insert()
    tf3.SetVal "cid" (IntDbConstant 2)
    tf3.SetVal "title" (DbConstant.newVarchar "course2")
    tf3.SetVal "majorid" (BigIntDbConstant 1002L)

    tx3.Rollback()

    let tx4 =
        db.Transaction.NewTransaction false Serializable

    use tf4 =
        db.Catalog.GetTableInfo tx4 "RecoveryTest"
        |> Option.get
        |> newTableFile db.File tx4.Buffer tx4.Concurrency tx4.Recovery tx4.ReadOnly true

    tf4.BeforeFirst()
    tf4.Next() |> should be True
    tf4.GetVal "title"
    |> DbConstant.toString
    |> should equal "course1"
    tf4.Next() |> should be False

    db.Catalog.DropTable tx4 "RecoveryTest"
    tx4.Commit()

[<Fact>]
let ``B-tree index`` () =
    use db =
        Database.defaultConfig ()
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    createTable db
    createIndex db

    let blk = BlockId.newBlockId "RecoveryTest.tbl" 0L

    let key5 =
        SearchKey.newSearchKey [ IntDbConstant 5 ]

    let rids =
        Array.init 10 (fun i -> RecordId.newRecordId i blk)

    let key7 =
        SearchKey.newSearchKey [ IntDbConstant 7 ]

    let rid2 = RecordId.newRecordId 6 blk

    let key777 =
        SearchKey.newSearchKey [ IntDbConstant 777 ]

    let rid3 = RecordId.newRecordId 999 blk

    let tx1 =
        db.Transaction.NewTransaction false Serializable

    use index1 =
        db.Catalog.GetIndexInfosByField tx1 "RecoveryTest" "cid"
        |> List.head
        |> IndexFactory.newIndex db.File tx1

    rids
    |> Array.iter (fun id -> index1.Insert true key5 id)
    index1.Insert true key7 rid2

    tx1.Commit()

    let tx2 =
        db.Transaction.NewTransaction false Serializable

    use index2 =
        db.Catalog.GetIndexInfosByField tx2 "RecoveryTest" "cid"
        |> List.head
        |> IndexFactory.newIndex db.File tx2

    let mutable cnt = 0
    SearchRange.newSearchRangeBySearchKey key5
    |> index2.BeforeFirst
    while index2.Next() do
        cnt <- cnt + 1
    cnt |> should equal 10

    SearchRange.newSearchRangeBySearchKey key7
    |> index2.BeforeFirst
    index2.Next() |> should be True
    index2.GetDataRecordId() |> should equal rid2
    index2.Next() |> should be False

    tx2.Commit()

    let tx3 =
        db.Transaction.NewTransaction false Serializable

    use index3 =
        db.Catalog.GetIndexInfosByField tx3 "RecoveryTest" "cid"
        |> List.head
        |> IndexFactory.newIndex db.File tx3

    index3.Delete true key7 rid2
    index3.Insert true key777 rid3

    tx3.Rollback()

    let tx4 =
        db.Transaction.NewTransaction false Serializable

    use index4 =
        db.Catalog.GetIndexInfosByField tx4 "RecoveryTest" "cid"
        |> List.head
        |> IndexFactory.newIndex db.File tx4

    SearchRange.newSearchRangeBySearchKey key7
    |> index4.BeforeFirst
    index4.Next() |> should be True
    index4.GetDataRecordId() |> should equal rid2

    SearchRange.newSearchRangeBySearchKey key777
    |> index4.BeforeFirst
    index4.Next() |> should be False

    db.Catalog.DropTable tx4 "RecoveryTest"
    tx4.Commit()
