module RyakDB.Test.Recovery.RecoveryServiceTest

open Xunit
open FsUnit.Xunit
open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Table
open RyakDB.Index
open RyakDB.Transaction
open RyakDB.Table.TableFile
open RyakDB.Database
open RyakDB.Recovery.RecoveryService

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

    let buff1 = tx1.Buffer.Pin blockId
    tx1.Recovery.LogSetVal buff1 104 (IntDbConstant 1234)
    |> buff1.SetVal 104 (IntDbConstant 1234)
    tx1.Buffer.Unpin buff1

    let tx2 =
        db.Transaction.NewTransaction false Serializable

    let buff2 = tx2.Buffer.Pin blockId
    tx2.Recovery.LogSetVal buff2 120 (DbConstant.newVarchar "xyz")
    |> buff2.SetVal 120 (DbConstant.newVarchar "xyz")
    tx2.Buffer.Unpin buff2

    let tx3 =
        db.Transaction.NewTransaction false Serializable

    let buff3 = tx3.Buffer.Pin blockId
    tx3.Recovery.LogSetVal buff3 140 (DbConstant.newVarchar "rst")
    |> buff3.SetVal 140 (DbConstant.newVarchar "rst")
    tx3.Buffer.Unpin buff3

    tx2.Commit()

    let tx4 =
        db.Transaction.NewTransaction true Serializable

    let buff4 = tx4.Buffer.Pin blockId
    buff4.GetVal 104 IntDbType
    |> should equal (IntDbConstant 1234)
    buff4.GetVal 120 (VarcharDbType 0)
    |> should equal (DbConstant.newVarchar "xyz")
    buff4.GetVal 140 (VarcharDbType 0)
    |> should equal (DbConstant.newVarchar "rst")
    tx4.Buffer.Unpin buff4
    tx4.Commit()

    let tx5 =
        db.Transaction.NewTransaction false Serializable

    db.Recovery.RecoverSystem tx5
    tx5.Commit()

    let tx6 =
        db.Transaction.NewTransaction true Serializable

    let buff6 = tx6.Buffer.Pin blockId
    buff6.GetVal 104 IntDbType
    |> should equal (IntDbConstant 9999)
    buff6.GetVal 120 (VarcharDbType 0)
    |> should equal (DbConstant.newVarchar "xyz")
    buff6.GetVal 140 (VarcharDbType 0)
    |> should equal (DbConstant.newVarchar "kjih")
    tx6.Buffer.Unpin buff6
    tx6.Commit()

[<Fact>]
let checkpoint () =
    use db =
        Database.defaultConfig ()
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    initBuffer db

    let tx1 =
        db.Transaction.NewTransaction false Serializable

    let buff1 = tx1.Buffer.Pin blockId
    tx1.Recovery.LogSetVal buff1 204 (IntDbConstant 3538)
    |> buff1.SetVal 204 (IntDbConstant 3538)
    tx1.Buffer.Unpin buff1

    let tx2 =
        db.Transaction.NewTransaction false Serializable

    let buff2 = tx2.Buffer.Pin blockId
    tx2.Recovery.LogSetVal buff2 220 (DbConstant.newVarchar "twel")
    |> buff2.SetVal 220 (DbConstant.newVarchar "twel")
    tx2.Buffer.Unpin buff2

    let tx3 =
        db.Transaction.NewTransaction false Serializable

    let buff3 = tx3.Buffer.Pin blockId
    tx3.Recovery.LogSetVal buff3 240 (DbConstant.newVarchar "tfth")
    |> buff3.SetVal 240 (DbConstant.newVarchar "tfth")
    tx3.Buffer.Unpin buff3

    let tx4 =
        db.Transaction.NewTransaction false Serializable

    let buff4 = tx4.Buffer.Pin blockId
    tx4.Recovery.LogSetVal buff4 304 (IntDbConstant 9323)
    |> buff4.SetVal 304 (IntDbConstant 9323)
    tx4.Buffer.Unpin buff4

    tx2.Commit()
    tx3.Rollback()

    db.Transaction.CreateCheckpoint()

    tx1.Commit()

    let tx5 =
        db.Transaction.NewTransaction false Serializable

    let buff5 = tx5.Buffer.Pin blockId
    tx5.Recovery.LogSetVal buff5 320 (DbConstant.newVarchar "sixt")
    |> buff5.SetVal 320 (DbConstant.newVarchar "sixt")
    tx5.Buffer.Unpin buff5

    let tx6 =
        db.Transaction.NewTransaction false Serializable

    let buff6 = tx6.Buffer.Pin blockId
    tx6.Recovery.LogSetVal buff6 240 (DbConstant.newVarchar "eenth")
    |> buff6.SetVal 240 (DbConstant.newVarchar "eenth")
    tx6.Buffer.Unpin buff6

    tx5.Commit()

    let tx7 =
        db.Transaction.NewTransaction false Serializable

    db.Recovery.RecoverSystem tx7
    tx7.Commit()

    let tx8 =
        db.Transaction.NewTransaction true Serializable

    let buff8 = tx8.Buffer.Pin blockId
    buff8.GetVal 204 IntDbType
    |> should equal (IntDbConstant 3538)
    buff8.GetVal 220 (VarcharDbType 0)
    |> should equal (DbConstant.newVarchar "twel")
    buff8.GetVal 240 (VarcharDbType 0)
    |> should equal (DbConstant.newVarchar "urth")
    buff8.GetVal 304 IntDbType
    |> should equal (IntDbConstant 9265)
    buff8.GetVal 320 (VarcharDbType 0)
    |> should equal (DbConstant.newVarchar "sixt")
    buff8.GetVal 340 (VarcharDbType 0)
    |> should equal (DbConstant.newVarchar "ghth")
    tx8.Buffer.Unpin buff8
    tx8.Commit()

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
        |> newTableFile db.File tx1 true

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
        |> newTableFile db.File tx2 true

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
        |> newTableFile db.File tx3 true

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
        db.Transaction.NewTransaction true Serializable

    use tf4 =
        db.Catalog.GetTableInfo tx4 "RecoveryTest"
        |> Option.get
        |> newTableFile db.File tx4 true

    tf4.BeforeFirst()
    tf4.Next() |> should be True
    tf4.GetVal "title"
    |> DbConstant.toString
    |> should equal "course1"
    tf4.Next() |> should be False
    tx4.Commit()

    let tx5 =
        db.Transaction.NewTransaction false Serializable

    db.Catalog.DropTable tx5 "RecoveryTest"
    tx5.Commit()

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
        db.Catalog.GetIndexInfosByTable tx1 "RecoveryTest"
        |> List.head
        |> IndexFactory.newIndex db.File tx1

    rids
    |> Array.iter (fun id -> index1.Insert true key5 id)
    index1.Insert true key7 rid2

    tx1.Commit()

    let tx2 =
        db.Transaction.NewTransaction false Serializable

    db.Recovery.RecoverSystem tx2
    tx2.Commit()

    let tx3 =
        db.Transaction.NewTransaction true Serializable

    use index3 =
        db.Catalog.GetIndexInfosByTable tx3 "RecoveryTest"
        |> List.head
        |> IndexFactory.newIndex db.File tx3

    let mutable cnt = 0
    SearchRange.newSearchRangeBySearchKey key5
    |> index3.BeforeFirst
    while index3.Next() do
        cnt <- cnt + 1
    cnt |> should equal 10

    SearchRange.newSearchRangeBySearchKey key7
    |> index3.BeforeFirst
    index3.Next() |> should be True
    index3.GetDataRecordId() |> should equal rid2
    index3.Next() |> should be False

    tx3.Commit()

    let tx4 =
        db.Transaction.NewTransaction false Serializable

    use index4 =
        db.Catalog.GetIndexInfosByTable tx4 "RecoveryTest"
        |> List.head
        |> IndexFactory.newIndex db.File tx4

    index4.Delete true key7 rid2
    index4.Insert true key777 rid3

    tx4.Rollback()

    let tx5 =
        db.Transaction.NewTransaction true Serializable

    use index5 =
        db.Catalog.GetIndexInfosByTable tx5 "RecoveryTest"
        |> List.head
        |> IndexFactory.newIndex db.File tx5

    SearchRange.newSearchRangeBySearchKey key7
    |> index5.BeforeFirst
    index5.Next() |> should be True
    index5.GetDataRecordId() |> should equal rid2

    SearchRange.newSearchRangeBySearchKey key777
    |> index5.BeforeFirst
    index5.Next() |> should be False

    tx5.Commit()

    let tx6 =
        db.Transaction.NewTransaction false Serializable

    db.Catalog.DropTable tx6 "RecoveryTest"
    tx6.Commit()

[<Fact>]
let ``crash during rollback`` () =
    use db =
        Database.defaultConfig ()
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    initBuffer db

    let tx1 =
        db.Transaction.NewTransaction false Serializable

    let tx2 =
        db.Transaction.NewTransaction false Serializable

    let tx3 =
        db.Transaction.NewTransaction false Serializable

    let buff1 = tx1.Buffer.Pin blockId
    let buff2 = tx2.Buffer.Pin blockId
    let buff3 = tx3.Buffer.Pin blockId

    tx1.Recovery.LogSetVal buff1 504 (IntDbConstant 1111)
    |> buff1.SetVal 504 (IntDbConstant 1111)

    tx2.Recovery.LogSetVal buff2 520 (DbConstant.newVarchar "bbbb")
    |> buff2.SetVal 520 (DbConstant.newVarchar "bbbb")

    tx3.Recovery.LogSetVal buff3 540 (DbConstant.newVarchar "BBBB")
    |> buff3.SetVal 540 (DbConstant.newVarchar "BBBB")

    tx1.Recovery.LogSetVal buff1 504 (IntDbConstant 2222)
    |> buff1.SetVal 504 (IntDbConstant 2222)

    tx2.Recovery.LogSetVal buff2 520 (DbConstant.newVarchar "cccc")
    |> buff2.SetVal 520 (DbConstant.newVarchar "cccc")

    tx3.Recovery.LogSetVal buff3 540 (DbConstant.newVarchar "CCCC")
    |> buff3.SetVal 540 (DbConstant.newVarchar "CCCC")

    tx1.Buffer.Unpin buff1
    tx2.Buffer.Unpin buff2
    tx3.Buffer.Unpin buff3

    tx3.Commit()

    let tx4 =
        db.Transaction.NewTransaction true Serializable

    let buff4 = tx4.Buffer.Pin blockId
    buff4.GetVal 504 IntDbType
    |> should equal (IntDbConstant 2222)
    buff4.GetVal 520 (VarcharDbType 0)
    |> should equal (DbConstant.newVarchar "cccc")
    buff4.GetVal 540 (VarcharDbType 0)
    |> should equal (DbConstant.newVarchar "CCCC")
    tx4.Buffer.Unpin buff4
    tx4.Commit()

    RecoveryService.rollbackPartially db.File db.Log db.Catalog tx1 5
    RecoveryService.rollbackPartially db.File db.Log db.Catalog tx2 6

    let tx5 =
        db.Transaction.NewTransaction false Serializable

    db.Recovery.RecoverSystem tx5
    tx5.Commit()

    let tx6 =
        db.Transaction.NewTransaction true Serializable

    let buff6 = tx6.Buffer.Pin blockId
    buff6.GetVal 504 IntDbType
    |> should equal (IntDbConstant 0)
    buff6.GetVal 520 (VarcharDbType 0)
    |> should equal (DbConstant.newVarchar "aaaa")
    buff6.GetVal 540 (VarcharDbType 0)
    |> should equal (DbConstant.newVarchar "CCCC")
    tx6.Buffer.Unpin buff6
    tx6.Commit()

[<Fact>]
let ``crash during recovery`` () =
    use db =
        Database.defaultConfig ()
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    initBuffer db

    let tx1 =
        db.Transaction.NewTransaction false Serializable

    let tx2 =
        db.Transaction.NewTransaction false Serializable

    let tx3 =
        db.Transaction.NewTransaction false Serializable

    let buff1 = tx1.Buffer.Pin blockId
    let buff2 = tx2.Buffer.Pin blockId
    let buff3 = tx3.Buffer.Pin blockId

    tx1.Recovery.LogSetVal buff1 404 (IntDbConstant 1111)
    |> buff1.SetVal 404 (IntDbConstant 1111)

    tx2.Recovery.LogSetVal buff2 420 (DbConstant.newVarchar "bbbb")
    |> buff2.SetVal 420 (DbConstant.newVarchar "bbbb")

    tx3.Recovery.LogSetVal buff3 440 (DbConstant.newVarchar "BBBB")
    |> buff3.SetVal 440 (DbConstant.newVarchar "BBBB")

    tx1.Recovery.LogSetVal buff1 404 (IntDbConstant 2222)
    |> buff1.SetVal 404 (IntDbConstant 2222)

    tx2.Recovery.LogSetVal buff2 420 (DbConstant.newVarchar "cccc")
    |> buff2.SetVal 420 (DbConstant.newVarchar "cccc")

    tx3.Recovery.LogSetVal buff3 440 (DbConstant.newVarchar "CCCC")
    |> buff3.SetVal 440 (DbConstant.newVarchar "CCCC")

    tx1.Buffer.Unpin buff1
    tx2.Buffer.Unpin buff2
    tx3.Buffer.Unpin buff3

    tx3.Commit()

    let tx4 =
        db.Transaction.NewTransaction true Serializable

    let buff4 = tx4.Buffer.Pin blockId
    buff4.GetVal 404 IntDbType
    |> should equal (IntDbConstant 2222)
    buff4.GetVal 420 (VarcharDbType 0)
    |> should equal (DbConstant.newVarchar "cccc")
    buff4.GetVal 440 (VarcharDbType 0)
    |> should equal (DbConstant.newVarchar "CCCC")
    tx4.Buffer.Unpin buff4
    tx4.Commit()

    let tx5 =
        db.Transaction.NewTransaction false Serializable

    RecoveryService.recoverPartially db.File db.Log db.Catalog tx5 7
    tx5.Commit()

    let tx6 =
        db.Transaction.NewTransaction false Serializable

    db.Recovery.RecoverSystem tx6
    tx6.Commit()

    let tx7 =
        db.Transaction.NewTransaction true Serializable

    let buff7 = tx7.Buffer.Pin blockId
    buff7.GetVal 404 IntDbType
    |> should equal (IntDbConstant 0)
    buff7.GetVal 420 (VarcharDbType 0)
    |> should equal (DbConstant.newVarchar "aaaa")
    buff7.GetVal 440 (VarcharDbType 0)
    |> should equal (DbConstant.newVarchar "CCCC")
    tx7.Buffer.Unpin buff7
    tx7.Commit()
