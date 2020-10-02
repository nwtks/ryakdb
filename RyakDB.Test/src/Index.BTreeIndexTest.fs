module RyakDB.Test.Index.BTreeIndexTest

open Xunit
open FsUnit.Xunit
open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Table
open RyakDB.Index
open RyakDB.Transaction
open RyakDB.Database

let createTable db =
    let tx =
        db.TxMgr.NewTransaction false Serializable

    Schema.newSchema ()
    |> (fun sch ->
        sch.AddField "cid" IntDbType
        sch.AddField "title" (VarcharDbType 100)
        sch.AddField "deptid" IntDbType
        sch.AddField "majorid" BigIntDbType
        db.CatalogMgr.CreateTable tx "BITable" sch)
    tx.Commit()

let createIndex db =
    let tx =
        db.TxMgr.NewTransaction false Serializable

    db.CatalogMgr.CreateIndex tx "BITable_SI1" IndexType.BTree "BITable" [ "cid" ]
    db.CatalogMgr.CreateIndex tx "BITable_SI2" IndexType.BTree "BITable" [ "title" ]
    db.CatalogMgr.CreateIndex tx "BITable_SI3" IndexType.BTree "BITable" [ "deptid" ]
    db.CatalogMgr.CreateIndex tx "BITable_SI4" IndexType.BTree "BITable" [ "majorid" ]
    db.CatalogMgr.CreateIndex tx "BITable_MI1" IndexType.BTree "BITable" [ "cid"; "deptid" ]
    tx.Commit()

[<Fact>]
let ``single key`` () =
    use db =
        { Database.defaultConfig () with
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    createTable db
    createIndex db

    let tx =
        db.TxMgr.NewTransaction false Serializable

    let index =
        db.CatalogMgr.GetIndexInfoByName tx "BITable_SI1"
        |> Option.get
        |> IndexFactory.newIndex db.FileMgr tx

    let blk = BlockId.newBlockId "BITable.tbl" 0L

    let key5 =
        SearchKey.newSearchKey [ IntDbConstant 5 ]

    let rids =
        Array.init 10 (fun i -> RecordId.newRecordId i blk)

    rids
    |> Array.iter (fun id -> index.Insert false key5 id)

    let key7 =
        SearchKey.newSearchKey [ IntDbConstant 7 ]

    let rid2 = RecordId.newRecordId 6 blk
    index.Insert false key7 rid2

    let mutable cnt = 0
    SearchRange.newSearchRangeBySearchKey key5
    |> index.BeforeFirst
    while index.Next() do
        cnt <- cnt + 1
    cnt |> should equal 10

    SearchRange.newSearchRangeBySearchKey key7
    |> index.BeforeFirst
    index.Next() |> should be True
    index.GetDataRecordId() |> should equal rid2
    index.Next() |> should be False

    rids
    |> Array.iter (fun id -> index.Delete false key5 id)
    SearchRange.newSearchRangeBySearchKey key5
    |> index.BeforeFirst
    index.Next() |> should be False

    SearchRange.newSearchRangeBySearchKey key7
    |> index.BeforeFirst
    index.Next() |> should be True

    index.Close()
    db.CatalogMgr.DropTable tx "BITable"
    tx.Commit()

[<Fact>]
let ``varchar key`` () =
    use db =
        { Database.defaultConfig () with
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    createTable db
    createIndex db

    let tx =
        db.TxMgr.NewTransaction false Serializable

    let index =
        db.CatalogMgr.GetIndexInfoByField tx "BITable" "title"
        |> List.head
        |> IndexFactory.newIndex db.FileMgr tx

    let blk = BlockId.newBlockId "BITable.tbl" 0L

    let key1 =
        SearchKey.newSearchKey [ DbConstant.newVarchar "BAEBAEBAEBASAEBASE" ]

    let key2 =
        SearchKey.newSearchKey [ DbConstant.newVarchar "KARBAEBAEBASAEBASE" ]

    let key3 =
        SearchKey.newSearchKey [ DbConstant.newVarchar "AAEBAEBAEBASAEBASZ" ]

    let key4 =
        SearchKey.newSearchKey [ DbConstant.newVarchar "BAEBAEBAEBASAEBASZ1" ]

    for i in 0 .. 999 do
        RecordId.newRecordId i blk
        |> index.Insert false key1
        RecordId.newRecordId (1000 + i) blk
        |> index.Insert false key2
        RecordId.newRecordId (2000 + i) blk
        |> index.Insert false key3
        RecordId.newRecordId (3000 + i) blk
        |> index.Insert false key4

    let mutable cnt = 0
    SearchRange.newSearchRangeBySearchKey key1
    |> index.BeforeFirst
    while index.Next() do
        cnt <- cnt + 1
    cnt |> should equal 1000
    SearchRange.newSearchRangeBySearchKey key2
    |> index.BeforeFirst
    while index.Next() do
        cnt <- cnt + 1
    cnt |> should equal 2000
    SearchRange.newSearchRangeBySearchKey key3
    |> index.BeforeFirst
    while index.Next() do
        cnt <- cnt + 1
    cnt |> should equal 3000
    SearchRange.newSearchRangeBySearchKey key4
    |> index.BeforeFirst
    while index.Next() do
        cnt <- cnt + 1
    cnt |> should equal 4000

    for i in 0 .. 999 do
        RecordId.newRecordId i blk
        |> index.Delete false key1

    SearchRange.newSearchRangeBySearchKey key1
    |> index.BeforeFirst
    index.Next() |> should be False

    SearchRange.newSearchRangeBySearchKey key2
    |> index.BeforeFirst
    index.Next() |> should be True

    SearchRange.newSearchRangeBySearchKey key3
    |> index.BeforeFirst
    index.Next() |> should be True

    SearchRange.newSearchRangeBySearchKey key4
    |> index.BeforeFirst
    index.Next() |> should be True

    index.Close()
    db.CatalogMgr.DropTable tx "BITable"
    tx.Commit()

[<Fact>]
let ``multi key`` () =
    use db =
        { Database.defaultConfig () with
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    createTable db
    createIndex db

    let tx =
        db.TxMgr.NewTransaction false Serializable

    let index =
        db.CatalogMgr.GetIndexInfoByName tx "BITable_MI1"
        |> Option.get
        |> IndexFactory.newIndex db.FileMgr tx

    let key11 =
        SearchKey.newSearchKey [ IntDbConstant 1
                                 IntDbConstant 1 ]

    let blk1 = BlockId.newBlockId "BITable.tbl" 1L

    let rids1 =
        Array.init 10 (fun i -> RecordId.newRecordId i blk1)

    rids1
    |> Array.iter (fun id -> index.Insert false key11 id)

    let key21 =
        SearchKey.newSearchKey [ IntDbConstant 2
                                 IntDbConstant 1 ]

    let blk2 = BlockId.newBlockId "BITable.tbl" 2L
    let rid2 = RecordId.newRecordId 100 blk2
    index.Insert false key21 rid2

    let key12 =
        SearchKey.newSearchKey [ IntDbConstant 1
                                 IntDbConstant 2 ]

    let blk3 = BlockId.newBlockId "BITable.tbl" 3L

    let rids3 =
        Array.init 7 (fun i -> RecordId.newRecordId i blk3)

    rids3
    |> Array.iter (fun id -> index.Insert false key12 id)

    let mutable cnt = 0
    SearchRange.newSearchRangeBySearchKey key11
    |> index.BeforeFirst
    while index.Next() do
        cnt <- cnt + 1
    cnt |> should equal 10

    SearchRange.newSearchRangeBySearchKey key21
    |> index.BeforeFirst
    index.Next() |> should be True
    index.GetDataRecordId() |> should equal rid2
    index.Next() |> should be False

    SearchRange.newSearchRangeBySearchKey key12
    |> index.BeforeFirst
    while index.Next() do
        cnt <- cnt + 1
    cnt |> should equal 17

    rids1
    |> Array.iter (fun id -> index.Delete false key11 id)
    SearchRange.newSearchRangeBySearchKey key11
    |> index.BeforeFirst
    index.Next() |> should be False

    SearchRange.newSearchRangeBySearchKey key21
    |> index.BeforeFirst
    index.Next() |> should be True

    SearchRange.newSearchRangeBySearchKey key12
    |> index.BeforeFirst
    while index.Next() do
        cnt <- cnt + 1
    cnt |> should equal 24

    index.Close()
    db.CatalogMgr.DropTable tx "BITable"
    tx.Commit()

[<Fact>]
let ``branch overflow`` () =
    use db =
        { Database.defaultConfig () with
              BlockSize = 2048
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    createTable db
    createIndex db

    let tx =
        db.TxMgr.NewTransaction false Serializable

    let index =
        db.CatalogMgr.GetIndexInfoByField tx "BITable" "majorid"
        |> List.head
        |> IndexFactory.newIndex db.FileMgr tx

    let blk = BlockId.newBlockId "BITable.tbl" 0L

    for i in 9999L .. -1L .. 0L do
        RecordId.newRecordId (int32 i) blk
        |> index.Insert false (SearchKey.newSearchKey [ BigIntDbConstant(i % 1000L) ])

    let key123 =
        SearchKey.newSearchKey [ IntDbConstant 123 ]

    let mutable cnt = 0
    SearchRange.newSearchRangeBySearchKey key123
    |> index.BeforeFirst
    while index.Next() do
        cnt <- cnt + 1
    cnt |> should equal 10

    for i in 9999L .. -1L .. 0L do
        RecordId.newRecordId (int32 i) blk
        |> index.Delete false (SearchKey.newSearchKey [ BigIntDbConstant(i % 1000L) ])

    SearchRange.newSearchRangeBySearchKey key123
    |> index.BeforeFirst
    index.Next() |> should be False

    index.Close()
    db.CatalogMgr.DropTable tx "BITable"
    tx.Commit()

[<Fact>]
let ``search range`` () =
    use db =
        { Database.defaultConfig () with
              BlockSize = 2048
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    createTable db
    createIndex db

    let tx =
        db.TxMgr.NewTransaction false Serializable

    let index =
        db.CatalogMgr.GetIndexInfoByName tx "BITable_SI3"
        |> Option.get
        |> IndexFactory.newIndex db.FileMgr tx

    let blk0 = BlockId.newBlockId "BITable.tbl" 0L
    let blk23 = BlockId.newBlockId "BITable.tbl" 23L

    for i in 0 .. 9999 do
        RecordId.newRecordId i blk0
        |> index.Insert false (SearchKey.newSearchKey [ IntDbConstant(i % 500) ])

    let key7 =
        SearchKey.newSearchKey [ IntDbConstant 7 ]

    for i in 0 .. 99 do
        RecordId.newRecordId i blk23
        |> index.Insert false key7

    let mutable cnt = 0

    SearchRange.newSearchRangeBySearchKey key7
    |> index.BeforeFirst
    while index.Next() do
        cnt <- cnt + 1
    cnt |> should equal 120

    cnt <- 0
    SearchRange.newSearchRangeByRanges [ DbConstantRange.newConstantRange
                                             (Some(IntDbConstant 5))
                                             true
                                             (Some(IntDbConstant 5))
                                             true ]
    |> index.BeforeFirst
    while index.Next() do
        cnt <- cnt + 1
    cnt |> should equal 20

    cnt <- 0
    SearchRange.newSearchRangeByRanges [ DbConstantRange.newConstantRange
                                             (Some(IntDbConstant 10))
                                             true
                                             (Some(IntDbConstant 12))
                                             true ]
    |> index.BeforeFirst
    while index.Next() do
        cnt <- cnt + 1
    cnt |> should equal 60

    cnt <- 0
    SearchRange.newSearchRangeByRanges [ DbConstantRange.newConstantRange
                                             (Some(IntDbConstant 10))
                                             true
                                             (Some(IntDbConstant 12))
                                             false ]
    |> index.BeforeFirst
    while index.Next() do
        cnt <- cnt + 1
    cnt |> should equal 40

    cnt <- 0

    let range =
        SearchRange.newSearchRangeByRanges [ DbConstantRange.newConstantRange
                                                 (Some(IntDbConstant 10))
                                                 false
                                                 (Some(IntDbConstant 12))
                                                 true ]

    range |> index.BeforeFirst
    while index.Next() do
        cnt <- cnt + 1
    cnt |> should equal 40

    cnt <- 0
    SearchRange.newSearchRangeByRanges [ DbConstantRange.newConstantRange
                                             (Some(IntDbConstant 10))
                                             false
                                             (Some(IntDbConstant 12))
                                             false ]
    |> index.BeforeFirst
    while index.Next() do
        cnt <- cnt + 1
    cnt |> should equal 20

    cnt <- 0
    SearchRange.newSearchRangeByRanges [ DbConstantRange.newConstantRange None false (Some(IntDbConstant 5)) false ]
    |> index.BeforeFirst
    while index.Next() do
        cnt <- cnt + 1
    cnt |> should equal 100

    cnt <- 0
    SearchRange.newSearchRangeByRanges [ DbConstantRange.newConstantRange None false (Some(IntDbConstant 5)) true ]
    |> index.BeforeFirst
    while index.Next() do
        cnt <- cnt + 1
    cnt |> should equal 120

    cnt <- 0
    SearchRange.newSearchRangeByRanges [ DbConstantRange.newConstantRange (Some(IntDbConstant 480)) false None false ]
    |> index.BeforeFirst
    while index.Next() do
        cnt <- cnt + 1
    cnt |> should equal 380

    cnt <- 0
    SearchRange.newSearchRangeByRanges [ DbConstantRange.newConstantRange (Some(IntDbConstant 480)) true None false ]
    |> index.BeforeFirst
    while index.Next() do
        cnt <- cnt + 1
    cnt |> should equal 400

    for i in 0 .. 9999 do
        RecordId.newRecordId i blk0
        |> index.Delete false (SearchKey.newSearchKey [ IntDbConstant(i % 500) ])
    SearchKey.newSearchKey [ IntDbConstant 5 ]
    |> SearchRange.newSearchRangeBySearchKey
    |> index.BeforeFirst
    index.Next() |> should be False

    for i in 0 .. 99 do
        RecordId.newRecordId i blk23
        |> index.Delete false key7
    SearchRange.newSearchRangeBySearchKey key7
    |> index.BeforeFirst
    index.Next() |> should be False

    db.CatalogMgr.DropTable tx "BITable"
    tx.Commit()
