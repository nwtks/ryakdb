module RyakDB.Test.Index.HashIndexTest

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
        sch.AddField "title" (VarcharDbType 500)
        sch.AddField "deptid" IntDbType
        db.CatalogMgr.CreateTable tx "HITable" sch)
    tx.Commit()

let createIndex db =
    let tx =
        db.TxMgr.NewTransaction false Serializable

    db.CatalogMgr.CreateIndex tx "HITable_SI1" IndexType.Hash "HITable" [ "cid" ]
    db.CatalogMgr.CreateIndex tx "HITable_SI2" IndexType.Hash "HITable" [ "title" ]
    db.CatalogMgr.CreateIndex tx "HITable_MI1" IndexType.Hash "HITable" [ "cid"; "deptid" ]
    tx.Commit()

[<Fact>]
let ``single key`` () =
    let db =
        { Database.defaultConfig () with
              InMemory = true }
        |> createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    createTable db
    createIndex db

    let tx =
        db.TxMgr.NewTransaction false Serializable

    let index =
        db.CatalogMgr.GetIndexInfoByName tx "HITable_SI1"
        |> Option.get
        |> IndexFactory.newIndex db.FileMgr tx

    let blk = BlockId.newBlockId "HITable.tbl" 0L

    let key5 =
        SearchKey.newSearchKey [ (IntDbConstant 5) ]

    let rids =
        Array.init 10 (fun i -> RecordId.newRecordId i blk)

    rids
    |> Array.iter (fun id -> index.Insert false key5 id)

    let key7 =
        SearchKey.newSearchKey [ (IntDbConstant 7) ]

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
    db.CatalogMgr.DropTable tx "HITable"
    tx.Commit()

[<Fact>]
let ``varchar key`` () =
    let db =
        { Database.defaultConfig () with
              InMemory = true }
        |> createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    createTable db
    createIndex db

    let tx =
        db.TxMgr.NewTransaction false Serializable

    let index =
        db.CatalogMgr.GetIndexInfoByField tx "HITable" "title"
        |> List.head
        |> IndexFactory.newIndex db.FileMgr tx

    let blk = BlockId.newBlockId "HITable.tbl" 0L

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
    db.CatalogMgr.DropTable tx "HITable"
    tx.Commit()

[<Fact>]
let ``multi key`` () =
    let db =
        { Database.defaultConfig () with
              InMemory = true }
        |> createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    createTable db
    createIndex db

    let tx =
        db.TxMgr.NewTransaction false Serializable

    let index =
        db.CatalogMgr.GetIndexInfoByName tx "HITable_MI1"
        |> Option.get
        |> IndexFactory.newIndex db.FileMgr tx

    let key11 =
        SearchKey.newSearchKey [ (IntDbConstant 1)
                                 (IntDbConstant 1) ]

    let blk1 = BlockId.newBlockId "HITable.tbl" 1L

    let rids1 =
        Array.init 10 (fun i -> RecordId.newRecordId i blk1)

    rids1
    |> Array.iter (fun id -> index.Insert false key11 id)

    let key21 =
        SearchKey.newSearchKey [ (IntDbConstant 2)
                                 (IntDbConstant 1) ]

    let blk2 = BlockId.newBlockId "HITable.tbl" 2L
    let rid2 = RecordId.newRecordId 100 blk2
    index.Insert false key21 rid2

    let key12 =
        SearchKey.newSearchKey [ (IntDbConstant 1)
                                 (IntDbConstant 2) ]

    let blk3 = BlockId.newBlockId "HITable.tbl" 3L

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
    db.CatalogMgr.DropTable tx "HITable"
    tx.Commit()
