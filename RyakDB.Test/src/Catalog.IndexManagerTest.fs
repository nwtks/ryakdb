module RyakDB.Test.Catalog.IndexManagerTest

open Xunit
open FsUnit.Xunit
open RyakDB.DataType
open RyakDB.Table
open RyakDB.Index
open RyakDB.Transaction
open RyakDB.Catalog.CatalogManager
open RyakDB.Database

[<Fact>]
let ``create index`` () =
    let db =
        { Database.defaultConfig () with
              InMemory = true }
        |> createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let tx =
        db.TxMgr.NewTransaction false Serializable

    let tbl = "test_create_index_TBL"
    db.CatalogMgr.DropTable tx tbl
    let sch = Schema.newSchema ()
    sch.AddField "AAA" IntDbType
    sch.AddField "BBB" (VarcharDbType 123)
    sch.AddField "CCC" BigIntDbType
    db.CatalogMgr.CreateTable tx tbl sch

    let i1 = "test_create_index_I1"
    db.CatalogMgr.DropIndex tx i1
    db.CatalogMgr.CreateIndex tx i1 IndexType.Hash tbl [ "AAA" ]

    let i2 = "test_create_index_I2"
    db.CatalogMgr.DropIndex tx i2
    db.CatalogMgr.CreateIndex tx i2 IndexType.BTree tbl [ "BBB" ]

    let i3 = "test_create_index_I3"
    db.CatalogMgr.DropIndex tx i3
    db.CatalogMgr.CreateIndex tx i3 IndexType.Hash tbl [ "CCC" ]

    let index1 = db.CatalogMgr.GetIndexInfoByName tx i1
    (index1 |> Option.get).IndexName
    |> should equal i1
    (index1 |> Option.get).FieldNames
    |> should equal [ "AAA" ]

    let index2 = db.CatalogMgr.GetIndexInfoByName tx i2
    (index2 |> Option.get).IndexName
    |> should equal i2
    (index2 |> Option.get).FieldNames
    |> should equal [ "BBB" ]

    let index3 = db.CatalogMgr.GetIndexInfoByName tx i3
    (index3 |> Option.get).IndexName
    |> should equal i3
    (index3 |> Option.get).FieldNames
    |> should equal [ "CCC" ]

    db.CatalogMgr.GetIndexInfoByName tx "test_create_index_I4"
    |> should equal None

    let indexes1 =
        db.CatalogMgr.GetIndexInfoByField tx tbl "AAA"

    indexes1.Head.IndexName |> should equal i1
    indexes1.Head.FieldNames |> should equal [ "AAA" ]

    let indexes2 =
        db.CatalogMgr.GetIndexInfoByField tx tbl "BBB"

    indexes2.Head.IndexName |> should equal i2
    indexes2.Head.FieldNames |> should equal [ "BBB" ]

    let indexes3 =
        db.CatalogMgr.GetIndexInfoByField tx tbl "CCC"

    indexes3.Head.IndexName |> should equal i3
    indexes3.Head.FieldNames |> should equal [ "CCC" ]

    db.CatalogMgr.GetIndexedFields tx tbl
    |> should equal [ "AAA"; "BBB"; "CCC" ]

    tx.Commit()
