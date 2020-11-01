module RyakDB.Test.Catalog.IndexCatalogServiceTest

open Xunit
open FsUnit.Xunit
open RyakDB.DataType
open RyakDB.Table
open RyakDB.Index
open RyakDB.Transaction
open RyakDB.Catalog.CatalogService
open RyakDB.Database

[<Fact>]
let ``create index`` () =
    use db =
        { Database.defaultConfig () with
              BlockSize = 1024
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let tx1 =
        db.Transaction.NewTransaction false Serializable

    let tbl = "test_create_index_TBL"
    db.Catalog.DropTable tx1 tbl
    let sch = Schema.newSchema ()
    sch.AddField "AAA" IntDbType
    sch.AddField "BBB" (VarcharDbType 123)
    sch.AddField "CCC" BigIntDbType
    db.Catalog.CreateTable tx1 tbl sch

    let i1 = "test_create_index_I1"
    db.Catalog.DropIndex tx1 i1
    db.Catalog.CreateIndex tx1 i1 (Hash 100) tbl [ "AAA" ]

    let i2 = "test_create_index_I2"
    db.Catalog.DropIndex tx1 i2
    db.Catalog.CreateIndex tx1 i2 BTree tbl [ "BBB" ]

    let i3 = "test_create_index_I3"
    db.Catalog.DropIndex tx1 i3
    db.Catalog.CreateIndex tx1 i3 (Hash 100) tbl [ "CCC" ]

    tx1.Commit()

    let tx2 =
        db.Transaction.NewTransaction true Serializable

    let index1 = db.Catalog.GetIndexInfoByName tx2 i1
    index1
    |> Option.get
    |> IndexInfo.indexName
    |> should equal i1
    index1
    |> Option.get
    |> IndexInfo.fieldNames
    |> should equal [ "AAA" ]

    let index2 = db.Catalog.GetIndexInfoByName tx2 i2
    index2
    |> Option.get
    |> IndexInfo.indexName
    |> should equal i2
    index2
    |> Option.get
    |> IndexInfo.fieldNames
    |> should equal [ "BBB" ]

    let index3 = db.Catalog.GetIndexInfoByName tx2 i3
    index3
    |> Option.get
    |> IndexInfo.indexName
    |> should equal i3
    index3
    |> Option.get
    |> IndexInfo.fieldNames
    |> should equal [ "CCC" ]

    db.Catalog.GetIndexInfoByName tx2 "test_create_index_I4"
    |> should equal None

    let indexes =
        db.Catalog.GetIndexInfosByTable tx2 tbl
        |> List.sortBy IndexInfo.indexName

    indexes
    |> List.map IndexInfo.indexName
    |> should equal [ i1; i2; i3 ]
    indexes
    |> List.map IndexInfo.fieldNames
    |> should equal [ [ "AAA" ]; [ "BBB" ]; [ "CCC" ] ]

    tx2.Commit()

    let tx3 =
        db.Transaction.NewTransaction false Serializable

    db.Catalog.DropIndex tx3 i3
    db.Catalog.DropIndex tx3 i2
    db.Catalog.DropIndex tx3 i1
    db.Catalog.DropTable tx3 tbl
    tx3.Commit()
