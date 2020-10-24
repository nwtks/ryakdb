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

    let tx =
        db.Transaction.NewTransaction false Serializable

    let tbl = "test_create_index_TBL"
    db.Catalog.DropTable tx tbl
    let sch = Schema.newSchema ()
    sch.AddField "AAA" IntDbType
    sch.AddField "BBB" (VarcharDbType 123)
    sch.AddField "CCC" BigIntDbType
    db.Catalog.CreateTable tx tbl sch

    let i1 = "test_create_index_I1"
    db.Catalog.DropIndex tx i1
    db.Catalog.CreateIndex tx i1 Hash tbl [ "AAA" ]

    let i2 = "test_create_index_I2"
    db.Catalog.DropIndex tx i2
    db.Catalog.CreateIndex tx i2 BTree tbl [ "BBB" ]

    let i3 = "test_create_index_I3"
    db.Catalog.DropIndex tx i3
    db.Catalog.CreateIndex tx i3 Hash tbl [ "CCC" ]

    let index1 = db.Catalog.GetIndexInfoByName tx i1
    index1
    |> Option.get
    |> IndexInfo.indexName
    |> should equal i1
    index1
    |> Option.get
    |> IndexInfo.fieldNames
    |> should equal [ "AAA" ]

    let index2 = db.Catalog.GetIndexInfoByName tx i2
    index2
    |> Option.get
    |> IndexInfo.indexName
    |> should equal i2
    index2
    |> Option.get
    |> IndexInfo.fieldNames
    |> should equal [ "BBB" ]

    let index3 = db.Catalog.GetIndexInfoByName tx i3
    index3
    |> Option.get
    |> IndexInfo.indexName
    |> should equal i3
    index3
    |> Option.get
    |> IndexInfo.fieldNames
    |> should equal [ "CCC" ]

    db.Catalog.GetIndexInfoByName tx "test_create_index_I4"
    |> should equal None

    let indexes =
        db.Catalog.GetIndexInfosByTable tx tbl
        |> List.sortBy IndexInfo.indexName

    indexes
    |> List.map IndexInfo.indexName
    |> should equal [ i1; i2; i3 ]
    indexes
    |> List.map IndexInfo.fieldNames
    |> should equal [ [ "AAA" ]; [ "BBB" ]; [ "CCC" ] ]

    tx.Commit()
