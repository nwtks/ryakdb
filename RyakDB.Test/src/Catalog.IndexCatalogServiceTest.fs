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
    (index1 |> Option.get).IndexName
    |> should equal i1
    (index1 |> Option.get).FieldNames
    |> should equal [ "AAA" ]

    let index2 = db.Catalog.GetIndexInfoByName tx i2
    (index2 |> Option.get).IndexName
    |> should equal i2
    (index2 |> Option.get).FieldNames
    |> should equal [ "BBB" ]

    let index3 = db.Catalog.GetIndexInfoByName tx i3
    (index3 |> Option.get).IndexName
    |> should equal i3
    (index3 |> Option.get).FieldNames
    |> should equal [ "CCC" ]

    db.Catalog.GetIndexInfoByName tx "test_create_index_I4"
    |> should equal None

    let indexes1 =
        db.Catalog.GetIndexInfosByField tx tbl "AAA"

    indexes1.Head.IndexName |> should equal i1
    indexes1.Head.FieldNames |> should equal [ "AAA" ]

    let indexes2 =
        db.Catalog.GetIndexInfosByField tx tbl "BBB"

    indexes2.Head.IndexName |> should equal i2
    indexes2.Head.FieldNames |> should equal [ "BBB" ]

    let indexes3 =
        db.Catalog.GetIndexInfosByField tx tbl "CCC"

    indexes3.Head.IndexName |> should equal i3
    indexes3.Head.FieldNames |> should equal [ "CCC" ]

    db.Catalog.GetIndexedFields tx tbl
    |> should equal [ "AAA"; "BBB"; "CCC" ]

    tx.Commit()