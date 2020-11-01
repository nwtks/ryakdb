module RyakDB.Test.Catalog.ViewCatalogServiceTest

open Xunit
open FsUnit.Xunit
open RyakDB.Transaction
open RyakDB.Catalog.CatalogService
open RyakDB.Database

[<Fact>]
let ``create view`` () =
    use db =
        { Database.defaultConfig () with
              BlockSize = 1024
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let tx1 =
        db.Transaction.NewTransaction false Serializable

    let v1 = "test_create_view_V1"
    db.Catalog.DropView tx1 v1
    db.Catalog.CreateView tx1 v1 "abcde"

    let v2 = "test_create_view_V2"
    db.Catalog.DropView tx1 v2
    db.Catalog.CreateView tx1 v2 "select * from T"

    tx1.Commit()

    let tx2 =
        db.Transaction.NewTransaction true Serializable

    db.Catalog.GetViewDef tx2 v1
    |> Option.get
    |> should equal "abcde"

    db.Catalog.GetViewDef tx2 v2
    |> Option.get
    |> should equal "select * from T"

    db.Catalog.GetViewDef tx2 "test_create_view_V3"
    |> should equal None

    tx2.Commit()

    let tx3 =
        db.Transaction.NewTransaction false Serializable

    db.Catalog.DropView tx3 v2
    db.Catalog.DropView tx3 v1
    tx3.Commit()
