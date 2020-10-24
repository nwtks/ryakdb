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

    let tx =
        db.Transaction.NewTransaction false Serializable

    let v1 = "test_create_view_V1"
    db.Catalog.DropView tx v1
    db.Catalog.CreateView tx v1 "abcde"

    let v2 = "test_create_view_V2"
    db.Catalog.DropView tx v2
    db.Catalog.CreateView tx v2 "select * from T"

    db.Catalog.GetViewDef tx v1
    |> Option.get
    |> should equal "abcde"

    db.Catalog.GetViewDef tx v2
    |> Option.get
    |> should equal "select * from T"

    db.Catalog.GetViewDef tx "test_create_view_V3"
    |> should equal None

    tx.Commit()
