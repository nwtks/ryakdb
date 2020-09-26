module RyakDB.Test.Catalog.ViewManagerTest

open Xunit
open FsUnit.Xunit
open RyakDB.Transaction
open RyakDB.Catalog.CatalogManager
open RyakDB.Database

[<Fact>]
let ``create view`` () =
    let db =
        { Database.defaultConfig () with
              InMemory = true }
        |> createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let tx =
        db.TxMgr.NewTransaction false Serializable

    let v1 = "test_create_view_V1"
    db.CatalogMgr.DropView tx v1
    db.CatalogMgr.CreateView tx v1 "abcde"

    let v2 = "test_create_view_V2"
    db.CatalogMgr.DropView tx v2
    db.CatalogMgr.CreateView tx v2 "select * from T"

    db.CatalogMgr.GetViewDef tx v1
    |> Option.get
    |> should equal "abcde"

    db.CatalogMgr.GetViewDef tx v2
    |> Option.get
    |> should equal "select * from T"

    db.CatalogMgr.GetViewDef tx "test_create_view_V3"
    |> should equal None

    tx.Commit()
