module RyakDB.Test.Catalog.TableManagerTest

open Xunit
open FsUnit.Xunit
open RyakDB.DataType
open RyakDB.Table
open RyakDB.Transaction
open RyakDB.Catalog.CatalogManager
open RyakDB.Database

[<Fact>]
let ``create table`` () =
    let db =
        { Database.defaultConfig () with
              InMemory = true }
        |> createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let tx =
        db.TxMgr.NewTransaction false Serializable

    let t1 = "test_create_table_T1"
    db.CatalogMgr.DropTable tx t1
    let sch1 = Schema.newSchema ()
    sch1.AddField "AAA" IntDbType
    sch1.AddField "BBB" (VarcharDbType 30)
    db.CatalogMgr.CreateTable tx t1 sch1

    let t2 = "test_create_table_T2"
    db.CatalogMgr.DropTable tx t2
    let sch2 = Schema.newSchema ()
    sch2.AddField "AAA" IntDbType
    sch2.AddField "CCC" (VarcharDbType 123)
    db.CatalogMgr.CreateTable tx t2 sch2

    let ti1 = db.CatalogMgr.GetTableInfo tx t1
    (ti1 |> Option.get).TableName |> should equal t1
    (ti1 |> Option.get).Schema.HasField "AAA"
    |> should be True
    (ti1 |> Option.get).Schema.HasField "BBB"
    |> should be True
    (ti1 |> Option.get).Schema.HasField "CCC"
    |> should be False

    let ti2 = db.CatalogMgr.GetTableInfo tx t2
    (ti2 |> Option.get).TableName |> should equal t2
    (ti2 |> Option.get).Schema.HasField "AAA"
    |> should be True
    (ti2 |> Option.get).Schema.HasField "BBB"
    |> should be False
    (ti2 |> Option.get).Schema.HasField "CCC"
    |> should be True

    db.CatalogMgr.GetTableInfo tx "test_create_table_T3"
    |> should equal None

    tx.Commit()
