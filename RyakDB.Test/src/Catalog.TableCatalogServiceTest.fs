module RyakDB.Test.Catalog.TableCatalogServiceTest

open Xunit
open FsUnit.Xunit
open RyakDB.DataType
open RyakDB.Table
open RyakDB.Transaction
open RyakDB.Catalog.CatalogService
open RyakDB.Database

[<Fact>]
let ``create table`` () =
    use db =
        { Database.defaultConfig () with
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let tx =
        db.Transaction.NewTransaction false Serializable

    let t1 = "test_create_table_T1"
    db.Catalog.DropTable tx t1
    let sch1 = Schema.newSchema ()
    sch1.AddField "AAA" IntDbType
    sch1.AddField "BBB" (VarcharDbType 30)
    db.Catalog.CreateTable tx t1 sch1

    let t2 = "test_create_table_T2"
    db.Catalog.DropTable tx t2
    let sch2 = Schema.newSchema ()
    sch2.AddField "AAA" IntDbType
    sch2.AddField "CCC" (VarcharDbType 123)
    db.Catalog.CreateTable tx t2 sch2

    let ti1 = db.Catalog.GetTableInfo tx t1
    ti1
    |> Option.get
    |> TableInfo.tableName
    |> should equal t1
    (ti1 |> Option.get |> TableInfo.schema).HasField "AAA"
    |> should be True
    (ti1 |> Option.get |> TableInfo.schema).HasField "BBB"
    |> should be True
    (ti1 |> Option.get |> TableInfo.schema).HasField "CCC"
    |> should be False

    let ti2 = db.Catalog.GetTableInfo tx t2
    ti2
    |> Option.get
    |> TableInfo.tableName
    |> should equal t2
    (ti2 |> Option.get |> TableInfo.schema).HasField "AAA"
    |> should be True
    (ti2 |> Option.get |> TableInfo.schema).HasField "BBB"
    |> should be False
    (ti2 |> Option.get |> TableInfo.schema).HasField "CCC"
    |> should be True

    db.Catalog.GetTableInfo tx "test_create_table_T3"
    |> should equal None

    tx.Commit()
