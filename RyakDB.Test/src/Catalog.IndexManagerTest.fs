module RyakDB.Test.Catalog.IndexManagerTest

open Xunit
open FsUnit.Xunit
open RyakDB.DataType
open RyakDB.Table
open RyakDB.Index
open RyakDB.Transaction
open RyakDB.Catalog
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
    Assert.Equal(i1, (index1 |> Option.get).IndexName)
    Assert.Equal([ "AAA" ], (index1 |> Option.get).FieldNames |> List.toSeq)

    let index2 = db.CatalogMgr.GetIndexInfoByName tx i2
    Assert.Equal(i2, (index2 |> Option.get).IndexName)
    Assert.Equal([ "BBB" ], (index2 |> Option.get).FieldNames |> List.toSeq)

    let index3 = db.CatalogMgr.GetIndexInfoByName tx i3
    Assert.Equal(i3, (index3 |> Option.get).IndexName)
    Assert.Equal([ "CCC" ], (index3 |> Option.get).FieldNames |> List.toSeq)

    let index4 =
        db.CatalogMgr.GetIndexInfoByName tx "test_create_index_I4"

    Assert.True(Option.isNone index4)

    let indexes1 =
        db.CatalogMgr.GetIndexInfoByField tx tbl "AAA"

    Assert.Equal(i1, indexes1.Head.IndexName)
    Assert.Equal([ "AAA" ], indexes1.Head.FieldNames |> List.toSeq)

    let indexes2 =
        db.CatalogMgr.GetIndexInfoByField tx tbl "BBB"

    Assert.Equal(i2, indexes2.Head.IndexName)
    Assert.Equal([ "BBB" ], indexes2.Head.FieldNames |> List.toSeq)

    let indexes3 =
        db.CatalogMgr.GetIndexInfoByField tx tbl "CCC"

    Assert.Equal(i3, indexes3.Head.IndexName)
    Assert.Equal([ "CCC" ], indexes3.Head.FieldNames |> List.toSeq)

    let fields = db.CatalogMgr.GetIndexedFields tx tbl
    Assert.Equal([ "AAA"; "BBB"; "CCC" ], fields |> List.toSeq)

    tx.Commit()
