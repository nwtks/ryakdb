module RyakDB.Test.CatalogTest

open Xunit
open RyakDB.DataType
open RyakDB.Query
open RyakDB.Table
open RyakDB.Transaction
open RyakDB.Catalog
open RyakDB.Database

module TableManagerTest =
    [<Fact>]
    let ``create table`` () =
        let db =
            { Database.defaultConfig () with
                  InMemory = true }
            |> Database.createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

        let tx =
            db.TxMgr.NewTransaction false Serializable

        let t1 = "test_create_table_T1"
        db.CatalogMgr.DropTable tx t1
        let sch1 = Schema.newSchema ()
        sch1.AddField "AAA" IntSqlType
        sch1.AddField "BBB" (VarcharSqlType 30)
        db.CatalogMgr.CreateTable tx t1 sch1

        let t2 = "test_create_table_T2"
        db.CatalogMgr.DropTable tx t2
        let sch2 = Schema.newSchema ()
        sch2.AddField "AAA" IntSqlType
        sch2.AddField "CCC" (VarcharSqlType 123)
        db.CatalogMgr.CreateTable tx t2 sch2

        let ti1 = db.CatalogMgr.GetTableInfo tx t1
        Assert.Equal(t1, (ti1 |> Option.get).TableName)
        Assert.True((ti1 |> Option.get).Schema.HasField "AAA")
        Assert.True((ti1 |> Option.get).Schema.HasField "BBB")
        Assert.False((ti1 |> Option.get).Schema.HasField "CCC")

        let ti2 = db.CatalogMgr.GetTableInfo tx t2
        Assert.Equal(t2, (ti2 |> Option.get).TableName)
        Assert.True((ti2 |> Option.get).Schema.HasField "AAA")
        Assert.False((ti2 |> Option.get).Schema.HasField "BBB")
        Assert.True((ti2 |> Option.get).Schema.HasField "CCC")

        let ti3 =
            db.CatalogMgr.GetTableInfo tx "test_create_table_T3"

        Assert.True(Option.isNone ti3)

        tx.Commit()

module IndexManagerTest =
    [<Fact>]
    let ``create index`` () =
        let db =
            { Database.defaultConfig () with
                  InMemory = true }
            |> Database.createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

        let tx =
            db.TxMgr.NewTransaction false Serializable

        let tbl = "test_create_index_TBL"
        db.CatalogMgr.DropTable tx tbl
        let sch = Schema.newSchema ()
        sch.AddField "AAA" IntSqlType
        sch.AddField "BBB" (VarcharSqlType 123)
        sch.AddField "CCC" BigIntSqlType
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

module ViewManagerTest =
    [<Fact>]
    let ``create view`` () =
        let db =
            { Database.defaultConfig () with
                  InMemory = true }
            |> Database.createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

        let tx =
            db.TxMgr.NewTransaction false Serializable

        let v1 = "test_create_view_V1"
        db.CatalogMgr.DropView tx v1
        db.CatalogMgr.CreateView tx v1 "abcde"

        let v2 = "test_create_view_V2"
        db.CatalogMgr.DropView tx v2
        db.CatalogMgr.CreateView tx v2 "select * from T"

        let vdef1 = db.CatalogMgr.GetViewDef tx v1
        Assert.Equal("abcde", vdef1 |> Option.get)

        let vdef2 = db.CatalogMgr.GetViewDef tx v2
        Assert.Equal("select * from T", vdef2 |> Option.get)

        let vdef3 =
            db.CatalogMgr.GetViewDef tx "test_create_view_V3"

        Assert.True(Option.isNone vdef3)

        tx.Commit()
