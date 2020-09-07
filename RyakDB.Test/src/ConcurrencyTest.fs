namespace RyakDB.Test.ConcurrencyTest

open Xunit
open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Concurrency
open RyakDB.Transaction
open RyakDB.Execution.Plan
open RyakDB.Database
open RyakDB.Test.TestInit

module ConcurrencyManagerTest =
    let init filename max =
        Array.init max (fun i -> BlockId.newBlockId filename (int64 i))

    [<Fact>]
    let ``serializable, read modify`` () =
        let filename =
            "test_concurrencymanager_"
            + System.DateTime.Now.Ticks.ToString()

        let blocks = init filename 5

        let db =
            { Database.defaultConfig () with
                  InMemory = true }
            |> Database.createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

        let tx1 =
            db.TxMgr.NewTransaction false Serializable

        let tx2 =
            db.TxMgr.NewTransaction false Serializable

        tx1.ConcurMgr.ReadFile filename
        tx1.ConcurMgr.ReadBlock blocks.[0]
        Assert.Throws(fun () -> tx2.ConcurMgr.ModifyBlock blocks.[0])
        |> ignore

        tx1.Rollback()
        tx2.Rollback()

    [<Fact>]
    let ``serializable, read read`` () =
        let filename =
            "test_concurrencymanager_"
            + System.DateTime.Now.Ticks.ToString()

        let blocks = init filename 5

        let db =
            { Database.defaultConfig () with
                  InMemory = true }
            |> Database.createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

        let tx1 =
            db.TxMgr.NewTransaction false Serializable

        let tx2 =
            db.TxMgr.NewTransaction false Serializable

        tx1.ConcurMgr.ReadFile filename
        tx2.ConcurMgr.ReadBlock blocks.[0]
        tx1.ConcurMgr.ReadBlock blocks.[1]

        tx1.Rollback()
        tx2.Rollback()

    [<Fact>]
    let ``serializable, modify read`` () =
        let filename =
            "test_concurrencymanager_"
            + System.DateTime.Now.Ticks.ToString()

        let blocks = init filename 5

        let db =
            { Database.defaultConfig () with
                  InMemory = true }
            |> Database.createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

        let tx1 =
            db.TxMgr.NewTransaction false Serializable

        let tx2 =
            db.TxMgr.NewTransaction false Serializable

        tx1.ConcurMgr.ModifyBlock blocks.[0]
        tx2.ConcurMgr.ReadBlock blocks.[1]

        tx1.Rollback()
        tx2.Rollback()

    [<Fact>]
    let ``serializable, phantom`` () =
        let filename =
            "test_concurrencymanager_"
            + System.DateTime.Now.Ticks.ToString()

        let blocks = init filename 5

        let db =
            { Database.defaultConfig () with
                  InMemory = true }
            |> Database.createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

        let tx1 =
            db.TxMgr.NewTransaction false Serializable

        let tx2 =
            db.TxMgr.NewTransaction false Serializable

        tx1.ConcurMgr.ReadBlock blocks.[0]
        Assert.Throws(fun () -> tx2.ConcurMgr.InsertBlock blocks.[1])
        |> ignore

        tx1.Rollback()
        tx2.Rollback()

    [<Fact>]
    let ``repeatable read, read modify`` () =
        let filename =
            "test_concurrencymanager_"
            + System.DateTime.Now.Ticks.ToString()

        let blocks = init filename 5

        let db =
            { Database.defaultConfig () with
                  InMemory = true }
            |> Database.createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

        let tx1 =
            db.TxMgr.NewTransaction false RepeatableRead

        let tx2 =
            db.TxMgr.NewTransaction false RepeatableRead

        tx1.ConcurMgr.ReadBlock blocks.[0]
        Assert.Throws(fun () -> tx2.ConcurMgr.ModifyBlock blocks.[0])
        |> ignore

        tx1.Rollback()
        tx2.Rollback()

    [<Fact>]
    let ``repeatable read, phantom`` () =
        let filename =
            "test_concurrencymanager_"
            + System.DateTime.Now.Ticks.ToString()

        let blocks = init filename 5

        let db =
            { Database.defaultConfig () with
                  InMemory = true }
            |> Database.createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

        let tx1 =
            db.TxMgr.NewTransaction false RepeatableRead

        let tx2 =
            db.TxMgr.NewTransaction false RepeatableRead

        tx1.ConcurMgr.ReadFile filename
        tx1.ConcurMgr.ReadBlock blocks.[0]
        tx2.ConcurMgr.InsertBlock blocks.[1]
        tx2.ConcurMgr.ModifyBlock blocks.[1]

        tx1.Rollback()
        tx2.Rollback()

    [<Fact>]
    let ``read committed, end statement`` () =
        let filename =
            "test_concurrencymanager_"
            + System.DateTime.Now.Ticks.ToString()

        let blocks = init filename 5

        let db =
            { Database.defaultConfig () with
                  InMemory = true }
            |> Database.createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

        let tx1 =
            db.TxMgr.NewTransaction false ReadCommitted

        let tx2 =
            db.TxMgr.NewTransaction false ReadCommitted

        tx1.ConcurMgr.ReadBlock blocks.[0]
        tx1.ConcurMgr.ReadBlock blocks.[1]
        tx1.EndStatement()
        tx2.ConcurMgr.ModifyBlock blocks.[2]

        tx1.Rollback()
        tx2.Rollback()

    [<Fact>]
    let ``serializable, serializable`` () =
        let db =
            { Database.defaultConfig () with
                  InMemory = true }
            |> Database.createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

        TestInit.setupStudentTable db

        let tx1 =
            db.TxMgr.NewTransaction false Serializable

        let tx2 =
            db.TxMgr.NewTransaction false Serializable

        db.CatalogMgr.GetTableInfo tx1 "student"
        |> Option.get
        |> Plan.newTablePlan tx1
        |> Plan.openScan db.FileMgr
        |> (fun scan1 ->
            scan1.BeforeFirst()
            db.CatalogMgr.GetTableInfo tx2 "student"
            |> Option.get
            |> Plan.newTablePlan tx2
            |> Plan.openScan db.FileMgr
            |> (fun scan2 ->
                scan2.BeforeFirst()
                scan1.Next() |> ignore
                scan2.Next() |> ignore
                Assert.Throws(fun () -> scan2.Insert()) |> ignore
                scan2.Close())
            scan1.Close())

        db.CatalogMgr.GetTableInfo tx1 "student"
        |> Option.get
        |> Plan.newTablePlan tx1
        |> Plan.openScan db.FileMgr
        |> (fun scan1 ->
            Assert.Throws(fun () -> scan1.Insert()) |> ignore
            db.CatalogMgr.GetTableInfo tx2 "student"
            |> Option.get
            |> Plan.newTablePlan tx2
            |> Plan.openScan db.FileMgr
            |> (fun scan2 ->
                Assert.Throws(fun () -> scan2.Insert()) |> ignore
                scan2.Close())
            scan1.Close())

        tx1.Rollback()
        tx2.Rollback()

    [<Fact>]
    let ``serializable, repeatable read`` () =
        let db =
            { Database.defaultConfig () with
                  InMemory = true }
            |> Database.createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

        TestInit.setupStudentTable db

        let tx1 =
            db.TxMgr.NewTransaction false Serializable

        let tx2 =
            db.TxMgr.NewTransaction false RepeatableRead

        db.CatalogMgr.GetTableInfo tx2 "student"
        |> Option.get
        |> Plan.newTablePlan tx2
        |> Plan.openScan db.FileMgr
        |> (fun scan2 ->
            scan2.BeforeFirst()
            db.CatalogMgr.GetTableInfo tx1 "student"
            |> Option.get
            |> Plan.newTablePlan tx1
            |> Plan.openScan db.FileMgr
            |> (fun scan1 ->
                scan1.BeforeFirst()
                scan2.Next() |> ignore
                scan1.Next() |> ignore
                scan1.Insert()
                scan1.Close())
            scan2.Close())

        db.CatalogMgr.GetTableInfo tx2 "student"
        |> Option.get
        |> Plan.newTablePlan tx2
        |> Plan.openScan db.FileMgr
        |> (fun scan2 ->
            scan2.BeforeFirst()
            db.CatalogMgr.GetTableInfo tx1 "student"
            |> Option.get
            |> Plan.newTablePlan tx1
            |> Plan.openScan db.FileMgr
            |> (fun scan1 ->
                Assert.Throws(fun () ->
                    scan1.BeforeFirst()
                    scan2.Next() |> ignore)
                |> ignore
                scan1.Next() |> ignore
                Assert.Throws(fun () -> scan2.Insert()) |> ignore
                scan1.Close())
            scan2.Close())

        tx1.Rollback()
        tx2.Rollback()

    [<Fact>]
    let ``serializable, read committed`` () =
        let db =
            { Database.defaultConfig () with
                  InMemory = true }
            |> Database.createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

        TestInit.setupStudentTable db

        let tx1 =
            db.TxMgr.NewTransaction false Serializable

        let tx2 =
            db.TxMgr.NewTransaction false ReadCommitted

        db.CatalogMgr.GetTableInfo tx2 "student"
        |> Option.get
        |> Plan.newTablePlan tx2
        |> Plan.openScan db.FileMgr
        |> (fun scan2 ->
            scan2.BeforeFirst()
            db.CatalogMgr.GetTableInfo tx1 "student"
            |> Option.get
            |> Plan.newTablePlan tx1
            |> Plan.openScan db.FileMgr
            |> (fun scan1 ->
                scan1.BeforeFirst()
                scan2.Next() |> ignore
                Assert.Equal(IntSqlConstant 1, scan2.GetVal "s_id")
                tx2.EndStatement()
                scan1.Next() |> ignore
                scan1.Insert()
                scan1.Close())
            scan2.Close())

        tx1.Rollback()
        tx2.Rollback()
