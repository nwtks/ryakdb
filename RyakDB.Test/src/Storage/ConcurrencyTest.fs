namespace RyakDB.Storage.ConcurrencyTest

open Xunit
open RyakDB.Sql.Type
open RyakDB.Storage.Type
open RyakDB.Storage.File
open RyakDB.Storage.Concurrency
open RyakDB.Query.Algebra
open RyakDB.Server.Database
open RyakDB.TestInit

module LockTableTest =
    let init max =
        let filename =
            "test_locktable_"
            + System.DateTime.Now.Ticks.ToString()

        Array.init max (fun i -> BlockIdLockerKey(BlockId.newBlockId filename (int64 i)))

    [<Fact>]
    let ``S lock`` () =
        let lockTbl = LockTable.newLockTable 1000
        let blocks = init 100
        let txNo1 = 12341L
        let txNo2 = 12342L

        blocks
        |> Array.iter (fun block ->
            lockTbl.SLock txNo1 block
            lockTbl.SLock txNo1 block
            lockTbl.SLock txNo2 block
            lockTbl.SLock txNo1 block)

        lockTbl.ReleaseAll txNo1 false
        lockTbl.ReleaseAll txNo2 false

    [<Fact>]
    let ``X lock`` () =
        let lockTbl = LockTable.newLockTable 1000
        let blocks = init 5
        let txNo1 = 123451L
        let txNo2 = 123452L

        lockTbl.XLock txNo1 blocks.[0]
        lockTbl.XLock txNo1 blocks.[0]
        lockTbl.SLock txNo1 blocks.[0]
        lockTbl.SLock txNo1 blocks.[0]

        lockTbl.SLock txNo1 blocks.[1]
        lockTbl.SLock txNo1 blocks.[1]
        lockTbl.XLock txNo1 blocks.[1]
        lockTbl.XLock txNo1 blocks.[1]

        Assert.Throws(fun () -> lockTbl.SLock txNo2 blocks.[0])
        |> ignore
        Assert.Throws(fun () -> lockTbl.SLock txNo2 blocks.[1])
        |> ignore
        Assert.Throws(fun () -> lockTbl.XLock txNo2 blocks.[0])
        |> ignore
        Assert.Throws(fun () -> lockTbl.XLock txNo2 blocks.[1])
        |> ignore

        lockTbl.ReleaseAll txNo1 false

        lockTbl.XLock txNo2 blocks.[0]
        lockTbl.XLock txNo2 blocks.[1]

        lockTbl.ReleaseAll txNo2 false

    [<Fact>]
    let ``SIX lock`` () =
        let lockTbl = LockTable.newLockTable 1000
        let blocks = init 5
        let txNo1 = 123456781L
        let txNo2 = 123456782L

        lockTbl.SLock txNo1 blocks.[0]
        Assert.Throws(fun () -> lockTbl.SIXLock txNo2 blocks.[0])
        |> ignore

        lockTbl.SIXLock txNo1 blocks.[1]
        Assert.Throws(fun () -> lockTbl.SIXLock txNo2 blocks.[1])
        |> ignore

        lockTbl.ReleaseAll txNo1 false
        lockTbl.ReleaseAll txNo2 false

    [<Fact>]
    let ``IS lock`` () =
        let lockTbl = LockTable.newLockTable 1000
        let blocks = init 10
        let txNo1 = 1234561L
        let txNo2 = 1234562L

        blocks
        |> Array.iter (fun block -> lockTbl.ISLock txNo1 block)

        lockTbl.SLock txNo1 blocks.[0]
        lockTbl.ISLock txNo1 blocks.[1]
        lockTbl.XLock txNo1 blocks.[2]
        lockTbl.IXLock txNo1 blocks.[3]
        lockTbl.SIXLock txNo1 blocks.[4]

        lockTbl.SLock txNo2 blocks.[5]
        lockTbl.ISLock txNo2 blocks.[6]
        Assert.Throws(fun () -> lockTbl.XLock txNo2 blocks.[7])
        |> ignore
        lockTbl.IXLock txNo2 blocks.[8]
        lockTbl.SIXLock txNo2 blocks.[9]

        lockTbl.ReleaseAll txNo1 false
        lockTbl.ReleaseAll txNo2 false

    [<Fact>]
    let ``IX lock`` () =
        let lockTbl = LockTable.newLockTable 1000
        let blocks = init 5
        let txNo1 = 12345671L
        let txNo2 = 12345672L

        lockTbl.SLock txNo1 blocks.[0]
        Assert.Throws(fun () -> lockTbl.IXLock txNo2 blocks.[0])
        |> ignore

        lockTbl.IXLock txNo1 blocks.[1]
        lockTbl.IXLock txNo2 blocks.[1]

        lockTbl.IXLock txNo1 blocks.[2]
        Assert.Throws(fun () -> lockTbl.SLock txNo2 blocks.[2])
        |> ignore

        lockTbl.IXLock txNo1 blocks.[3]
        Assert.Throws(fun () -> lockTbl.XLock txNo2 blocks.[3])
        |> ignore

        lockTbl.ReleaseAll txNo1 false
        lockTbl.ReleaseAll txNo2 false

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
        |> Plan.openScan
        |> (fun scan1 ->
            scan1.BeforeFirst()
            db.CatalogMgr.GetTableInfo tx2 "student"
            |> Option.get
            |> Plan.newTablePlan tx2
            |> Plan.openScan
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
        |> Plan.openScan
        |> (fun scan1 ->
            Assert.Throws(fun () -> scan1.Insert()) |> ignore
            db.CatalogMgr.GetTableInfo tx2 "student"
            |> Option.get
            |> Plan.newTablePlan tx2
            |> Plan.openScan
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
        |> Plan.openScan
        |> (fun scan2 ->
            scan2.BeforeFirst()
            db.CatalogMgr.GetTableInfo tx1 "student"
            |> Option.get
            |> Plan.newTablePlan tx1
            |> Plan.openScan
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
        |> Plan.openScan
        |> (fun scan2 ->
            scan2.BeforeFirst()
            db.CatalogMgr.GetTableInfo tx1 "student"
            |> Option.get
            |> Plan.newTablePlan tx1
            |> Plan.openScan
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
        |> Plan.openScan
        |> (fun scan2 ->
            scan2.BeforeFirst()
            db.CatalogMgr.GetTableInfo tx1 "student"
            |> Option.get
            |> Plan.newTablePlan tx1
            |> Plan.openScan
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
