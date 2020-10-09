module RyakDB.Test.Concurrency.TransactionConcurrencyTest

open Xunit
open FsUnit.Xunit
open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Transaction
open RyakDB.Execution.Plan
open RyakDB.Database
open RyakDB.Test.TestInit

let init filename max =
    Array.init max (fun i -> BlockId.newBlockId filename (int64 i))

[<Fact>]
let ``serializable, read modify`` () =
    let filename =
        "test_concurrencymanager_"
        + System.DateTime.Now.Ticks.ToString()

    let blocks = init filename 5

    use db =
        { Database.defaultConfig () with
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let tx1 =
        db.Transaction.NewTransaction false Serializable

    let tx2 =
        db.Transaction.NewTransaction false Serializable

    tx1.Concurrency.ReadFile filename
    tx1.Concurrency.ReadBlock blocks.[0]
    shouldFail (fun () -> tx2.Concurrency.ModifyBlock blocks.[0])
    |> ignore

    tx1.Rollback()
    tx2.Rollback()

[<Fact>]
let ``serializable, read read`` () =
    let filename =
        "test_concurrencymanager_"
        + System.DateTime.Now.Ticks.ToString()

    let blocks = init filename 5

    use db =
        { Database.defaultConfig () with
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let tx1 =
        db.Transaction.NewTransaction false Serializable

    let tx2 =
        db.Transaction.NewTransaction false Serializable

    tx1.Concurrency.ReadFile filename
    tx2.Concurrency.ReadBlock blocks.[0]
    tx1.Concurrency.ReadBlock blocks.[1]

    tx1.Rollback()
    tx2.Rollback()

[<Fact>]
let ``serializable, modify read`` () =
    let filename =
        "test_concurrencymanager_"
        + System.DateTime.Now.Ticks.ToString()

    let blocks = init filename 5

    use db =
        { Database.defaultConfig () with
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let tx1 =
        db.Transaction.NewTransaction false Serializable

    let tx2 =
        db.Transaction.NewTransaction false Serializable

    tx1.Concurrency.ModifyBlock blocks.[0]
    tx2.Concurrency.ReadBlock blocks.[1]

    tx1.Rollback()
    tx2.Rollback()

[<Fact>]
let ``serializable, phantom`` () =
    let filename =
        "test_concurrencymanager_"
        + System.DateTime.Now.Ticks.ToString()

    let blocks = init filename 5

    use db =
        { Database.defaultConfig () with
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let tx1 =
        db.Transaction.NewTransaction false Serializable

    let tx2 =
        db.Transaction.NewTransaction false Serializable

    tx1.Concurrency.ReadBlock blocks.[0]
    shouldFail (fun () -> tx2.Concurrency.InsertBlock blocks.[1])
    |> ignore

    tx1.Rollback()
    tx2.Rollback()

[<Fact>]
let ``repeatable read, read modify`` () =
    let filename =
        "test_concurrencymanager_"
        + System.DateTime.Now.Ticks.ToString()

    let blocks = init filename 5

    use db =
        { Database.defaultConfig () with
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let tx1 =
        db.Transaction.NewTransaction false RepeatableRead

    let tx2 =
        db.Transaction.NewTransaction false RepeatableRead

    tx1.Concurrency.ReadBlock blocks.[0]
    shouldFail (fun () -> tx2.Concurrency.ModifyBlock blocks.[0])
    |> ignore

    tx1.Rollback()
    tx2.Rollback()

[<Fact>]
let ``repeatable read, phantom`` () =
    let filename =
        "test_concurrencymanager_"
        + System.DateTime.Now.Ticks.ToString()

    let blocks = init filename 5

    use db =
        { Database.defaultConfig () with
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let tx1 =
        db.Transaction.NewTransaction false RepeatableRead

    let tx2 =
        db.Transaction.NewTransaction false RepeatableRead

    tx1.Concurrency.ReadFile filename
    tx1.Concurrency.ReadBlock blocks.[0]
    tx2.Concurrency.InsertBlock blocks.[1]
    tx2.Concurrency.ModifyBlock blocks.[1]

    tx1.Rollback()
    tx2.Rollback()

[<Fact>]
let ``read committed, end statement`` () =
    let filename =
        "test_concurrencymanager_"
        + System.DateTime.Now.Ticks.ToString()

    let blocks = init filename 5

    use db =
        { Database.defaultConfig () with
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let tx1 =
        db.Transaction.NewTransaction false ReadCommitted

    let tx2 =
        db.Transaction.NewTransaction false ReadCommitted

    tx1.Concurrency.ReadBlock blocks.[0]
    tx1.Concurrency.ReadBlock blocks.[1]
    tx1.EndStatement()
    tx2.Concurrency.ModifyBlock blocks.[2]

    tx1.Rollback()
    tx2.Rollback()

[<Fact>]
let ``serializable, serializable`` () =
    use db =
        { Database.defaultConfig () with
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    TestInit.setupStudentTable db

    let tx1 =
        db.Transaction.NewTransaction false Serializable

    let tx2 =
        db.Transaction.NewTransaction false Serializable

    db.Catalog.GetTableInfo tx1 "student"
    |> Option.get
    |> Plan.newTablePlan tx1
    |> Plan.openScan db.File
    |> (fun scan1 ->
        scan1.BeforeFirst()
        db.Catalog.GetTableInfo tx2 "student"
        |> Option.get
        |> Plan.newTablePlan tx2
        |> Plan.openScan db.File
        |> (fun scan2 ->
            scan2.BeforeFirst()
            scan1.Next() |> ignore
            scan2.Next() |> ignore
            shouldFail (fun () -> scan2.Insert()) |> ignore
            scan2.Close())
        scan1.Close())

    db.Catalog.GetTableInfo tx1 "student"
    |> Option.get
    |> Plan.newTablePlan tx1
    |> Plan.openScan db.File
    |> (fun scan1 ->
        shouldFail (fun () -> scan1.Insert()) |> ignore
        db.Catalog.GetTableInfo tx2 "student"
        |> Option.get
        |> Plan.newTablePlan tx2
        |> Plan.openScan db.File
        |> (fun scan2 ->
            shouldFail (fun () -> scan2.Insert()) |> ignore
            scan2.Close())
        scan1.Close())

    tx1.Rollback()
    tx2.Rollback()

[<Fact>]
let ``serializable, repeatable read`` () =
    use db =
        { Database.defaultConfig () with
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    TestInit.setupStudentTable db

    let tx1 =
        db.Transaction.NewTransaction false Serializable

    let tx2 =
        db.Transaction.NewTransaction false RepeatableRead

    db.Catalog.GetTableInfo tx2 "student"
    |> Option.get
    |> Plan.newTablePlan tx2
    |> Plan.openScan db.File
    |> (fun scan2 ->
        scan2.BeforeFirst()
        db.Catalog.GetTableInfo tx1 "student"
        |> Option.get
        |> Plan.newTablePlan tx1
        |> Plan.openScan db.File
        |> (fun scan1 ->
            scan1.BeforeFirst()
            scan2.Next() |> ignore
            scan1.Next() |> ignore
            scan1.Insert()
            scan1.Close())
        scan2.Close())

    db.Catalog.GetTableInfo tx2 "student"
    |> Option.get
    |> Plan.newTablePlan tx2
    |> Plan.openScan db.File
    |> (fun scan2 ->
        scan2.BeforeFirst()
        db.Catalog.GetTableInfo tx1 "student"
        |> Option.get
        |> Plan.newTablePlan tx1
        |> Plan.openScan db.File
        |> (fun scan1 ->
            shouldFail (fun () ->
                scan1.BeforeFirst()
                scan2.Next() |> ignore)
            |> ignore
            scan1.Next() |> ignore
            shouldFail (fun () -> scan2.Insert()) |> ignore
            scan1.Close())
        scan2.Close())

    tx1.Rollback()
    tx2.Rollback()

[<Fact>]
let ``serializable, read committed`` () =
    use db =
        { Database.defaultConfig () with
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    TestInit.setupStudentTable db

    let tx1 =
        db.Transaction.NewTransaction false Serializable

    let tx2 =
        db.Transaction.NewTransaction false ReadCommitted

    db.Catalog.GetTableInfo tx2 "student"
    |> Option.get
    |> Plan.newTablePlan tx2
    |> Plan.openScan db.File
    |> (fun scan2 ->
        scan2.BeforeFirst()
        db.Catalog.GetTableInfo tx1 "student"
        |> Option.get
        |> Plan.newTablePlan tx1
        |> Plan.openScan db.File
        |> (fun scan1 ->
            scan1.BeforeFirst()
            scan2.Next() |> ignore
            scan2.GetVal "s_id"
            |> should equal (IntDbConstant 1)
            tx2.EndStatement()
            scan1.Next() |> ignore
            scan1.Insert()
            scan1.Close())
        scan2.Close())

    tx1.Rollback()
    tx2.Rollback()
