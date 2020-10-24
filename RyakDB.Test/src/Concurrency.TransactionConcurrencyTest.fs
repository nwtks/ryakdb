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
              BlockSize = 1024
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
              BlockSize = 1024
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
              BlockSize = 1024
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
              BlockSize = 1024
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
              BlockSize = 1024
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
              BlockSize = 1024
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
              BlockSize = 1024
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
              BlockSize = 1024
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    TestInit.setupStudentTable db

    let tx1 =
        db.Transaction.NewTransaction false Serializable

    let tx2 =
        db.Transaction.NewTransaction false Serializable

    use scan1 =
        db.Catalog.GetTableInfo tx1 "student"
        |> Option.get
        |> Plan.newTablePlan db.File tx1
        |> Plan.openScan

    scan1.BeforeFirst()

    use scan2 =
        db.Catalog.GetTableInfo tx2 "student"
        |> Option.get
        |> Plan.newTablePlan db.File tx2
        |> Plan.openScan

    scan2.BeforeFirst()
    scan1.Next() |> ignore
    scan2.Next() |> ignore
    shouldFail (fun () -> scan2.Insert()) |> ignore

    use scan1 =
        db.Catalog.GetTableInfo tx1 "student"
        |> Option.get
        |> Plan.newTablePlan db.File tx1
        |> Plan.openScan

    shouldFail (fun () -> scan1.Insert()) |> ignore

    use scan2 =
        db.Catalog.GetTableInfo tx2 "student"
        |> Option.get
        |> Plan.newTablePlan db.File tx2
        |> Plan.openScan

    shouldFail (fun () -> scan2.Insert()) |> ignore

    tx1.Rollback()
    tx2.Rollback()

[<Fact>]
let ``serializable, repeatable read`` () =
    use db =
        { Database.defaultConfig () with
              BlockSize = 1024
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    TestInit.setupStudentTable db

    let tx1 =
        db.Transaction.NewTransaction false Serializable

    let tx2 =
        db.Transaction.NewTransaction false RepeatableRead

    use scan1 =
        db.Catalog.GetTableInfo tx2 "student"
        |> Option.get
        |> Plan.newTablePlan db.File tx2
        |> Plan.openScan

    scan1.BeforeFirst()

    use scan2 =
        db.Catalog.GetTableInfo tx1 "student"
        |> Option.get
        |> Plan.newTablePlan db.File tx1
        |> Plan.openScan

    scan2.BeforeFirst()
    scan1.Next() |> ignore
    scan2.Next() |> ignore
    scan2.Insert()

    use scan3 =
        db.Catalog.GetTableInfo tx2 "student"
        |> Option.get
        |> Plan.newTablePlan db.File tx2
        |> Plan.openScan

    scan3.BeforeFirst()

    use scan4 =
        db.Catalog.GetTableInfo tx1 "student"
        |> Option.get
        |> Plan.newTablePlan db.File tx1
        |> Plan.openScan

    shouldFail (fun () ->
        scan4.BeforeFirst()
        scan3.Next() |> ignore)
    |> ignore
    scan4.Next() |> ignore
    shouldFail (fun () -> scan3.Insert()) |> ignore

    tx1.Rollback()
    tx2.Rollback()

[<Fact>]
let ``serializable, read committed`` () =
    use db =
        { Database.defaultConfig () with
              BlockSize = 1024
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    TestInit.setupStudentTable db

    let tx1 =
        db.Transaction.NewTransaction false Serializable

    let tx2 =
        db.Transaction.NewTransaction false ReadCommitted

    use scan1 =
        db.Catalog.GetTableInfo tx2 "student"
        |> Option.get
        |> Plan.newTablePlan db.File tx2
        |> Plan.openScan

    scan1.BeforeFirst()

    use scan2 =
        db.Catalog.GetTableInfo tx1 "student"
        |> Option.get
        |> Plan.newTablePlan db.File tx1
        |> Plan.openScan

    scan2.BeforeFirst()
    scan1.Next() |> ignore
    scan1.GetVal "s_id"
    |> should equal (IntDbConstant 1)
    tx2.EndStatement()
    scan2.Next() |> ignore
    scan2.Insert()

    tx1.Rollback()
    tx2.Rollback()
