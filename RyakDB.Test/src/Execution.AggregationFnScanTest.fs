module RyakDB.Test.Execution.AggregationFnScanTest

open Xunit
open FsUnit.Xunit
open RyakDB.DataType
open RyakDB.Query
open RyakDB.Transaction
open RyakDB.Execution.Plan
open RyakDB.Execution.MergeSort
open RyakDB.Database
open RyakDB.Test.TestInit

[<Fact>]
let count () =
    use db =
        { Database.defaultConfig () with
              BlockSize = 1024
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    setupStudentTable db

    let tx =
        db.Transaction.NewTransaction true Serializable

    let newSortScan =
        MergeSort.newSortScanFactory db.File db.BufferPool tx

    use scan =
        db.Catalog.GetTableInfo tx "student"
        |> Option.get
        |> Plan.newTablePlan db.File tx
        |> Plan.pipeGroupByPlan newSortScan [] [ CountFn("grad_year") ]
        |> Plan.openScan

    let mutable i = 0
    scan.BeforeFirst()
    while scan.Next() do
        i <- i + 1
        scan.GetVal "count_of_grad_year"
        |> should equal (IntDbConstant 900)
    i |> should equal 1

    tx.Commit()

[<Fact>]
let max () =
    use db =
        { Database.defaultConfig () with
              BlockSize = 1024
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    setupStudentTable db

    let tx =
        db.Transaction.NewTransaction true Serializable

    let newSortScan =
        MergeSort.newSortScanFactory db.File db.BufferPool tx

    use scan =
        db.Catalog.GetTableInfo tx "student"
        |> Option.get
        |> Plan.newTablePlan db.File tx
        |> Plan.pipeGroupByPlan newSortScan [] [ MaxFn("grad_year") ]
        |> Plan.openScan

    let mutable i = 0
    scan.BeforeFirst()
    while scan.Next() do
        i <- i + 1
        scan.GetVal "max_of_grad_year"
        |> should equal (IntDbConstant 2009)
    i |> should equal 1

    tx.Commit()

[<Fact>]
let min () =
    use db =
        { Database.defaultConfig () with
              BlockSize = 1024
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    setupStudentTable db

    let tx =
        db.Transaction.NewTransaction true Serializable

    let newSortScan =
        MergeSort.newSortScanFactory db.File db.BufferPool tx

    use scan =
        db.Catalog.GetTableInfo tx "student"
        |> Option.get
        |> Plan.newTablePlan db.File tx
        |> Plan.pipeGroupByPlan newSortScan [] [ MinFn("s_name") ]
        |> Plan.openScan

    let mutable i = 0
    scan.BeforeFirst()
    while scan.Next() do
        i <- i + 1
        scan.GetVal "min_of_s_name"
        |> should equal (DbConstant.newVarchar "student 1")
    i |> should equal 1

    tx.Commit()

[<Fact>]
let sum () =
    use db =
        { Database.defaultConfig () with
              BlockSize = 1024
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    setupStudentTable db

    let tx =
        db.Transaction.NewTransaction true Serializable

    let newSortScan =
        MergeSort.newSortScanFactory db.File db.BufferPool tx

    use scan =
        db.Catalog.GetTableInfo tx "student"
        |> Option.get
        |> Plan.newTablePlan db.File tx
        |> Plan.pipeGroupByPlan newSortScan [] [ SumFn("s_id") ]
        |> Plan.openScan

    let mutable i = 0
    scan.BeforeFirst()
    while scan.Next() do
        i <- i + 1
        scan.GetVal "sum_of_s_id"
        |> should equal (IntDbConstant 405450)
    i |> should equal 1

    tx.Commit()

[<Fact>]
let avg () =
    use db =
        { Database.defaultConfig () with
              BlockSize = 1024
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    setupStudentTable db

    let tx =
        db.Transaction.NewTransaction true Serializable

    let newSortScan =
        MergeSort.newSortScanFactory db.File db.BufferPool tx

    use scan =
        db.Catalog.GetTableInfo tx "student"
        |> Option.get
        |> Plan.newTablePlan db.File tx
        |> Plan.pipeGroupByPlan newSortScan [] [ AvgFn("major_id") ]
        |> Plan.openScan

    let mutable i = 0
    scan.BeforeFirst()
    while scan.Next() do
        i <- i + 1
        scan.GetVal "avg_of_major_id"
        |> should equal (DoubleDbConstant 20.3)
    i |> should equal 1

    tx.Commit()
