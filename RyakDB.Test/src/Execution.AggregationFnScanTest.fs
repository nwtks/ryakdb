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
    let db =
        { Database.defaultConfig () with
              InMemory = true }
        |> createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    TestInit.setupStudentTable db

    let tx =
        db.TxMgr.NewTransaction false Serializable

    let newSortScan = MergeSort.newSortScan db.FileMgr tx

    db.CatalogMgr.GetTableInfo tx "student"
    |> Option.get
    |> Plan.newTablePlan tx
    |> Plan.newGroupByPlan newSortScan [] [ CountFn("grad_year") ]
    |> Plan.openScan db.FileMgr
    |> (fun scan ->
        let mutable i = 0
        scan.BeforeFirst()
        while scan.Next() do
            i <- i + 1
            scan.GetVal "count_of_grad_year"
            |> should equal (IntDbConstant 900)
        i |> should equal 1
        scan.Close())

    tx.Commit()

[<Fact>]
let max () =
    let db =
        { Database.defaultConfig () with
              InMemory = true }
        |> createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    TestInit.setupStudentTable db

    let tx =
        db.TxMgr.NewTransaction false Serializable

    let newSortScan = MergeSort.newSortScan db.FileMgr tx

    db.CatalogMgr.GetTableInfo tx "student"
    |> Option.get
    |> Plan.newTablePlan tx
    |> Plan.newGroupByPlan newSortScan [] [ MaxFn("grad_year") ]
    |> Plan.openScan db.FileMgr
    |> (fun scan ->
        let mutable i = 0
        scan.BeforeFirst()
        while scan.Next() do
            i <- i + 1
            scan.GetVal "max_of_grad_year"
            |> should equal (IntDbConstant 2009)
        i |> should equal 1
        scan.Close())

    tx.Commit()

[<Fact>]
let min () =
    let db =
        { Database.defaultConfig () with
              InMemory = true }
        |> createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    TestInit.setupStudentTable db

    let tx =
        db.TxMgr.NewTransaction false Serializable

    let newSortScan = MergeSort.newSortScan db.FileMgr tx

    db.CatalogMgr.GetTableInfo tx "student"
    |> Option.get
    |> Plan.newTablePlan tx
    |> Plan.newGroupByPlan newSortScan [] [ MinFn("s_name") ]
    |> Plan.openScan db.FileMgr
    |> (fun scan ->
        let mutable i = 0
        scan.BeforeFirst()
        while scan.Next() do
            i <- i + 1
            scan.GetVal "min_of_s_name"
            |> should equal (DbConstant.newVarchar "student 1")
        i |> should equal 1
        scan.Close())

    tx.Commit()

[<Fact>]
let sum () =
    let db =
        { Database.defaultConfig () with
              InMemory = true }
        |> createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    TestInit.setupStudentTable db

    let tx =
        db.TxMgr.NewTransaction false Serializable

    let newSortScan = MergeSort.newSortScan db.FileMgr tx

    db.CatalogMgr.GetTableInfo tx "student"
    |> Option.get
    |> Plan.newTablePlan tx
    |> Plan.newGroupByPlan newSortScan [] [ SumFn("s_id") ]
    |> Plan.openScan db.FileMgr
    |> (fun scan ->
        let mutable i = 0
        scan.BeforeFirst()
        while scan.Next() do
            i <- i + 1
            scan.GetVal "sum_of_s_id"
            |> should equal (IntDbConstant 405450)
        i |> should equal 1
        scan.Close())

    tx.Commit()

[<Fact>]
let avg () =
    let db =
        { Database.defaultConfig () with
              InMemory = true }
        |> createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    TestInit.setupStudentTable db

    let tx =
        db.TxMgr.NewTransaction false Serializable

    let newSortScan = MergeSort.newSortScan db.FileMgr tx

    db.CatalogMgr.GetTableInfo tx "student"
    |> Option.get
    |> Plan.newTablePlan tx
    |> Plan.newGroupByPlan newSortScan [] [ AvgFn("major_id") ]
    |> Plan.openScan db.FileMgr
    |> (fun scan ->
        let mutable i = 0
        scan.BeforeFirst()
        while scan.Next() do
            i <- i + 1
            scan.GetVal "avg_of_major_id"
            |> should equal (DoubleDbConstant 20.3)
        i |> should equal 1
        scan.Close())

    tx.Commit()
