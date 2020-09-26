module RyakDB.Test.Execution.PlanTest

open Xunit
open FsUnit.Xunit
open RyakDB.DataType
open RyakDB.Query
open RyakDB.Query.Predicate
open RyakDB.Execution.Plan
open RyakDB.Transaction
open RyakDB.Execution.MergeSort
open RyakDB.Database
open RyakDB.Test.TestInit

[<Fact>]
let ``table plan`` () =
    let db =
        { Database.defaultConfig () with
              InMemory = true }
        |> createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    TestInit.setupStudentTable db

    let tx =
        db.TxMgr.NewTransaction false Serializable

    db.CatalogMgr.GetTableInfo tx "student"
    |> Option.get
    |> Plan.newTablePlan tx
    |> Plan.openScan db.FileMgr
    |> (fun scan ->
        let mutable i = 0
        scan.BeforeFirst()
        while scan.Next() do
            i <- i + 1
            scan.GetVal "s_id"
            |> should equal (IntDbConstant i)
            scan.GetVal "s_name"
            |> should equal (DbConstant.newVarchar ("student " + i.ToString()))
            scan.GetVal "grad_year"
            |> should equal (IntDbConstant(i % 50 + 1960))
        i |> should equal 900
        scan.Close())

    tx.Commit()

[<Fact>]
let ``select plan`` () =
    let db =
        { Database.defaultConfig () with
              InMemory = true }
        |> createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    TestInit.setupStudentTable db

    let tx =
        db.TxMgr.NewTransaction false Serializable

    let pred =
        [ Term(EqualOperator, FieldNameExpression "major_id", IntDbConstant 20 |> ConstantExpression) ]
        |> Predicate

    db.CatalogMgr.GetTableInfo tx "student"
    |> Option.get
    |> Plan.newTablePlan tx
    |> Plan.newSelectPlan pred
    |> Plan.openScan db.FileMgr
    |> (fun scan ->
        let mutable i = 0
        scan.BeforeFirst()
        while scan.Next() do
            i <- i + 1
            scan.GetVal "major_id"
            |> should equal (IntDbConstant 20)
        i |> should equal 23
        scan.Close())

    tx.Commit()

[<Fact>]
let ``project plan`` () =
    let db =
        { Database.defaultConfig () with
              InMemory = true }
        |> createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    TestInit.setupStudentTable db

    let tx =
        db.TxMgr.NewTransaction false Serializable

    db.CatalogMgr.GetTableInfo tx "student"
    |> Option.get
    |> Plan.newTablePlan tx
    |> Plan.newProjectPlan [ "grad_year"
                             "s_name" ]
    |> Plan.openScan db.FileMgr
    |> (fun scan ->
        let mutable i = 0
        scan.BeforeFirst()
        while scan.Next() do
            i <- i + 1
            scan.GetVal "s_name"
            |> should equal (DbConstant.newVarchar ("student " + i.ToString()))
            scan.GetVal "grad_year"
            |> should equal (IntDbConstant(i % 50 + 1960))
            scan.HasField "s_id" |> should be False
            scan.HasField "major_id" |> should be False
        i |> should equal 900
        scan.Close())

    tx.Commit()

[<Fact>]
let ``product plan`` () =
    let db =
        { Database.defaultConfig () with
              InMemory = true }
        |> createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    TestInit.setupStudentTable db
    TestInit.setupDeptTable db

    let tx =
        db.TxMgr.NewTransaction false Serializable

    let pred =
        [ Term(EqualOperator, FieldNameExpression "major_id", FieldNameExpression "d_id") ]
        |> Predicate

    [ db.CatalogMgr.GetTableInfo tx "student"
      |> Option.get
      |> Plan.newTablePlan tx
      db.CatalogMgr.GetTableInfo tx "dept"
      |> Option.get
      |> Plan.newTablePlan tx ]
    |> List.reduce Plan.newProductPlan
    |> Plan.newSelectPlan pred
    |> Plan.openScan db.FileMgr
    |> (fun scan ->
        let mutable i = 0
        scan.BeforeFirst()
        while scan.Next() do
            i <- i + 1
            scan.GetVal "s_id"
            |> should equal (IntDbConstant i)
            scan.GetVal "d_id"
            |> should equal (IntDbConstant(i % 40 + 1))
            scan.GetVal "d_name"
            |> should equal (DbConstant.newVarchar ("dept " + (i % 40 + 1).ToString()))
        i |> should equal 900
        scan.Close())

    tx.Commit()

[<Fact>]
let ``group by plan`` () =
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
    |> Plan.newGroupByPlan
        newSortScan
           [ "grad_year" ]
           [ CountFn("grad_year")
             MinFn("major_id")
             MaxFn("s_name") ]
    |> Plan.openScan db.FileMgr
    |> (fun scan ->
        let mutable i = 0
        scan.BeforeFirst()
        while scan.Next() do
            i <- i + 1
            scan.GetVal "count_of_grad_year"
            |> should equal (IntDbConstant 18)
        i |> should equal 50
        scan.Close())

    tx.Commit()

[<Fact>]
let ``sort plan`` () =
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
    |> Plan.newSortPlan
        newSortScan
           [ SortField("grad_year", SortAsc)
             SortField("s_id", SortDesc) ]
    |> Plan.openScan db.FileMgr
    |> (fun scan ->
        let mutable i = 0
        let mutable prevGradYear = 0
        let mutable prevSid = 0
        scan.BeforeFirst()
        while scan.Next() do
            i <- i + 1

            let gradYear =
                scan.GetVal "grad_year" |> DbConstant.toInt

            let sid = scan.GetVal "s_id" |> DbConstant.toInt
            prevGradYear
            |> should be (lessThanOrEqualTo gradYear)
            if prevGradYear = gradYear then prevSid |> should be (greaterThan sid)
            prevGradYear <- gradYear
            prevSid <- sid
        i |> should equal 900
        scan.Close())

    tx.Commit()
