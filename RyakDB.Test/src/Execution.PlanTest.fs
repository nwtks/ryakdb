module RyakDB.Test.Execution.PlanTest

open Xunit
open FsUnit.Xunit
open RyakDB.DataType
open RyakDB.Query
open RyakDB.Query.Predicate
open RyakDB.Transaction
open RyakDB.Execution.Plan
open RyakDB.Execution.MergeSort
open RyakDB.Database
open RyakDB.Test.TestInit

[<Fact>]
let ``table plan`` () =
    use db =
        { Database.defaultConfig () with
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    TestInit.setupStudentTable db

    let tx =
        db.Transaction.NewTransaction false Serializable

    use scan =
        db.Catalog.GetTableInfo tx "student"
        |> Option.get
        |> Plan.newTablePlan db.File tx
        |> Plan.openScan

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

    tx.Commit()

[<Fact>]
let ``select plan`` () =
    use db =
        { Database.defaultConfig () with
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    TestInit.setupStudentTable db

    let tx =
        db.Transaction.NewTransaction false Serializable

    let pred =
        [ Term(EqualOperator, FieldNameExpression "major_id", IntDbConstant 20 |> ConstantExpression) ]
        |> Predicate

    use scan =
        db.Catalog.GetTableInfo tx "student"
        |> Option.get
        |> Plan.newTablePlan db.File tx
        |> Plan.pipeSelectPlan pred
        |> Plan.openScan

    let mutable i = 0
    scan.BeforeFirst()
    while scan.Next() do
        i <- i + 1
        scan.GetVal "major_id"
        |> should equal (IntDbConstant 20)
    i |> should equal 23

    tx.Commit()

[<Fact>]
let ``project plan`` () =
    use db =
        { Database.defaultConfig () with
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    TestInit.setupStudentTable db

    let tx =
        db.Transaction.NewTransaction false Serializable

    use scan =
        db.Catalog.GetTableInfo tx "student"
        |> Option.get
        |> Plan.newTablePlan db.File tx
        |> Plan.pipeProjectPlan [ "grad_year"
                                  "s_name" ]
        |> Plan.openScan

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

    tx.Commit()

[<Fact>]
let ``product plan`` () =
    use db =
        { Database.defaultConfig () with
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    TestInit.setupStudentTable db
    TestInit.setupDeptTable db

    let tx =
        db.Transaction.NewTransaction false Serializable

    let pred =
        [ Term(EqualOperator, FieldNameExpression "major_id", FieldNameExpression "d_id") ]
        |> Predicate

    use scan =
        [ db.Catalog.GetTableInfo tx "student"
          |> Option.get
          |> Plan.newTablePlan db.File tx
          db.Catalog.GetTableInfo tx "dept"
          |> Option.get
          |> Plan.newTablePlan db.File tx ]
        |> List.reduce Plan.pipeProductPlan
        |> Plan.pipeSelectPlan pred
        |> Plan.openScan

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

    tx.Commit()

[<Fact>]
let ``group by plan`` () =
    use db =
        { Database.defaultConfig () with
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    TestInit.setupStudentTable db

    let tx =
        db.Transaction.NewTransaction false Serializable

    let newSortScan =
        MergeSort.newSortScan db.File db.BufferPool tx

    use scan =
        db.Catalog.GetTableInfo tx "student"
        |> Option.get
        |> Plan.newTablePlan db.File tx
        |> Plan.pipeGroupByPlan
            newSortScan
               [ "grad_year" ]
               [ CountFn("grad_year")
                 MinFn("major_id")
                 MaxFn("s_name") ]
        |> Plan.openScan

    let mutable i = 0
    scan.BeforeFirst()
    while scan.Next() do
        i <- i + 1
        scan.GetVal "count_of_grad_year"
        |> should equal (IntDbConstant 18)
    i |> should equal 50

    tx.Commit()

[<Fact>]
let ``sort plan`` () =
    use db =
        { Database.defaultConfig () with
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    TestInit.setupStudentTable db

    let tx =
        db.Transaction.NewTransaction false Serializable

    let newSortScan =
        MergeSort.newSortScan db.File db.BufferPool tx

    use scan =
        db.Catalog.GetTableInfo tx "student"
        |> Option.get
        |> Plan.newTablePlan db.File tx
        |> Plan.pipeSortPlan
            newSortScan
               [ SortField("grad_year", SortAsc)
                 SortField("s_id", SortDesc) ]
        |> Plan.openScan

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

    tx.Commit()
