module RyakDB.Test.Execution.PlanTest

open Xunit
open RyakDB.DataType
open RyakDB.Query
open RyakDB.Query.Predicate
open RyakDB.Execution.Plan
open RyakDB.Transaction
open RyakDB.Execution.Materialize
open RyakDB.Database
open RyakDB.Test.TestInit

[<Fact>]
let ``table plan`` () =
    let db =
        { Database.defaultConfig () with
              InMemory = true }
        |> Database.createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

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
            Assert.Equal(IntSqlConstant i, scan.GetVal "s_id")
            Assert.Equal(SqlConstant.newVarchar ("student " + i.ToString()), scan.GetVal "s_name")
            Assert.Equal(IntSqlConstant(i % 50 + 1960), scan.GetVal "grad_year")
        Assert.Equal(900, i)
        scan.Close())

    tx.Commit()

[<Fact>]
let ``select plan`` () =
    let db =
        { Database.defaultConfig () with
              InMemory = true }
        |> Database.createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    TestInit.setupStudentTable db

    let tx =
        db.TxMgr.NewTransaction false Serializable

    let pred =
        [ Term(EqualOperator, FieldNameExpression "major_id", IntSqlConstant 20 |> ConstantExpression) ]
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
            Assert.Equal(IntSqlConstant 20, scan.GetVal "major_id")
        Assert.Equal(23, i)
        scan.Close())

    tx.Commit()

[<Fact>]
let ``project plan`` () =
    let db =
        { Database.defaultConfig () with
              InMemory = true }
        |> Database.createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

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
            Assert.Equal(SqlConstant.newVarchar ("student " + i.ToString()), scan.GetVal "s_name")
            Assert.Equal(IntSqlConstant(i % 50 + 1960), scan.GetVal "grad_year")
            Assert.False(scan.HasField "s_id")
            Assert.False(scan.HasField "major_id")
        Assert.Equal(900, i)
        scan.Close())

    tx.Commit()

[<Fact>]
let ``product plan`` () =
    let db =
        { Database.defaultConfig () with
              InMemory = true }
        |> Database.createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

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
            Assert.Equal(IntSqlConstant i, scan.GetVal "s_id")
            Assert.Equal(IntSqlConstant(i % 40 + 1), scan.GetVal "d_id")
            Assert.Equal(SqlConstant.newVarchar ("dept " + (i % 40 + 1).ToString()), scan.GetVal "d_name")
        Assert.Equal(900, i)
        scan.Close())

    tx.Commit()

[<Fact>]
let ``group by plan`` () =
    let db =
        { Database.defaultConfig () with
              InMemory = true }
        |> Database.createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    TestInit.setupStudentTable db

    let tx =
        db.TxMgr.NewTransaction false Serializable

    let newSortScan = Materialize.newSortScan db.FileMgr tx

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
            Assert.Equal(IntSqlConstant 18, scan.GetVal "count_of_grad_year")
        Assert.Equal(50, i)
        scan.Close())

    tx.Commit()

[<Fact>]
let ``sort plan`` () =
    let db =
        { Database.defaultConfig () with
              InMemory = true }
        |> Database.createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    TestInit.setupStudentTable db

    let tx =
        db.TxMgr.NewTransaction false Serializable

    let newSortScan = Materialize.newSortScan db.FileMgr tx

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
                scan.GetVal "grad_year" |> SqlConstant.toInt

            let sid = scan.GetVal "s_id" |> SqlConstant.toInt
            Assert.True(prevGradYear <= gradYear)
            if prevGradYear = gradYear then Assert.True(prevSid > sid)
            prevGradYear <- gradYear
            prevSid <- sid
        Assert.Equal(900, i)
        scan.Close())

    tx.Commit()
