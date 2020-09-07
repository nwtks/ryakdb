namespace RyakDB.Test.Execution.AggregationFnScanTest

open Xunit
open RyakDB.DataType
open RyakDB.Query
open RyakDB.Transaction
open RyakDB.Execution.Plan
open RyakDB.Execution.Materialize
open RyakDB.Database
open RyakDB.Test.TestInit

module AggregationFnScanTest =
    [<Fact>]
    let count () =
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
        |> Plan.newGroupByPlan newSortScan [] [ CountFn("grad_year") ]
        |> Plan.openScan db.FileMgr
        |> (fun scan ->
            let mutable i = 0
            scan.BeforeFirst()
            while scan.Next() do
                i <- i + 1
                Assert.Equal(IntSqlConstant 900, scan.GetVal "count_of_grad_year")
            Assert.Equal(1, i)
            scan.Close())

        tx.Commit()

    [<Fact>]
    let max () =
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
        |> Plan.newGroupByPlan newSortScan [] [ MaxFn("grad_year") ]
        |> Plan.openScan db.FileMgr
        |> (fun scan ->
            let mutable i = 0
            scan.BeforeFirst()
            while scan.Next() do
                i <- i + 1
                Assert.Equal(IntSqlConstant 2009, scan.GetVal "max_of_grad_year")
            Assert.Equal(1, i)
            scan.Close())

        tx.Commit()

    [<Fact>]
    let min () =
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
        |> Plan.newGroupByPlan newSortScan [] [ MinFn("s_name") ]
        |> Plan.openScan db.FileMgr
        |> (fun scan ->
            let mutable i = 0
            scan.BeforeFirst()
            while scan.Next() do
                i <- i + 1
                Assert.Equal(SqlConstant.newVarchar "student 1", scan.GetVal "min_of_s_name")
            Assert.Equal(1, i)
            scan.Close())

        tx.Commit()

    [<Fact>]
    let sum () =
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
        |> Plan.newGroupByPlan newSortScan [] [ SumFn("s_id") ]
        |> Plan.openScan db.FileMgr
        |> (fun scan ->
            let mutable i = 0
            scan.BeforeFirst()
            while scan.Next() do
                i <- i + 1
                Assert.Equal(IntSqlConstant 405450, scan.GetVal "sum_of_s_id")
            Assert.Equal(1, i)
            scan.Close())

        tx.Commit()

    [<Fact>]
    let avg () =
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
        |> Plan.newGroupByPlan newSortScan [] [ AvgFn("major_id") ]
        |> Plan.openScan db.FileMgr
        |> (fun scan ->
            let mutable i = 0
            scan.BeforeFirst()
            while scan.Next() do
                i <- i + 1
                Assert.Equal(DoubleSqlConstant 20.3, scan.GetVal "avg_of_major_id")
            Assert.Equal(1, i)
            scan.Close())

        tx.Commit()
