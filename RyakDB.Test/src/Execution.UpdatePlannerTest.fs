module RyakDB.Test.Execution.UpdatePlannerTest

open Xunit
open FsUnit.Xunit
open RyakDB.DataType
open RyakDB.Table
open RyakDB.Index
open RyakDB.Transaction
open RyakDB.Execution.Plan
open RyakDB.Database

let createTable db =
    let tx =
        db.Transaction.NewTransaction false Serializable

    db.Planner.ExecuteUpdate tx "CREATE TABLE updatetest (tid INT, tname VARCHAR(100), tdate BIGINT)"
    |> ignore
    tx.Commit()

let createIndex db =
    let tx =
        db.Transaction.NewTransaction false Serializable

    db.Planner.ExecuteUpdate tx "CREATE INDEX updatetest_I1 ON updatetest (tdate) USING BTREE"
    |> ignore
    db.Planner.ExecuteUpdate tx "CREATE INDEX updatetest_I2 ON updatetest (tname) USING BTREE"
    |> ignore
    db.Planner.ExecuteUpdate tx "CREATE INDEX updatetest_I3 ON updatetest (tid) USING BTREE"
    |> ignore
    tx.Commit()

[<Fact>]
let insert () =
    use db =
        { Database.defaultConfig () with
              BlockSize = 1024
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    createTable db
    createIndex db

    let tx =
        db.Transaction.NewTransaction false Serializable

    db.Planner.ExecuteUpdate tx "INSERT INTO updatetest(tid, tname, tdate) VALUES (1, 'basketry', 9890033330000)"
    |> should equal 1

    use scan =
        db.Planner.CreateQueryPlan tx "select tid, tname, tdate from updatetest where tid = 1"
        |> Plan.openScan

    let mutable i = 0
    scan.BeforeFirst()
    while scan.Next() do
        i <- i + 1
        scan.GetVal "tid"
        |> should equal (IntDbConstant 1)
        scan.GetVal "tname"
        |> should equal (DbConstant.newVarchar "basketry")
        scan.GetVal "tdate"
        |> should equal (BigIntDbConstant 9890033330000L)
    i |> should equal 1

    db.Catalog.DropTable tx "updatetest"
    tx.Commit()

[<Fact>]
let delete () =
    use db =
        { Database.defaultConfig () with
              BlockSize = 1024
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    createTable db
    createIndex db

    let tx =
        db.Transaction.NewTransaction false Serializable

    [ 1 .. 5 ]
    |> List.iter (fun tid ->
        db.Planner.ExecuteUpdate
            tx
            ("insert into updatetest(tid,tname,tdate) values("
             + tid.ToString()
             + ",'test"
             + tid.ToString()
             + "',1000000000000"
             + tid.ToString()
             + ")")
        |> ignore)

    db.Planner.ExecuteUpdate tx "delete from updatetest where tid > 1"
    |> should equal 4

    use scan =
        db.Planner.CreateQueryPlan tx "select tid,tname,tdate from updatetest"
        |> Plan.openScan

    let mutable i = 0
    scan.BeforeFirst()
    while scan.Next() do
        i <- i + 1
        scan.GetVal "tid"
        |> should equal (IntDbConstant 1)
        scan.GetVal "tname"
        |> should equal (DbConstant.newVarchar "test1")
        scan.GetVal "tdate"
        |> should equal (BigIntDbConstant 10000000000001L)
    i |> should equal 1

    db.Catalog.DropTable tx "updatetest"
    tx.Commit()

[<Fact>]
let modify () =
    use db =
        { Database.defaultConfig () with
              BlockSize = 1024
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    createTable db
    createIndex db

    let tx =
        db.Transaction.NewTransaction false Serializable

    [ 1 .. 5 ]
    |> List.iter (fun tid ->
        db.Planner.ExecuteUpdate
            tx
            ("insert into updatetest(tid,tname,tdate) values("
             + tid.ToString()
             + ",'test"
             + tid.ToString()
             + "',1000000000000"
             + tid.ToString()
             + ")")
        |> ignore)

    db.Planner.ExecuteUpdate tx "update updatetest set tname = 'kkk', tdate=99999 where tid < 5"
    |> should equal 4

    use scan =
        db.Planner.CreateQueryPlan tx "select tid,tname,tdate from updatetest"
        |> Plan.openScan

    let mutable i = 0
    scan.BeforeFirst()
    while scan.Next() do
        i <- i + 1
        scan.GetVal "tid"
        |> should equal (IntDbConstant i)
        if i = 5 then
            scan.GetVal "tname"
            |> should equal (DbConstant.newVarchar "test5")
            scan.GetVal "tdate"
            |> should equal (BigIntDbConstant 10000000000005L)
        else
            scan.GetVal "tname"
            |> should equal (DbConstant.newVarchar "kkk")
            scan.GetVal "tdate"
            |> should equal (BigIntDbConstant 99999L)
    i |> should equal 5

    db.Catalog.DropTable tx "updatetest"
    tx.Commit()

[<Fact>]
let ``modify predicate`` () =
    use db =
        { Database.defaultConfig () with
              BlockSize = 1024
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    createTable db
    createIndex db

    let tx =
        db.Transaction.NewTransaction false Serializable

    [ 1 .. 5 ]
    |> List.iter (fun tid ->
        db.Planner.ExecuteUpdate
            tx
            ("insert into updatetest(tid,tname,tdate) values("
             + tid.ToString()
             + ",'test"
             + tid.ToString()
             + "',1000000000000"
             + tid.ToString()
             + ")")
        |> ignore)

    db.Planner.ExecuteUpdate tx "update updatetest set tid=111,tname='kkk',tdate=99999 where 2<=tid and tid<=4"
    |> should equal 3

    use scan =
        db.Planner.CreateQueryPlan tx "select tid,tname,tdate from updatetest where tid=111"
        |> Plan.openScan

    let mutable i = 0
    scan.BeforeFirst()
    while scan.Next() do
        i <- i + 1
        scan.GetVal "tid"
        |> should equal (IntDbConstant 111)
        scan.GetVal "tname"
        |> should equal (DbConstant.newVarchar "kkk")
        scan.GetVal "tdate"
        |> should equal (BigIntDbConstant 99999L)
    i |> should equal 3

    db.Catalog.DropTable tx "updatetest"
    tx.Commit()
