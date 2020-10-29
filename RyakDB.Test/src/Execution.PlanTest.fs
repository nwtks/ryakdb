module RyakDB.Test.Execution.PlanTest

open Xunit
open FsUnit.Xunit
open RyakDB.DataType
open RyakDB.Table
open RyakDB.Index
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
              BlockSize = 1024
              BufferPoolSize = 200
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    setupStudentTable db

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
              BlockSize = 1024
              BufferPoolSize = 200
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    setupStudentTable db

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
              BlockSize = 1024
              BufferPoolSize = 200
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    setupStudentTable db

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
              BlockSize = 1024
              BufferPoolSize = 200
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    setupStudentTable db
    setupDeptTable db

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
              BlockSize = 1024
              BufferPoolSize = 200
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    setupStudentTable db

    let tx =
        db.Transaction.NewTransaction false Serializable

    let newSortScan =
        MergeSort.newSortScanFactory db.File db.BufferPool tx

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
              BlockSize = 1024
              BufferPoolSize = 200
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    setupStudentTable db

    let tx =
        db.Transaction.NewTransaction false Serializable

    let newSortScan =
        MergeSort.newSortScanFactory db.File db.BufferPool tx

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

let setupIndexedData db =
    let tx =
        db.Transaction.NewTransaction false Serializable

    let sch = Schema.newSchema ()
    sch.AddField "key_1" IntDbType
    sch.AddField "key_2" IntDbType
    sch.AddField "key_3" IntDbType
    sch.AddField "data" (VarcharDbType 100)
    db.Catalog.CreateTable tx "testing_table" sch
    db.Catalog.CreateIndex tx "testing_index" BTree "testing_table" [ "key_1"; "key_2"; "key_3" ]

    use tf =
        db.Catalog.GetTableInfo tx "testing_table"
        |> Option.get
        |> TableFile.newTableFile db.File tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true

    use idx =
        db.Catalog.GetIndexInfoByName tx "testing_index"
        |> Option.get
        |> IndexFactory.newIndex db.File tx

    [ 1 .. 10 ]
    |> List.iter (fun i ->
        [ 1 .. 10 ]
        |> List.iter (fun j ->
            [ 1 .. 10 ]
            |> List.iter (fun k ->
                tf.Insert()
                tf.SetVal "key_1" (IntDbConstant i)
                tf.SetVal "key_2" (IntDbConstant j)
                tf.SetVal "key_3" (IntDbConstant k)
                tf.SetVal
                    "data"
                    (DbConstant.newVarchar
                        ("test_"
                         + i.ToString()
                         + "_"
                         + j.ToString()
                         + "_"
                         + k.ToString()))
                idx.Insert
                    true
                    (SearchKey.newSearchKey [ IntDbConstant i
                                              IntDbConstant j
                                              IntDbConstant k ])
                    (tf.CurrentRecordId()))))

    let sch2 = Schema.newSchema ()
    sch2.AddField "join_key_1" IntDbType
    sch2.AddField "join_key_2" IntDbType
    sch2.AddField "join_key_3" IntDbType
    sch2.AddField "join_data" (VarcharDbType 100)
    db.Catalog.CreateTable tx "testing_join_table" sch2

    use tf2 =
        db.Catalog.GetTableInfo tx "testing_join_table"
        |> Option.get
        |> TableFile.newTableFile db.File tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true

    [ 1 .. 10 ]
    |> List.iter (fun i ->
        [ 1 .. 10 ]
        |> List.iter (fun j ->
            [ 1 .. 10 ]
            |> List.iter (fun k ->
                tf2.Insert()
                tf2.SetVal "join_key_1" (IntDbConstant i)
                tf2.SetVal "join_key_2" (IntDbConstant j)
                tf2.SetVal "join_key_3" (IntDbConstant k)
                tf2.SetVal
                    "join_data"
                    (DbConstant.newVarchar
                        ("join_test_"
                         + i.ToString()
                         + "_"
                         + j.ToString()
                         + "_"
                         + k.ToString())))))

    tx.Commit()

[<Fact>]
let ``index select plan`` () =
    use db =
        { Database.defaultConfig () with
              BlockSize = 1024
              BufferPoolSize = 200
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    setupIndexedData db

    let tx =
        db.Transaction.NewTransaction false Serializable

    let ii =
        db.Catalog.GetIndexInfoByName tx "testing_index"
        |> Option.get

    let searchKey =
        SearchKey.newSearchKey [ IntDbConstant 1
                                 IntDbConstant 10
                                 IntDbConstant 7 ]
        |> SearchRange.newSearchRangeBySearchKey

    use scan =
        db.Catalog.GetTableInfo tx "testing_table"
        |> Option.get
        |> Plan.newTablePlan db.File tx
        |> Plan.pipeIndexSelectPlan db.File tx ii searchKey
        |> Plan.openScan

    scan.BeforeFirst()
    scan.Next() |> should be True
    scan.GetVal "key_1"
    |> DbConstant.toInt
    |> should equal 1
    scan.GetVal "key_2"
    |> DbConstant.toInt
    |> should equal 10
    scan.GetVal "key_3"
    |> DbConstant.toInt
    |> should equal 7
    scan.GetVal "data"
    |> DbConstant.toString
    |> should equal "test_1_10_7"
    scan.Next() |> should be False

    tx.Commit()

[<Fact>]
let ``range index select plan`` () =
    use db =
        { Database.defaultConfig () with
              BlockSize = 1024
              BufferPoolSize = 200
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    setupIndexedData db

    let tx =
        db.Transaction.NewTransaction false Serializable

    let ii =
        db.Catalog.GetIndexInfoByName tx "testing_index"
        |> Option.get

    let searchKey =
        SearchRange.newSearchRangeByFieldsRanges
            (IndexInfo.fieldNames ii)
            (Map.ofList [ "key_1",
                          IntDbConstant 5
                          |> DbConstantRange.newConstantRangeByConstant
                          "key_2",
                          DbConstantRange.newConstantRange (IntDbConstant 2 |> Some) true (IntDbConstant 8 |> Some) true ])

    use scan =
        db.Catalog.GetTableInfo tx "testing_table"
        |> Option.get
        |> Plan.newTablePlan db.File tx
        |> Plan.pipeIndexSelectPlan db.File tx ii searchKey
        |> Plan.openScan

    let mutable i = 0
    scan.BeforeFirst()
    while scan.Next() do
        i <- i + 1
        scan.GetVal "key_1"
        |> DbConstant.toInt
        |> should equal 5
    i |> should equal 70

    tx.Commit()

[<Fact>]
let ``index join plan`` () =
    use db =
        { Database.defaultConfig () with
              BlockSize = 1024
              BufferPoolSize = 200
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    setupIndexedData db

    let tx =
        db.Transaction.NewTransaction false Serializable

    let pred =
        Predicate [ Term(EqualOperator, FieldNameExpression("join_key_1"), ConstantExpression(IntDbConstant 10))
                    Term(EqualOperator, FieldNameExpression("join_key_2"), ConstantExpression(IntDbConstant 3))
                    Term
                        (GraterThanEqualOperator, FieldNameExpression("join_key_3"), ConstantExpression(IntDbConstant 5)) ]

    let p1 =
        db.Catalog.GetTableInfo tx "testing_join_table"
        |> Option.get
        |> Plan.newTablePlan db.File tx
        |> Plan.pipeSelectPlan pred

    let ii =
        db.Catalog.GetIndexInfoByName tx "testing_index"
        |> Option.get

    let p2 =
        db.Catalog.GetTableInfo tx "testing_table"
        |> Option.get
        |> Plan.newTablePlan db.File tx

    use scan =
        p1
        |> Plan.pipeIndexJoinPlan
            db.File
               tx
               p2
               ii
               [ "key_1", "join_key_1"
                 "key_2", "join_key_2"
                 "key_3", "join_key_3" ]
        |> Plan.openScan

    let mutable i = 0
    scan.BeforeFirst()
    while scan.Next() do
        i <- i + 1
        scan.GetVal "key_1"
        |> DbConstant.toInt
        |> should equal 10
        scan.GetVal "key_2"
        |> DbConstant.toInt
        |> should equal 3
        scan.GetVal "join_key_1"
        |> DbConstant.toInt
        |> should equal 10
        scan.GetVal "join_key_2"
        |> DbConstant.toInt
        |> should equal 3
        scan.GetVal "key_3"
        |> should equal (scan.GetVal "join_key_3")

    i |> should equal 6

    tx.Commit()
