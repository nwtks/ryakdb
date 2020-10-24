module RyakDB.Test.Index.BTreePageTest

open Xunit
open FsUnit.Xunit
open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Table
open RyakDB.Index.BTreePage
open RyakDB.Transaction
open RyakDB.Database

let createSchema () =
    let sch = Schema.newSchema ()
    sch.AddField "id" IntDbType
    sch

[<Fact>]
let insert () =
    let filename = "_test_insert"

    use db =
        { Database.defaultConfig () with
              BlockSize = 1024
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let tx =
        db.Transaction.NewTransaction false Serializable

    let schema = createSchema ()
    newBTreePageFormatter schema [ 0L ]
    |> tx.Buffer.PinNew filename
    |> ignore

    let blockId = BlockId.newBlockId filename 0L

    let page =
        newBTreePage tx.Buffer tx.Concurrency tx.Recovery schema blockId 1

    [ 0 .. 20 ]
    |> List.iter (fun i ->
        page.Insert 0
        page.SetVal 0 "id" (IntDbConstant(20 - i)))

    page.GetCountOfRecords() |> should equal 21

    [ 0 .. 20 ]
    |> List.iter (fun i ->
        page.GetVal i "id"
        |> DbConstant.toInt
        |> should equal i)

    tx.Commit()

[<Fact>]
let delete () =
    let filename = "_test_delete"

    use db =
        { Database.defaultConfig () with
              BlockSize = 1024
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let tx =
        db.Transaction.NewTransaction false Serializable

    let schema = createSchema ()
    newBTreePageFormatter schema [ 0L ]
    |> tx.Buffer.PinNew filename
    |> ignore

    let blockId = BlockId.newBlockId filename 0L

    let page =
        newBTreePage tx.Buffer tx.Concurrency tx.Recovery schema blockId 1

    [ 0 .. 10 ]
    |> List.iter (fun i ->
        page.Insert 0
        page.SetVal 0 "id" (IntDbConstant(10 - i)))

    page.Delete 10
    page.Delete 7
    page.Delete 3
    page.Delete 1

    page.GetCountOfRecords() |> should equal 7

    [ 0 .. 6 ]
    |> List.map (fun i -> page.GetVal i "id" |> DbConstant.toInt)
    |> should equal [ 0; 2; 4; 5; 6; 8; 9 ]

    tx.Commit()

[<Fact>]
let ``transfer records`` () =
    let filename = "_test_transfer"

    use db =
        { Database.defaultConfig () with
              BlockSize = 1024
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let tx =
        db.Transaction.NewTransaction false Serializable

    let schema = createSchema ()
    newBTreePageFormatter schema [ 0L ]
    |> tx.Buffer.PinNew filename
    |> ignore
    newBTreePageFormatter schema [ 0L ]
    |> tx.Buffer.PinNew filename
    |> ignore

    let blockId1 = BlockId.newBlockId filename 0L

    let page1 =
        newBTreePage tx.Buffer tx.Concurrency tx.Recovery schema blockId1 1

    [ 0 .. 10 ]
    |> List.iter (fun i ->
        page1.Insert 0
        page1.SetVal 0 "id" (IntDbConstant(10 - i)))

    let blockId2 = BlockId.newBlockId filename 1L

    let page2 =
        newBTreePage tx.Buffer tx.Concurrency tx.Recovery schema blockId2 1

    [ 0 .. 5 ]
    |> List.iter (fun i ->
        page2.Insert 0
        page2.SetVal 0 "id" (IntDbConstant(100 - i)))

    page1.TransferRecords 3 page2 2 4

    page1.GetCountOfRecords() |> should equal 7

    [ 0 .. 6 ]
    |> List.map (fun i -> page1.GetVal i "id" |> DbConstant.toInt)
    |> should equal [ 0; 1; 2; 7; 8; 9; 10 ]

    page2.GetCountOfRecords() |> should equal 10

    [ 0 .. 9 ]
    |> List.map (fun i -> page2.GetVal i "id" |> DbConstant.toInt)
    |> should equal [ 95; 96; 3; 4; 5; 6; 97; 98; 99; 100 ]

    tx.Commit()

[<Fact>]
let split () =
    let filename = "_test_split"

    use db =
        { Database.defaultConfig () with
              BlockSize = 1024
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let tx =
        db.Transaction.NewTransaction false Serializable

    let schema = createSchema ()
    newBTreePageFormatter schema [ 0L ]
    |> tx.Buffer.PinNew filename
    |> ignore
    newBTreePageFormatter schema [ 0L ]
    |> tx.Buffer.PinNew filename
    |> ignore

    let blockId1 = BlockId.newBlockId filename 0L

    let page1 =
        newBTreePage tx.Buffer tx.Concurrency tx.Recovery schema blockId1 1

    [ 0 .. 20 ]
    |> List.iter (fun i ->
        page1.Insert 0
        page1.SetVal 0 "id" (IntDbConstant(20 - i)))

    page1.GetCountOfRecords() |> should equal 21

    let blockId2 =
        page1.Split 8 [ 100L ]
        |> BlockId.newBlockId filename

    let page2 =
        newBTreePage tx.Buffer tx.Concurrency tx.Recovery schema blockId2 1

    page1.GetCountOfRecords() |> should equal 8

    [ 0 .. 7 ]
    |> List.iter (fun i ->
        page1.GetVal i "id"
        |> DbConstant.toInt
        |> should equal i)

    page2.GetCountOfRecords() |> should equal 13

    page2.GetFlag 0 |> should equal 100L

    [ 0 .. 12 ]
    |> List.iter (fun i ->
        page2.GetVal i "id"
        |> DbConstant.toInt
        |> should equal (i + 8))

    tx.Commit()
