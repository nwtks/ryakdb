module RyakDB.Test.Index.BTreePageTest

open Xunit
open FsUnit.Xunit
open RyakDB.Index.BTreePage
open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Table
open RyakDB.Transaction
open RyakDB.Database

let createTx () =
    let db =
        { Database.defaultConfig () with
              InMemory = true }
        |> createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    db.TxMgr.NewTransaction false Serializable

let createSchema () =
    let sch = Schema.newSchema ()
    sch.AddField "id" IntDbType
    sch

[<Fact>]
let insert () =
    let filename = "_test_insert"
    let tx = createTx ()
    let schema = createSchema ()
    newBTreePageFormatter schema [ 0L ]
    |> tx.Buffer.PinNew filename
    |> ignore

    let blockId = BlockId.newBlockId filename 0L

    let page =
        newBTreePage tx.Buffer tx.Concurrency tx.Recovery schema blockId 1

    for i in 0 .. 20 do
        page.Insert 0
        page.SetVal 0 "id" (IntDbConstant(20 - i))
    page.GetCountOfRecords() |> should equal 21
    for i in 0 .. 20 do
        page.GetVal i "id"
        |> DbConstant.toInt
        |> should equal i

    tx.Commit()

[<Fact>]
let delete () =
    let filename = "_test_delete"
    let tx = createTx ()
    let schema = createSchema ()
    newBTreePageFormatter schema [ 0L ]
    |> tx.Buffer.PinNew filename
    |> ignore

    let blockId = BlockId.newBlockId filename 0L

    let page =
        newBTreePage tx.Buffer tx.Concurrency tx.Recovery schema blockId 1

    for i in 0 .. 10 do
        page.Insert 0
        page.SetVal 0 "id" (IntDbConstant(10 - i))

    page.Delete 10
    page.Delete 7
    page.Delete 3
    page.Delete 1
    page.GetCountOfRecords() |> should equal 7
    let expect = [| 0; 2; 4; 5; 6; 8; 9 |]
    for i in 0 .. 6 do
        page.GetVal i "id"
        |> DbConstant.toInt
        |> should equal expect.[i]

    tx.Commit()

[<Fact>]
let ``transfer records`` () =
    let filename = "_test_transfer"
    let tx = createTx ()
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

    for i in 0 .. 10 do
        page1.Insert 0
        page1.SetVal 0 "id" (IntDbConstant(10 - i))

    let blockId2 = BlockId.newBlockId filename 1L

    let page2 =
        newBTreePage tx.Buffer tx.Concurrency tx.Recovery schema blockId2 1

    for i in 0 .. 5 do
        page2.Insert 0
        page2.SetVal 0 "id" (IntDbConstant(100 - i))

    page1.TransferRecords 3 page2 2 4

    page1.GetCountOfRecords() |> should equal 7
    let expect1 = [| 0; 1; 2; 7; 8; 9; 10 |]
    for i in 0 .. 6 do
        page1.GetVal i "id"
        |> DbConstant.toInt
        |> should equal expect1.[i]

    page2.GetCountOfRecords() |> should equal 10

    let expect2 =
        [| 95
           96
           3
           4
           5
           6
           97
           98
           99
           100 |]

    for i in 0 .. 9 do
        page2.GetVal i "id"
        |> DbConstant.toInt
        |> should equal expect2.[i]

    tx.Commit()

[<Fact>]
let split () =
    let filename = "_test_split"
    let tx = createTx ()
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

    for i in 0 .. 20 do
        page1.Insert 0
        page1.SetVal 0 "id" (IntDbConstant(20 - i))
    page1.GetCountOfRecords() |> should equal 21

    let blockId2 =
        page1.Split 10 [ 100L ]
        |> BlockId.newBlockId filename

    let page2 =
        newBTreePage tx.Buffer tx.Concurrency tx.Recovery schema blockId2 1

    page1.GetCountOfRecords() |> should equal 10
    for i in 0 .. 9 do
        page1.GetVal i "id"
        |> DbConstant.toInt
        |> should equal i

    page2.GetCountOfRecords() |> should equal 11
    page2.GetFlag 0 |> should equal 100L
    for i in 0 .. 10 do
        page2.GetVal i "id"
        |> DbConstant.toInt
        |> should equal (i + 10)

    tx.Commit()
