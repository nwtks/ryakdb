module RyakDB.Test.Index.BTreeLeafTest

open Xunit
open FsUnit.Xunit
open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Table
open RyakDB.Index
open RyakDB.Index.BTreePage
open RyakDB.Index.BTreeLeaf
open RyakDB.Transaction
open RyakDB.Database

let createTx () =
    let db =
        { Database.defaultConfig () with
              InMemory = true }
        |> createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    db.TxMgr.NewTransaction false Serializable

[<Fact>]
let insert () =
    let indexFilename = "_test_insert_index"
    let indexBlockId = BlockId.newBlockId indexFilename 0L

    let keyType =
        SearchKeyType.newSearchKeyTypeByTypes [ IntDbType ]

    let tx = createTx ()
    let filename = "_test_insert"
    newBTreePageFormatter (BTreeLeaf.keyTypeToSchema keyType) [ -1L; -1L ]
    |> tx.Buffer.PinNew filename
    |> ignore
    let blockId = BlockId.newBlockId filename 0L
    for i in 0 .. 20 do
        let leaf =
            SearchKey.newSearchKey [ IntDbConstant i ]
            |> SearchRange.newSearchRangeBySearchKey
            |> newBTreeLeaf tx.Buffer tx.Concurrency tx.Recovery indexFilename blockId keyType

        RecordId.newRecordId i indexBlockId
        |> leaf.Insert
        |> ignore
        leaf.Close()

    let leaf =
        SearchRange.newSearchRangeByRanges [ DbConstantRange.newConstantRange
                                                 IntDbType
                                                 (IntDbConstant 0 |> Some)
                                                 true
                                                 (IntDbConstant 20 |> Some)
                                                 true ]
        |> newBTreeLeaf tx.Buffer tx.Concurrency tx.Recovery indexFilename blockId keyType

    leaf.GetCountOfRecords() |> should equal 21
    for i in 0 .. 20 do
        leaf.Next() |> should be True
        leaf.GetDataRecordId()
        |> should equal (RecordId.newRecordId i indexBlockId)
    leaf.Close()

    tx.Commit()

[<Fact>]
let delete () =
    let indexFilename = "_test_delete_index"
    let indexBlockId = BlockId.newBlockId indexFilename 0L

    let keyType =
        SearchKeyType.newSearchKeyTypeByTypes [ IntDbType ]

    let tx = createTx ()
    let filename = "_test_delete"
    newBTreePageFormatter (BTreeLeaf.keyTypeToSchema keyType) [ -1L; -1L ]
    |> tx.Buffer.PinNew filename
    |> ignore
    let blockId = BlockId.newBlockId filename 0L
    for i in 0 .. 20 do
        let leaf =
            SearchKey.newSearchKey [ IntDbConstant i ]
            |> SearchRange.newSearchRangeBySearchKey
            |> newBTreeLeaf tx.Buffer tx.Concurrency tx.Recovery indexFilename blockId keyType

        RecordId.newRecordId i indexBlockId
        |> leaf.Insert
        |> ignore
        leaf.Close()

    for i in 0 .. 20 do
        let leaf =
            SearchKey.newSearchKey [ IntDbConstant i ]
            |> SearchRange.newSearchRangeBySearchKey
            |> newBTreeLeaf tx.Buffer tx.Concurrency tx.Recovery indexFilename blockId keyType

        RecordId.newRecordId i indexBlockId |> leaf.Delete
        leaf.Close()

    let leaf =
        SearchRange.newSearchRangeByRanges [ DbConstantRange.newConstantRange
                                                 IntDbType
                                                 (IntDbConstant 0 |> Some)
                                                 true
                                                 (IntDbConstant 20 |> Some)
                                                 true ]
        |> newBTreeLeaf tx.Buffer tx.Concurrency tx.Recovery indexFilename blockId keyType

    leaf.GetCountOfRecords() |> should equal 0
    leaf.Close()

    tx.Commit()

[<Fact>]
let split () =
    let indexFilename = "_test_split_index"
    let indexBlockId = BlockId.newBlockId indexFilename 0L

    let keyType =
        SearchKeyType.newSearchKeyTypeByTypes [ IntDbType ]

    let tx = createTx ()
    let filename = "_test_split"
    newBTreePageFormatter (BTreeLeaf.keyTypeToSchema keyType) [ -1L; -1L ]
    |> tx.Buffer.PinNew filename
    |> ignore
    let blockId = BlockId.newBlockId filename 0L
    let buffer = tx.Buffer.Pin blockId

    let maxCount =
        (buffer.BufferSize - 8)
        / (BTreePage.slotSize (BTreeLeaf.keyTypeToSchema keyType) buffer)

    let count = maxCount * 2

    let mutable newEntry = None
    for i in 0 .. count do
        let leaf =
            SearchKey.newSearchKey [ IntDbConstant i ]
            |> SearchRange.newSearchRangeBySearchKey
            |> newBTreeLeaf tx.Buffer tx.Concurrency tx.Recovery indexFilename blockId keyType

        RecordId.newRecordId i indexBlockId
        |> leaf.Insert
        |> Option.iter (fun e -> newEntry <- Some(e))
        leaf.Close()
    if Option.isNone newEntry then failwith "Not split"

    let leaf =
        SearchRange.newSearchRangeByRanges [ DbConstantRange.newConstantRange
                                                 IntDbType
                                                 (IntDbConstant 0 |> Some)
                                                 true
                                                 (IntDbConstant count |> Some)
                                                 true ]
        |> newBTreeLeaf tx.Buffer tx.Concurrency tx.Recovery indexFilename blockId keyType

    let mutable cnt = 0
    while leaf.Next() do
        cnt <- cnt + 1
    leaf.Close()
    cnt |> should equal (count + 1)

    for i in 0 .. count do
        let leaf =
            SearchKey.newSearchKey [ IntDbConstant i ]
            |> SearchRange.newSearchRangeBySearchKey
            |> newBTreeLeaf tx.Buffer tx.Concurrency tx.Recovery indexFilename blockId keyType

        RecordId.newRecordId i indexBlockId |> leaf.Delete
        leaf.Close()

    tx.Commit()

[<Fact>]
let overflow () =
    let indexFilename = "_test_overflow_index"
    let indexBlockId = BlockId.newBlockId indexFilename 0L

    let keyType =
        SearchKeyType.newSearchKeyTypeByTypes [ IntDbType ]

    let tx = createTx ()
    let filename = "_test_overflow"
    newBTreePageFormatter (BTreeLeaf.keyTypeToSchema keyType) [ -1L; -1L ]
    |> tx.Buffer.PinNew filename
    |> ignore
    let blockId = BlockId.newBlockId filename 0L
    let buffer = tx.Buffer.Pin blockId

    let maxCount =
        (buffer.BufferSize - 8)
        / (BTreePage.slotSize (BTreeLeaf.keyTypeToSchema keyType) buffer)

    let count = maxCount * 2

    let insertRange =
        SearchKey.newSearchKey [ IntDbConstant 0 ]
        |> SearchRange.newSearchRangeBySearchKey

    for i in 0 .. count do
        let leaf =
            newBTreeLeaf tx.Buffer tx.Concurrency tx.Recovery indexFilename blockId keyType insertRange

        RecordId.newRecordId i indexBlockId
        |> leaf.Insert
        |> ignore
        leaf.Close()

    let leaf =
        SearchRange.newSearchRangeByRanges [ DbConstantRange.newConstantRange
                                                 IntDbType
                                                 (IntDbConstant 0 |> Some)
                                                 true
                                                 (IntDbConstant count |> Some)
                                                 true ]
        |> newBTreeLeaf tx.Buffer tx.Concurrency tx.Recovery indexFilename blockId keyType

    let mutable cnt = 0
    while leaf.Next() do
        cnt <- cnt + 1
    leaf.Close()
    cnt |> should equal (count + 1)

    for i in 0 .. count do
        let leaf =
            newBTreeLeaf tx.Buffer tx.Concurrency tx.Recovery indexFilename blockId keyType insertRange

        RecordId.newRecordId i indexBlockId |> leaf.Delete
        leaf.Close()

    tx.Commit()
