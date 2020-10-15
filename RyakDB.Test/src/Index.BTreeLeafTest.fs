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

[<Fact>]
let insert () =
    let indexFilename = "_test_insert_index"
    let indexBlockId = BlockId.newBlockId indexFilename 0L

    let keyType =
        SearchKeyType.newSearchKeyTypeByTypes [ IntDbType ]

    use db =
        { Database.defaultConfig () with
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let tx =
        db.Transaction.NewTransaction false Serializable

    let filename = "_test_insert"
    newBTreePageFormatter (BTreeLeaf.keyTypeToSchema keyType) [ -1L; -1L ]
    |> tx.Buffer.PinNew filename
    |> ignore
    let blockId = BlockId.newBlockId filename 0L
    for i in 0 .. 20 do
        use leaf =
            newBTreeLeaf tx.Buffer tx.Concurrency tx.Recovery indexFilename blockId keyType

        RecordId.newRecordId i indexBlockId
        |> leaf.Insert(SearchKey.newSearchKey [ IntDbConstant i ])
        |> ignore

    use leaf =
        newBTreeLeaf tx.Buffer tx.Concurrency tx.Recovery indexFilename blockId keyType

    SearchRange.newSearchRangeByRanges [ DbConstantRange.newConstantRange
                                             (IntDbConstant 0 |> Some)
                                             true
                                             (IntDbConstant 20 |> Some)
                                             true ]
    |> leaf.BeforeFirst
    for i in 0 .. 20 do
        leaf.Next() |> should be True
        leaf.GetDataRecordId()
        |> should equal (RecordId.newRecordId i indexBlockId)

    tx.Commit()

[<Fact>]
let delete () =
    let indexFilename = "_test_delete_index"
    let indexBlockId = BlockId.newBlockId indexFilename 0L

    let keyType =
        SearchKeyType.newSearchKeyTypeByTypes [ IntDbType ]

    use db =
        { Database.defaultConfig () with
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let tx =
        db.Transaction.NewTransaction false Serializable

    let filename = "_test_delete"
    newBTreePageFormatter (BTreeLeaf.keyTypeToSchema keyType) [ -1L; -1L ]
    |> tx.Buffer.PinNew filename
    |> ignore
    let blockId = BlockId.newBlockId filename 0L
    for i in 0 .. 20 do
        use leaf =
            newBTreeLeaf tx.Buffer tx.Concurrency tx.Recovery indexFilename blockId keyType

        RecordId.newRecordId i indexBlockId
        |> leaf.Insert(SearchKey.newSearchKey [ IntDbConstant i ])
        |> ignore

    for i in 0 .. 20 do
        use leaf =
            newBTreeLeaf tx.Buffer tx.Concurrency tx.Recovery indexFilename blockId keyType

        RecordId.newRecordId i indexBlockId
        |> leaf.Delete(SearchKey.newSearchKey [ IntDbConstant i ])

    use leaf =
        newBTreeLeaf tx.Buffer tx.Concurrency tx.Recovery indexFilename blockId keyType

    SearchRange.newSearchRangeByRanges [ DbConstantRange.newConstantRange
                                             (IntDbConstant 0 |> Some)
                                             true
                                             (IntDbConstant 20 |> Some)
                                             true ]
    |> leaf.BeforeFirst
    leaf.Next() |> should be False

    tx.Commit()

[<Fact>]
let ``split sibling`` () =
    let indexFilename = "_test_sibling_index"
    let indexBlockId = BlockId.newBlockId indexFilename 0L

    let keyType =
        SearchKeyType.newSearchKeyTypeByTypes [ IntDbType ]

    use db =
        { Database.defaultConfig () with
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let tx =
        db.Transaction.NewTransaction false Serializable

    let filename = "_test_sibling"
    newBTreePageFormatter (BTreeLeaf.keyTypeToSchema keyType) [ -1L; -1L ]
    |> tx.Buffer.PinNew filename
    |> ignore
    let blockId = BlockId.newBlockId filename 0L
    let buffer = tx.Buffer.Pin blockId

    let maxCount =
        (buffer.BufferSize - 20)
        / (BTreePage.slotSize (BTreeLeaf.keyTypeToSchema keyType) buffer)

    let count = maxCount * 2

    let mutable newEntry = None
    for i in 0 .. count do
        use leaf =
            newBTreeLeaf tx.Buffer tx.Concurrency tx.Recovery indexFilename blockId keyType

        RecordId.newRecordId i indexBlockId
        |> leaf.Insert(SearchKey.newSearchKey [ IntDbConstant i ])
        |> Option.iter (fun e -> newEntry <- Some(e))

    if Option.isNone newEntry then failwith "Not split"

    use leaf =
        newBTreeLeaf tx.Buffer tx.Concurrency tx.Recovery indexFilename blockId keyType

    let mutable cnt = 0
    SearchRange.newSearchRangeByRanges [ DbConstantRange.newConstantRange
                                             (IntDbConstant 0 |> Some)
                                             true
                                             (IntDbConstant count |> Some)
                                             true ]
    |> leaf.BeforeFirst
    while leaf.Next() do
        cnt <- cnt + 1
    cnt |> should equal (count + 1)

    for i in 0 .. count do
        use leaf =
            newBTreeLeaf tx.Buffer tx.Concurrency tx.Recovery indexFilename blockId keyType

        RecordId.newRecordId i indexBlockId
        |> leaf.Delete(SearchKey.newSearchKey [ IntDbConstant i ])

    tx.Commit()

[<Fact>]
let ``split overflow`` () =
    let indexFilename = "_test_overflow_index"
    let indexBlockId = BlockId.newBlockId indexFilename 0L

    let keyType =
        SearchKeyType.newSearchKeyTypeByTypes [ IntDbType ]

    use db =
        { Database.defaultConfig () with
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let tx =
        db.Transaction.NewTransaction false Serializable

    let filename = "_test_overflow"
    newBTreePageFormatter (BTreeLeaf.keyTypeToSchema keyType) [ -1L; -1L ]
    |> tx.Buffer.PinNew filename
    |> ignore
    let blockId = BlockId.newBlockId filename 0L
    let buffer = tx.Buffer.Pin blockId

    let maxCount =
        (buffer.BufferSize - 20)
        / (BTreePage.slotSize (BTreeLeaf.keyTypeToSchema keyType) buffer)

    let count = maxCount * 2

    let insertkey =
        SearchKey.newSearchKey [ IntDbConstant 0 ]

    for i in 0 .. count do
        use leaf =
            newBTreeLeaf tx.Buffer tx.Concurrency tx.Recovery indexFilename blockId keyType

        RecordId.newRecordId i indexBlockId
        |> leaf.Insert insertkey
        |> ignore

    use leaf =
        newBTreeLeaf tx.Buffer tx.Concurrency tx.Recovery indexFilename blockId keyType

    let mutable cnt = 0
    SearchRange.newSearchRangeByRanges [ DbConstantRange.newConstantRange
                                             (IntDbConstant 0 |> Some)
                                             true
                                             (IntDbConstant count |> Some)
                                             true ]
    |> leaf.BeforeFirst
    while leaf.Next() do
        cnt <- cnt + 1
    cnt |> should equal (count + 1)

    for i in 0 .. count do
        use leaf =
            newBTreeLeaf tx.Buffer tx.Concurrency tx.Recovery indexFilename blockId keyType

        RecordId.newRecordId i indexBlockId
        |> leaf.Delete insertkey

    tx.Commit()
