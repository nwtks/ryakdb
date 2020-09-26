module RyakDB.Test.Table.SlottedPageTest

open Xunit
open FsUnit.Xunit
open RyakDB.DataType
open RyakDB.Table
open RyakDB.Table.SlottedPage
open RyakDB.Transaction
open RyakDB.Database

let newSchema () =
    let schema = Schema.newSchema ()
    schema.AddField "cid" IntDbType
    schema.AddField "title" (VarcharDbType 20)
    schema.AddField "deptid" BigIntDbType
    schema

[<Fact>]
let ``record page`` () =
    let tableName = "test_record_page"

    let db =
        { Database.defaultConfig () with
              InMemory = true }
        |> createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let ti =
        TableInfo.newTableInfo tableName (newSchema ())

    let tx =
        db.TxMgr.NewTransaction false Serializable

    let buff =
        newSlottedPageFormatter ti
        |> tx.Buffer.PinNew ti.FileName

    tx.Buffer.Unpin buff

    let dummyFreeSlot =
        RecordId.newBlockRecordId -1 ti.FileName -1L

    let rp1 =
        newSlottedPage tx.Buffer tx.Concurrency tx.Recovery (buff.BlockId()) ti true

    while rp1.Next() do
        rp1.Delete dummyFreeSlot
    rp1.Close()

    let rp2 =
        newSlottedPage tx.Buffer tx.Concurrency tx.Recovery (buff.BlockId()) ti true

    let mutable insertId = 0
    let mutable numinserted = 0
    while rp2.InsertIntoNextEmptySlot() do
        rp2.SetVal "cid" (IntDbConstant insertId)
        rp2.SetVal "deptid" (BigIntDbConstant(int64 (insertId % 3 + 1) * 1000L))
        rp2.SetVal "title" (DbConstant.newVarchar ("course" + insertId.ToString()))
        insertId <- insertId + 1
        numinserted <- numinserted + 1
    rp2.Close()

    let rp3 =
        newSlottedPage tx.Buffer tx.Concurrency tx.Recovery (buff.BlockId()) ti true

    let mutable readId = 0
    while rp3.Next() do
        rp3.GetVal "title"
        |> DbConstant.toString
        |> should equal ("course" + readId.ToString())
        rp3.GetVal "deptid"
        |> DbConstant.toLong
        |> should equal (int64 (readId % 3 + 1) * 1000L)
        rp3.GetVal "cid"
        |> DbConstant.toInt
        |> should equal readId
        readId <- readId + 1
    rp3.Close()
    readId |> should equal numinserted

    let rp4 =
        newSlottedPage tx.Buffer tx.Concurrency tx.Recovery (buff.BlockId()) ti true

    let mutable numdeleted = 0
    while rp4.Next() do
        if rp4.GetVal "deptid" = BigIntDbConstant 3000L then
            rp4.Delete dummyFreeSlot
            numdeleted <- numdeleted + 1
    rp4.Close()
    numdeleted |> should equal (numinserted / 3)

    let rp5 =
        newSlottedPage tx.Buffer tx.Concurrency tx.Recovery (buff.BlockId()) ti true

    while rp5.Next() do
        rp5.GetVal "deptid"
        |> should not' (equal (BigIntDbConstant 3000L))
    rp5.Close()

    tx.Commit()
