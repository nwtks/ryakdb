module RyakDB.Test.Table.SlottedPageTest

open Xunit
open FsUnit.Xunit
open RyakDB.DataType
open RyakDB.Table
open RyakDB.Table.SlottedPage
open RyakDB.Transaction
open RyakDB.Database

let createSchema () =
    let schema = Schema.newSchema ()
    schema.AddField "cid" IntDbType
    schema.AddField "title" (VarcharDbType 20)
    schema.AddField "deptid" BigIntDbType
    schema

[<Fact>]
let ``insert read delete`` () =
    let schema = createSchema ()

    use db =
        { Database.defaultConfig () with
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let tx =
        db.Transaction.NewTransaction false Serializable

    let buff =
        newSlottedPageFormatter schema
        |> tx.Buffer.PinNew "SlottedPageTest.tbl"

    tx.Buffer.Unpin buff

    let sp1 =
        newSlottedPage tx.Buffer tx.Concurrency tx.Recovery (buff.BlockId()) schema true

    let mutable count = 0
    let mutable insertId = 0
    while sp1.InsertIntoNextEmptySlot() do
        sp1.SetVal "cid" (IntDbConstant insertId)
        sp1.SetVal "deptid" (BigIntDbConstant(int64 (insertId % 3 + 1) * 1000L))
        sp1.SetVal "title" (DbConstant.newVarchar ("course" + insertId.ToString()))
        insertId <- insertId + 1
        count <- count + 1
    sp1.Close()

    let sp2 =
        newSlottedPage tx.Buffer tx.Concurrency tx.Recovery (buff.BlockId()) schema true

    let mutable readId = 0
    while sp2.Next() do
        sp2.GetVal "title"
        |> DbConstant.toString
        |> should equal ("course" + readId.ToString())

        sp2.GetVal "deptid"
        |> DbConstant.toLong
        |> should equal (int64 (readId % 3 + 1) * 1000L)

        sp2.GetVal "cid"
        |> DbConstant.toInt
        |> should equal readId

        readId <- readId + 1
    sp2.Close()
    readId |> should equal count

    let sp3 =
        newSlottedPage tx.Buffer tx.Concurrency tx.Recovery (buff.BlockId()) schema true

    let mutable deletedCount = 0
    while sp3.Next() do
        if sp3.GetVal "deptid" = BigIntDbConstant 3000L then
            RecordId.newBlockRecordId -1 "SlottedPageTest.tbl" -1L
            |> sp3.Delete
            deletedCount <- deletedCount + 1
    sp3.Close()
    deletedCount |> should equal (count / 3)

    let sp4 =
        newSlottedPage tx.Buffer tx.Concurrency tx.Recovery (buff.BlockId()) schema true

    while sp4.Next() do
        sp4.GetVal "deptid"
        |> should not' (equal (BigIntDbConstant 3000L))
    sp4.Close()

    tx.Commit()
