module RyakDB.Test.Table.TablePageTest

open Xunit
open FsUnit.Xunit
open RyakDB.DataType
open RyakDB.Table
open RyakDB.Transaction
open RyakDB.Table.TablePage
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
              BlockSize = 1024
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let tx =
        db.Transaction.NewTransaction false Serializable

    let buff =
        newTablePageFormatter schema
        |> tx.Buffer.PinNew "TablePageTest.tbl"

    tx.Buffer.Unpin buff

    use tp1 =
        newTablePage tx (buff.BlockId()) schema true

    let mutable count = 0
    let mutable insertId = 0
    while tp1.InsertIntoNextEmptySlot() do
        tp1.SetVal "cid" (IntDbConstant insertId)
        tp1.SetVal "deptid" (BigIntDbConstant(int64 (insertId % 3 + 1) * 1000L))
        tp1.SetVal "title" (DbConstant.newVarchar ("course" + insertId.ToString()))
        insertId <- insertId + 1
        count <- count + 1

    use tp2 =
        newTablePage tx (buff.BlockId()) schema true

    let mutable readId = 0
    while tp2.Next() do
        tp2.GetVal "title"
        |> DbConstant.toString
        |> should equal ("course" + readId.ToString())

        tp2.GetVal "deptid"
        |> DbConstant.toLong
        |> should equal (int64 (readId % 3 + 1) * 1000L)

        tp2.GetVal "cid"
        |> DbConstant.toInt
        |> should equal readId

        readId <- readId + 1
    readId |> should equal count

    use tp3 =
        newTablePage tx (buff.BlockId()) schema true

    let mutable deletedCount = 0
    while tp3.Next() do
        if tp3.GetVal "deptid" = BigIntDbConstant 3000L then
            RecordId.newBlockRecordId -1 "TablePageTest.tbl" -1L
            |> tp3.Delete
            deletedCount <- deletedCount + 1
    deletedCount |> should equal (count / 3)

    use tp4 =
        newTablePage tx (buff.BlockId()) schema true

    while tp4.Next() do
        tp4.GetVal "deptid"
        |> should not' (equal (BigIntDbConstant 3000L))

    tx.Commit()
