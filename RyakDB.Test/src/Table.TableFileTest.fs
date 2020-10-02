module RyakDB.Test.Table.TableFileTest

open Xunit
open FsUnit.Xunit
open RyakDB.DataType
open RyakDB.Table
open RyakDB.Table.TableFile
open RyakDB.Transaction
open RyakDB.Database

let newSchema () =
    let schema = Schema.newSchema ()
    schema.AddField "cid" IntDbType
    schema.AddField "title" (VarcharDbType 20)
    schema.AddField "deptid" BigIntDbType
    schema

[<Fact>]
let ``record file`` () =
    let tableName = "test_record_file"

    use db =
        { Database.defaultConfig () with
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let ti =
        TableInfo.newTableInfo tableName (newSchema ())

    let tx =
        db.TxMgr.NewTransaction false Serializable

    FileHeaderFormatter.format
    |> tx.Buffer.PinNew ti.FileName
    |> tx.Buffer.Unpin

    let rf1 =
        newTableFile db.FileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true ti

    rf1.BeforeFirst()
    while rf1.Next() do
        rf1.Delete()
    rf1.Close()

    let rf2 =
        newTableFile db.FileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true ti

    for i in 0 .. 300 do
        rf2.Insert()
        rf2.SetVal "cid" (IntDbConstant i)
        rf2.SetVal "title" (DbConstant.newVarchar ("course" + i.ToString()))
        rf2.SetVal "deptid" (BigIntDbConstant(int64 (i % 3 + 1) * 1000L))
    rf2.Close()

    let rf3 =
        newTableFile db.FileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true ti

    rf3.BeforeFirst()
    let mutable readId = 0
    while rf3.Next() do
        rf3.GetVal "title"
        |> DbConstant.toString
        |> should equal ("course" + readId.ToString())
        rf3.GetVal "deptid"
        |> DbConstant.toLong
        |> should equal (int64 (readId % 3 + 1) * 1000L)
        rf3.GetVal "cid"
        |> DbConstant.toInt
        |> should equal readId
        readId <- readId + 1
    rf3.Close()
    readId |> should equal 301

    let rf4 =
        newTableFile db.FileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true ti

    rf4.BeforeFirst()
    let mutable numdeleted = 0
    while rf4.Next() do
        if rf4.GetVal "deptid" = BigIntDbConstant 3000L then
            rf4.Delete()
            numdeleted <- numdeleted + 1
    numdeleted |> should equal 100

    rf4.BeforeFirst()
    while rf4.Next() do
        rf4.GetVal "deptid"
        |> should not' (equal (BigIntDbConstant 3000L))

    for i in 301 .. 456 do
        rf4.Insert()
        rf4.SetVal "cid" (IntDbConstant i)
        rf4.SetVal "title" (DbConstant.newVarchar ("course" + i.ToString()))
        rf4.SetVal "deptid" (BigIntDbConstant(int64 (i % 3 + 1) * 1000L))
    rf4.Close()

    tx.Commit()
