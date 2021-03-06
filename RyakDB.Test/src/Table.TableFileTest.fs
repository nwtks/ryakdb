module RyakDB.Test.Table.TableFileTest

open Xunit
open FsUnit.Xunit
open RyakDB.DataType
open RyakDB.Table
open RyakDB.Transaction
open RyakDB.Table.TableFile
open RyakDB.Database

let createTableInfo () =
    let schema = Schema.newSchema ()
    schema.AddField "cid" IntDbType
    schema.AddField "title" (VarcharDbType 20)
    schema.AddField "deptid" BigIntDbType
    TableInfo.newTableInfo "test_record_file" schema

[<Fact>]
let ``insert read delete`` () =
    let ti = createTableInfo ()

    use db =
        { Database.defaultConfig () with
              BlockSize = 1024
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let tx =
        db.Transaction.NewTransaction false Serializable

    FileHeaderFormatter.format
    |> tx.Buffer.PinNew(TableInfo.tableFileName ti)
    |> tx.Buffer.Unpin
    use tf = newTableFile db.File tx true ti

    [ 1 .. 100 ]
    |> List.iter (fun i ->
        tf.Insert()
        tf.SetVal "cid" (IntDbConstant i)
        tf.SetVal "title" (DbConstant.newVarchar ("course" + i.ToString()))
        tf.SetVal "deptid" (BigIntDbConstant(int64 (i % 3 + 1) * 1000L)))

    tf.BeforeFirst()
    let mutable readId = 1
    while tf.Next() do
        tf.GetVal "title"
        |> DbConstant.toString
        |> should equal ("course" + readId.ToString())

        tf.GetVal "deptid"
        |> DbConstant.toLong
        |> should equal (int64 (readId % 3 + 1) * 1000L)

        tf.GetVal "cid"
        |> DbConstant.toInt
        |> should equal readId

        readId <- readId + 1
    readId |> should equal 101

    tf.BeforeFirst()
    let mutable deletedCount = 0
    while tf.Next() do
        if tf.GetVal "deptid" = BigIntDbConstant 3000L then
            tf.Delete()
            deletedCount <- deletedCount + 1
    deletedCount |> should equal 33

    tf.BeforeFirst()
    while tf.Next() do
        tf.GetVal "deptid"
        |> should not' (equal (BigIntDbConstant 3000L))

    [ 31 .. 45 ]
    |> List.iter (fun i ->
        tf.Insert()
        tf.SetVal "cid" (IntDbConstant i)
        tf.SetVal "title" (DbConstant.newVarchar ("course" + i.ToString()))
        tf.SetVal "deptid" (BigIntDbConstant(int64 (i % 3 + 1) * 1000L)))

    tx.Commit()

[<Fact>]
let ``reuse slot`` () =
    let ti = createTableInfo ()

    use db =
        { Database.defaultConfig () with
              BlockSize = 1024
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let tx =
        db.Transaction.NewTransaction false Serializable

    TableInfo.tableFileName ti
    |> TableFile.formatFileHeader db.File tx
    use tf = newTableFile db.File tx true ti

    tf.Insert()
    tf.SetVal "cid" (IntDbConstant 1)
    tf.SetVal "title" (DbConstant.newVarchar "course1")
    tf.SetVal "deptid" (BigIntDbConstant 1001L)

    tf.BeforeFirst()
    tf.Next() |> should be True
    tf.GetVal "title"
    |> DbConstant.toString
    |> should equal "course1"
    tf.Delete()

    tf.BeforeFirst()
    tf.Next() |> should be False

    tf.Insert()
    tf.SetVal "cid" (IntDbConstant 2)
    tf.SetVal "title" (DbConstant.newVarchar "course2")
    tf.SetVal "deptid" (BigIntDbConstant 1002L)

    tf.BeforeFirst()
    tf.Next() |> should be True
    tf.GetVal "title"
    |> DbConstant.toString
    |> should equal "course2"
    tf.Next() |> should be False

    tx.Commit()

[<Fact>]
let ``undo insert`` () =
    let ti = createTableInfo ()

    use db =
        { Database.defaultConfig () with
              BlockSize = 1024
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let tx =
        db.Transaction.NewTransaction false Serializable

    TableInfo.tableFileName ti
    |> TableFile.formatFileHeader db.File tx
    use tf = newTableFile db.File tx true ti

    tf.Insert()
    tf.SetVal "cid" (IntDbConstant 1)
    tf.SetVal "title" (DbConstant.newVarchar "course1")
    tf.SetVal "deptid" (BigIntDbConstant 1001L)

    tf.Insert()
    tf.SetVal "cid" (IntDbConstant 2)
    tf.SetVal "title" (DbConstant.newVarchar "course2")
    tf.SetVal "deptid" (BigIntDbConstant 1002L)

    tf.BeforeFirst()
    tf.Next() |> should be True
    tf.GetVal "title"
    |> DbConstant.toString
    |> should equal "course1"
    tf.Next() |> should be True
    tf.GetVal "title"
    |> DbConstant.toString
    |> should equal "course2"

    RecordId.newBlockRecordId 0 (TableInfo.tableFileName ti) 1L
    |> tf.UndoInsert

    tf.BeforeFirst()
    tf.Next() |> should be True
    tf.GetVal "title"
    |> DbConstant.toString
    |> should equal "course2"
    tf.Next() |> should be False

    tx.Commit()

[<Fact>]
let ``undo delete`` () =
    let ti = createTableInfo ()

    use db =
        { Database.defaultConfig () with
              BlockSize = 1024
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    let tx =
        db.Transaction.NewTransaction false Serializable

    TableInfo.tableFileName ti
    |> TableFile.formatFileHeader db.File tx
    use tf = newTableFile db.File tx true ti

    tf.Insert()
    tf.SetVal "cid" (IntDbConstant 1)
    tf.SetVal "title" (DbConstant.newVarchar "course1")
    tf.SetVal "deptid" (BigIntDbConstant 1001L)
    tf.Insert()
    tf.SetVal "cid" (IntDbConstant 2)
    tf.SetVal "title" (DbConstant.newVarchar "course2")
    tf.SetVal "deptid" (BigIntDbConstant 1002L)

    tf.BeforeFirst()
    tf.Next() |> should be True
    tf.GetVal "title"
    |> DbConstant.toString
    |> should equal "course1"
    tf.Delete()
    tf.Next() |> should be True
    tf.GetVal "title"
    |> DbConstant.toString
    |> should equal "course2"
    tf.Delete()

    RecordId.newBlockRecordId 0 (TableInfo.tableFileName ti) 1L
    |> tf.UndoDelete
    RecordId.newBlockRecordId 1 (TableInfo.tableFileName ti) 1L
    |> tf.UndoDelete

    tf.BeforeFirst()
    tf.Next() |> should be True
    tf.GetVal "title"
    |> DbConstant.toString
    |> should equal "course1"
    tf.Next() |> should be True
    tf.GetVal "title"
    |> DbConstant.toString
    |> should equal "course2"
    tf.Next() |> should be False

    tx.Commit()
