namespace RyakDB.Storage.RecordTest

open Xunit
open RyakDB.Sql.Type
open RyakDB.Storage.Type
open RyakDB.Storage.Record
open RyakDB.Storage.Catalog
open RyakDB.Server.Database

module RecordPageTest =
    let newSchema () =
        let schema = Schema.newSchema ()
        schema.AddField "cid" IntSqlType
        schema.AddField "title" (VarcharSqlType 20)
        schema.AddField "deptid" BigIntSqlType
        schema

    [<Fact>]
    let ``read only`` () =
        let tableName = "test_read_only"

        let db =
            { Database.defaultConfig () with
                  InMemory = true }
            |> Database.createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

        let ti =
            TableInfo.newTableInfo db.FileMgr tableName (newSchema ())

        let tx =
            db.TxMgr.NewTransaction true Serializable

        let buff =
            RecordFormatter.newFormatter ti
            |> tx.BufferMgr.PinNew ti.FileName

        tx.BufferMgr.Unpin buff

        let rp =
            RecordPage.newRecordPage tx (buff.BlockId()) ti true

        Assert.Throws(fun () -> rp.InsertIntoNextEmptySlot() |> ignore)
        |> ignore
        rp.Close()

        tx.Commit()

    [<Fact>]
    let ``record page`` () =
        let tableName = "test_record_page"

        let db =
            { Database.defaultConfig () with
                  InMemory = true }
            |> Database.createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

        let ti =
            TableInfo.newTableInfo db.FileMgr tableName (newSchema ())

        let tx =
            db.TxMgr.NewTransaction false Serializable

        let buff =
            RecordFormatter.newFormatter ti
            |> tx.BufferMgr.PinNew ti.FileName

        tx.BufferMgr.Unpin buff

        let dummyFreeSlot =
            RecordId.newBlockRecordId -1 ti.FileName -1L

        let rp1 =
            RecordPage.newRecordPage tx (buff.BlockId()) ti true

        while rp1.Next() do
            rp1.Delete dummyFreeSlot
        rp1.Close()

        let rp2 =
            RecordPage.newRecordPage tx (buff.BlockId()) ti true

        let mutable insertId = 0
        let mutable numinserted = 0
        while rp2.InsertIntoNextEmptySlot() do
            rp2.SetVal "cid" (IntSqlConstant insertId)
            rp2.SetVal "deptid" (BigIntSqlConstant(int64 (insertId % 3 + 1) * 1000L))
            rp2.SetVal "title" (SqlConstant.newVarchar ("course" + insertId.ToString()))
            insertId <- insertId + 1
            numinserted <- numinserted + 1
        rp2.Close()

        let rp3 =
            RecordPage.newRecordPage tx (buff.BlockId()) ti true

        let mutable readId = 0
        while rp3.Next() do
            Assert.Equal
                ("course" + readId.ToString(),
                 rp3.GetVal "title"
                 |> Option.get
                 |> SqlConstant.toString)
            Assert.Equal
                (int64 (readId % 3 + 1) * 1000L,
                 rp3.GetVal "deptid"
                 |> Option.get
                 |> SqlConstant.toLong)
            Assert.Equal
                (readId,
                 rp3.GetVal "cid"
                 |> Option.get
                 |> SqlConstant.toInt)
            readId <- readId + 1
        rp3.Close()
        Assert.Equal(numinserted, readId)

        let rp4 =
            RecordPage.newRecordPage tx (buff.BlockId()) ti true

        let mutable numdeleted = 0
        while rp4.Next() do
            if rp4.GetVal "deptid"
               |> Option.get = BigIntSqlConstant(3000L) then
                rp4.Delete dummyFreeSlot
                numdeleted <- numdeleted + 1
        rp4.Close()
        Assert.Equal(numinserted / 3, numdeleted)

        let rp5 =
            RecordPage.newRecordPage tx (buff.BlockId()) ti true

        while rp5.Next() do
            Assert.NotEqual(BigIntSqlConstant(3000L), rp5.GetVal "deptid" |> Option.get)
        rp5.Close()

        tx.Commit()

module RecordFileTest =
    let newSchema () =
        let schema = Schema.newSchema ()
        schema.AddField "cid" IntSqlType
        schema.AddField "title" (VarcharSqlType 20)
        schema.AddField "deptid" BigIntSqlType
        schema

    [<Fact>]
    let ``record file`` () =
        let tableName = "test_record_file"

        let db =
            { Database.defaultConfig () with
                  InMemory = true }
            |> Database.createDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

        let ti =
            TableInfo.newTableInfo db.FileMgr tableName (newSchema ())

        let tx =
            db.TxMgr.NewTransaction false Serializable

        FileHeaderFormatter.format
        |> tx.BufferMgr.PinNew ti.FileName
        |> tx.BufferMgr.Unpin

        let rf1 = ti.OpenFile tx true

        rf1.BeforeFirst()
        while rf1.Next() do
            rf1.Delete()
        rf1.Close()

        let rf2 = ti.OpenFile tx true

        for i in 0 .. 300 do
            rf2.Insert()
            rf2.SetVal "cid" (IntSqlConstant i)
            rf2.SetVal "title" (SqlConstant.newVarchar ("course" + i.ToString()))
            rf2.SetVal "deptid" (BigIntSqlConstant(int64 (i % 3 + 1) * 1000L))
        rf2.Close()

        let rf3 = ti.OpenFile tx true

        rf3.BeforeFirst()
        let mutable readId = 0
        while rf3.Next() do
            Assert.Equal
                ("course" + readId.ToString(),
                 rf3.GetVal "title"
                 |> Option.get
                 |> SqlConstant.toString)
            Assert.Equal
                (int64 (readId % 3 + 1) * 1000L,
                 rf3.GetVal "deptid"
                 |> Option.get
                 |> SqlConstant.toLong)
            Assert.Equal
                (readId,
                 rf3.GetVal "cid"
                 |> Option.get
                 |> SqlConstant.toInt)
            readId <- readId + 1
        rf3.Close()
        Assert.Equal(301, readId)

        let rf4 = ti.OpenFile tx true

        rf4.BeforeFirst()
        let mutable numdeleted = 0
        while rf4.Next() do
            if rf4.GetVal "deptid"
               |> Option.get = BigIntSqlConstant(3000L) then
                rf4.Delete()
                numdeleted <- numdeleted + 1
        Assert.Equal(100, numdeleted)

        rf4.BeforeFirst()
        while rf4.Next() do
            Assert.NotEqual(BigIntSqlConstant(3000L), rf4.GetVal "deptid" |> Option.get)

        for i in 301 .. 456 do
            rf4.Insert()
            rf4.SetVal "cid" (IntSqlConstant i)
            rf4.SetVal "title" (SqlConstant.newVarchar ("course" + i.ToString()))
            rf4.SetVal "deptid" (BigIntSqlConstant(int64 (i % 3 + 1) * 1000L))
        rf4.Close()

        tx.Commit()
