module RyakDB.Test.Storage.LogTest

open Xunit
open FsUnit.Xunit
open RyakDB.DataType
open RyakDB.Storage.File
open RyakDB.Storage.Log

[<Fact>]
let ``append read`` () =
    let logfilename = "test_log.log"

    let fileMgr =
        newFileManager ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 false

    let logMgr = newLogManager fileMgr logfilename

    logMgr.RemoveAndCreateNewLog()

    let lsn1 =
        logMgr.Append [ IntDbConstant 1
                        DbConstant.newVarchar "abc"
                        IntDbConstant -2 ]

    let lsn2 =
        logMgr.Append [ DbConstant.newVarchar "kool"
                        BigIntDbConstant 123457890123L ]

    let lsn3 = logMgr.Append [ DoubleDbConstant 3.3 ]

    logMgr.Flush lsn3

    let seq1 = logMgr.Records()
    Seq.head seq1
    |> (fun r ->
        r.LogSeqNo |> should equal lsn3
        r.NextVal DoubleDbType
        |> should equal (DoubleDbConstant 3.3))
    let seq2 = Seq.tail seq1
    Seq.head seq2
    |> (fun r ->
        r.LogSeqNo |> should equal lsn2
        r.NextVal(VarcharDbType 0)
        |> should equal (DbConstant.newVarchar "kool")
        r.NextVal BigIntDbType
        |> should equal (BigIntDbConstant 123457890123L))
    let seq3 = Seq.tail seq2
    Seq.head seq3
    |> (fun r ->
        r.LogSeqNo |> should equal lsn1
        r.NextVal IntDbType
        |> should equal (IntDbConstant 1)
        r.NextVal(VarcharDbType 0)
        |> should equal (DbConstant.newVarchar "abc")
        r.NextVal IntDbType
        |> should equal (IntDbConstant -2))
