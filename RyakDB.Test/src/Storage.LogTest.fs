namespace RyakDB.Test.Storage.LogTest

open Xunit
open RyakDB.DataType
open RyakDB.Storage.File
open RyakDB.Storage.Log

module LogManagerTest =
    [<Fact>]
    let ``append read`` () =
        let logfilename = "test_log.log"

        let fileMgr =
            FileManager.newFileManager ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 false

        let logMgr =
            LogManager.newLogManager fileMgr logfilename

        logMgr.RemoveAndCreateNewLog()

        let lsn1 =
            logMgr.Append
                [ IntSqlConstant(1)
                  SqlConstant.newVarchar "abc"
                  IntSqlConstant(-2) ]

        let lsn2 =
            logMgr.Append
                [ SqlConstant.newVarchar "kool"
                  BigIntSqlConstant(123457890123L) ]

        let lsn3 = logMgr.Append [ DoubleSqlConstant(3.3) ]
        logMgr.Flush lsn3

        let seq1 = logMgr.Records()
        Seq.head seq1
        |> (fun r ->
            Assert.Equal(lsn3, r.Lsn)
            Assert.Equal(DoubleSqlConstant(3.3), r.NextVal DoubleSqlType))
        let seq2 = Seq.tail seq1
        Seq.head seq2
        |> (fun r ->
            Assert.Equal(lsn2, r.Lsn)
            Assert.Equal(SqlConstant.newVarchar "kool", r.NextVal(VarcharSqlType 0))
            Assert.Equal(BigIntSqlConstant(123457890123L), r.NextVal BigIntSqlType))
        let seq3 = Seq.tail seq2
        Seq.head seq3
        |> (fun r ->
            Assert.Equal(lsn1, r.Lsn)
            Assert.Equal(IntSqlConstant(1), r.NextVal IntSqlType)
            Assert.Equal(SqlConstant.newVarchar "abc", r.NextVal(VarcharSqlType 0))
            Assert.Equal(IntSqlConstant(-2), r.NextVal IntSqlType))
