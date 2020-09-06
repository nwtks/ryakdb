namespace RyakDB.Test.Storage.PageTest

open Xunit
open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Storage.File
open RyakDB.Storage.Page

module PageTest =
    [<Fact>]
    let ``write read append`` () =
        let filename = FileManager.TmpFilePrefix + "_test_wra"

        let fileMgr =
            FileManager.newFileManager ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 false

        let blk1 = BlockId.newBlockId filename 0L
        let p1 = Page.newPage fileMgr
        p1.SetVal 0 (IntSqlConstant 1234)
        p1.SetVal 4 (BigIntSqlConstant 567890L)
        p1.Write blk1

        let p2 = Page.newPage fileMgr
        p2.Read blk1
        Assert.Equal(IntSqlConstant 1234, p2.GetVal 0 IntSqlType)
        Assert.Equal(BigIntSqlConstant 567890L, p2.GetVal 4 BigIntSqlType)
        Assert.Equal(1L, fileMgr.Size filename)

        let blk2 = p1.Append filename
        let p3 = Page.newPage fileMgr
        p3.Read blk2
        Assert.Equal(IntSqlConstant 1234, p3.GetVal 0 IntSqlType)
        Assert.Equal(BigIntSqlConstant 567890L, p3.GetVal 4 BigIntSqlType)
        Assert.Equal(2L, fileMgr.Size filename)

    [<Fact>]
    let ``extend file`` () =
        let filename =
            FileManager.TmpFilePrefix + "_test_extend"

        let fileMgr =
            FileManager.newFileManager ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 false

        let blk1 = BlockId.newBlockId filename 14L
        let p1 = Page.newPage fileMgr
        p1.Write blk1
        Assert.Equal(15L, fileMgr.Size filename)

    [<Fact>]
    let ``concurrent set`` () =
        let fileMgr =
            FileManager.newFileManager ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 false

        let pg = Page.newPage fileMgr

        [ for i in 0 .. 10 ->
            async {
                for _ in 0 .. 100 do
                    pg.SetVal (i * 8) (BigIntSqlConstant(int64 i))
            } ]
        |> Async.Parallel
        |> Async.RunSynchronously
        |> ignore
        for i in 0 .. 10 do
            Assert.Equal(BigIntSqlConstant(int64 i), pg.GetVal (i * 8) BigIntSqlType)

    [<Fact>]
    let ``concurrent get set`` () =
        let fileMgr =
            FileManager.newFileManager ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 false

        let pg = Page.newPage fileMgr

        for i in 0 .. 10 do
            pg.SetVal (i * 8) (BigIntSqlConstant(int64 i))

        [ for i in 0 .. 10 ->
            [ async {
                for _ in 0 .. 100 do
                    pg.SetVal (i * 8) (BigIntSqlConstant(int64 i))
              }
              async {
                  for _ in 0 .. 100 do
                      Assert.Equal(BigIntSqlConstant(int64 i), pg.GetVal (i * 8) BigIntSqlType)
              } ] ]
        |> List.collect id
        |> Async.Parallel
        |> Async.RunSynchronously
        |> ignore
