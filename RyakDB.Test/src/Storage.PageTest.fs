module RyakDB.Test.Storage.PageTest

open Xunit
open FsUnit.Xunit
open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Storage.File
open RyakDB.Storage.Page

[<Fact>]
let ``write read append`` () =
    let filename = FileManager.TmpFilePrefix + "_test_wra"

    let fileMgr =
        newFileManager ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 false

    let blk1 = BlockId.newBlockId filename 0L
    let p1 = newPage fileMgr
    p1.SetVal 0 (IntDbConstant 1234)
    p1.SetVal 4 (BigIntDbConstant 567890L)
    p1.Write blk1

    let p2 = newPage fileMgr
    p2.Read blk1
    Assert.Equal(IntDbConstant 1234, p2.GetVal 0 IntDbType)
    Assert.Equal(BigIntDbConstant 567890L, p2.GetVal 4 BigIntDbType)
    Assert.Equal(1L, fileMgr.Size filename)

    let blk2 = p1.Append filename
    let p3 = newPage fileMgr
    p3.Read blk2
    Assert.Equal(IntDbConstant 1234, p3.GetVal 0 IntDbType)
    Assert.Equal(BigIntDbConstant 567890L, p3.GetVal 4 BigIntDbType)
    Assert.Equal(2L, fileMgr.Size filename)

[<Fact>]
let ``extend file`` () =
    let filename =
        FileManager.TmpFilePrefix + "_test_extend"

    let fileMgr =
        newFileManager ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 false

    let blk1 = BlockId.newBlockId filename 14L
    let p1 = Page.newPage fileMgr
    p1.Write blk1
    Assert.Equal(15L, fileMgr.Size filename)

[<Fact>]
let ``concurrent set`` () =
    let fileMgr =
        newFileManager ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 false

    let pg = newPage fileMgr

    [ for i in 0 .. 10 ->
        async {
            for _ in 0 .. 100 do
                pg.SetVal (i * 8) (BigIntDbConstant(int64 i))
        } ]
    |> Async.Parallel
    |> Async.RunSynchronously
    |> ignore
    for i in 0 .. 10 do
        Assert.Equal(BigIntDbConstant(int64 i), pg.GetVal (i * 8) BigIntDbType)

[<Fact>]
let ``concurrent get set`` () =
    let fileMgr =
        newFileManager ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 false

    let pg = newPage fileMgr

    for i in 0 .. 10 do
        pg.SetVal (i * 8) (BigIntDbConstant(int64 i))

    [ for i in 0 .. 10 ->
        [ async {
            for _ in 0 .. 100 do
                pg.SetVal (i * 8) (BigIntDbConstant(int64 i))
          }
          async {
              for _ in 0 .. 100 do
                  Assert.Equal(BigIntDbConstant(int64 i), pg.GetVal (i * 8) BigIntDbType)
          } ] ]
    |> List.collect id
    |> Async.Parallel
    |> Async.RunSynchronously
    |> ignore
