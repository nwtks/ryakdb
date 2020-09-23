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
    p2.GetVal 0 IntDbType
    |> should equal (IntDbConstant 1234)
    p2.GetVal 4 BigIntDbType
    |> should equal (BigIntDbConstant 567890L)
    fileMgr.Size filename |> should equal 1L

    let blk2 = p1.Append filename
    let p3 = newPage fileMgr
    p3.Read blk2
    p3.GetVal 0 IntDbType
    |> should equal (IntDbConstant 1234)
    p3.GetVal 4 BigIntDbType
    |> should equal (BigIntDbConstant 567890L)
    fileMgr.Size filename |> should equal 2L

[<Fact>]
let ``extend file`` () =
    let filename =
        FileManager.TmpFilePrefix + "_test_extend"

    let fileMgr =
        newFileManager ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 false

    let blk1 = BlockId.newBlockId filename 14L
    let p1 = newPage fileMgr
    p1.Write blk1
    fileMgr.Size filename |> should equal 15L

[<Fact>]
let ``concurrent set`` () =
    let fileMgr =
        newFileManager ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 false

    let pg = newPage fileMgr

    [ for i in 0 .. 10 ->
        async {
            for _ in 0 .. 99 do
                pg.SetVal (i * 8) (int64 i |> BigIntDbConstant)
        } ]
    |> Async.Parallel
    |> Async.RunSynchronously
    |> ignore
    for i in 0 .. 10 do
        pg.GetVal (i * 8) BigIntDbType
        |> should equal (int64 i |> BigIntDbConstant)

[<Fact>]
let ``concurrent get set`` () =
    let fileMgr =
        newFileManager ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 false

    let pg = newPage fileMgr

    for i in 0 .. 10 do
        pg.SetVal (i * 8) (int64 i |> BigIntDbConstant)

    [ for i in 0 .. 10 ->
        [ async {
            for _ in 0 .. 99 do
                pg.SetVal (i * 8) (int64 i |> BigIntDbConstant)
          }
          async {
              for _ in 0 .. 99 do
                  pg.GetVal (i * 8) BigIntDbType
                  |> should equal (int64 i |> BigIntDbConstant)
          } ] ]
    |> List.collect id
    |> Async.Parallel
    |> Async.RunSynchronously
    |> ignore
