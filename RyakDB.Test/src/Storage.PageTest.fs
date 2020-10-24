module RyakDB.Test.Storage.PageTest

open Xunit
open FsUnit.Xunit
open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Storage.File
open RyakDB.Storage.Page

[<Fact>]
let ``write read append`` () =
    let filename = FileService.TmpFilePrefix + "_test_wra"

    let fileService =
        newFileService ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 false

    let blk1 = BlockId.newBlockId filename 0L
    let p1 = newPage fileService
    p1.SetVal 0 (IntDbConstant 1234)
    p1.SetVal 4 (BigIntDbConstant 567890L)
    p1.Write blk1

    let p2 = newPage fileService
    p2.Read blk1
    p2.GetVal 0 IntDbType
    |> should equal (IntDbConstant 1234)
    p2.GetVal 4 BigIntDbType
    |> should equal (BigIntDbConstant 567890L)
    fileService.Size filename |> should equal 1L

    let blk2 = p1.Append filename
    let p3 = newPage fileService
    p3.Read blk2
    p3.GetVal 0 IntDbType
    |> should equal (IntDbConstant 1234)
    p3.GetVal 4 BigIntDbType
    |> should equal (BigIntDbConstant 567890L)
    fileService.Size filename |> should equal 2L

[<Fact>]
let ``extend file`` () =
    let filename =
        FileService.TmpFilePrefix + "_test_extend"

    let fileService =
        newFileService ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 false

    let blk1 = BlockId.newBlockId filename 14L
    let p1 = newPage fileService
    p1.Write blk1
    fileService.Size filename |> should equal 15L

[<Fact>]
let ``concurrent set`` () =
    let fileService =
        newFileService ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 false

    let pg = newPage fileService

    [ 0 .. 10 ]
    |> List.map (fun i ->
        async {
            [ 0 .. 99 ]
            |> List.iter (fun _ -> pg.SetVal (i * 8) (int64 i |> BigIntDbConstant))
        })
    |> Async.Parallel
    |> Async.RunSynchronously
    |> ignore

    [ 0 .. 10 ]
    |> List.iter (fun i ->
        pg.GetVal (i * 8) BigIntDbType
        |> should equal (int64 i |> BigIntDbConstant))

[<Fact>]
let ``concurrent get set`` () =
    let fileService =
        newFileService ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 false

    let pg = newPage fileService

    [ 0 .. 10 ]
    |> List.iter (fun i -> pg.SetVal (i * 8) (int64 i |> BigIntDbConstant))

    [ 0 .. 10 ]
    |> List.collect (fun i ->
        [ async {
            [ 0 .. 99 ]
            |> List.iter (fun _ -> pg.SetVal (i * 8) (int64 i |> BigIntDbConstant))
          }
          async {
              [ 0 .. 99 ]
              |> List.iter (fun _ ->
                  pg.GetVal (i * 8) BigIntDbType
                  |> should equal (int64 i |> BigIntDbConstant))
          } ])
    |> Async.Parallel
    |> Async.RunSynchronously
    |> ignore
