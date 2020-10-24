module RyakDB.Test.Buffer.BufferPoolTest

open Xunit
open FsUnit.Xunit
open RyakDB.Storage
open RyakDB.Storage.File
open RyakDB.Storage.Log
open RyakDB.Buffer.BufferPool

[<Fact>]
let ``concurrent buffer pool pin`` () =
    let bufferCount = 10

    let filename =
        FileService.TmpFilePrefix
        + "_test_concurrent_buffer_pool_pin"

    let fileService =
        newFileService ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 true

    let logService =
        newLogService fileService "test_concurrent_buffer_pool_pin.log"

    let bufferPool =
        newBufferPool fileService logService bufferCount 1000

    let buffers =
        [ 0 .. bufferCount - 1 ]
        |> List.collect (fun i ->
            let blockId = BlockId.newBlockId filename (int64 i)
            [ 0 .. 9 ]
            |> List.map (fun _ ->
                async {
                    [ 0 .. 999 ]
                    |> List.iter (fun _ ->
                        bufferPool.Pin blockId
                        |> Option.get
                        |> bufferPool.Unpin)

                    return bufferPool.Pin blockId |> Option.get
                }))
        |> Async.Parallel
        |> Async.RunSynchronously

    [ 0 .. bufferCount - 1 ]
    |> List.iter (fun i ->
        let buff = buffers.[i * 10]
        [ 0 .. 9 ]
        |> List.iter (fun j -> buffers.[i * 10 + j] |> should sameAs buff))
