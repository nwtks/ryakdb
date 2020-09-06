namespace RyakDB.Test.Buffer.BufferPoolTest

open Xunit
open RyakDB.Storage
open RyakDB.Storage.File
open RyakDB.Storage.Log
open RyakDB.Buffer
open RyakDB.Buffer.BufferPool

module BufferPoolTest =
    [<Fact>]
    let ``concurrent buffer pool pin`` () =
        let bufferCount = 10

        let filename =
            FileManager.TmpFilePrefix
            + "_test_concurrent_buffer_pool_pin"

        let logfilename = "test_concurrent_buffer_pool_pin.log"

        let fileMgr =
            FileManager.newFileManager ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 true

        let logMgr =
            LogManager.newLogManager fileMgr logfilename

        let bufferPool =
            BufferPool.newBufferPool fileMgr logMgr bufferCount 1000

        let buffers =
            [ for i in 0 .. bufferCount - 1 ->
                let blockId = BlockId.newBlockId filename (int64 i)
                [ for _ in 0 .. 9 ->
                    async {
                        for _ in 0 .. 1000 do
                            bufferPool.Pin blockId
                            |> Option.get
                            |> bufferPool.Unpin
                        return bufferPool.Pin blockId |> Option.get
                    } ] ]
            |> List.collect id
            |> Async.Parallel
            |> Async.RunSynchronously

        for i in 0 .. bufferCount - 1 do
            let buff = buffers.[i * 10]
            for j in 0 .. 9 do
                Assert.Same(buff, buffers.[i * 10 + j])
