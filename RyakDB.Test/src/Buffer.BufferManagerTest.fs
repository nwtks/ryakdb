module RyakDB.Test.Buffer.BufferManagerTest

open Xunit
open RyakDB.Storage
open RyakDB.Storage.File
open RyakDB.Storage.Log
open RyakDB.Buffer
open RyakDB.Buffer.BufferPool
open RyakDB.Buffer.BufferManager

[<Fact>]
let ``available buffers`` () =
    let filename =
        FileManager.TmpFilePrefix
        + "_test_available_buffers"

    let logfilename = "test_available_buffers.log"

    let fileMgr =
        FileManager.newFileManager ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 true

    let logMgr =
        LogManager.newLogManager fileMgr logfilename

    let bufferPool =
        BufferPool.newBufferPool fileMgr logMgr 10 1000

    let buffMgr =
        BufferManager.newBufferManager bufferPool 12341L

    let a1 = buffMgr.Available()

    let blk1 = BlockId.newBlockId filename 1L
    let buff1 = buffMgr.Pin blk1
    let a2 = buffMgr.Available()

    let blk2 = BlockId.newBlockId filename 2L
    let buff2 = buffMgr.Pin blk2
    let a3 = buffMgr.Available()

    let blk3 = BlockId.newBlockId filename 3L
    let buff3 = buffMgr.Pin blk3
    let a4 = buffMgr.Available()

    buffMgr.Unpin buff1
    let a5 = buffMgr.Available()

    buffMgr.Unpin buff3
    let a6 = buffMgr.Available()

    buffMgr.Unpin buff2
    let a7 = buffMgr.Available()

    Assert.Equal(a1 - 1, a2)
    Assert.Equal(a1 - 2, a3)
    Assert.Equal(a1 - 3, a4)
    Assert.Equal(a1 - 2, a5)
    Assert.Equal(a1 - 1, a6)
    Assert.Equal(a1, a7)

[<Fact>]
let ``concurrent buffer manager pin`` () =
    let filename =
        FileManager.TmpFilePrefix
        + "_test_concurrent_buffer_manager_pin"

    let logfilename = "test_concurrent_buffer_manager_pin.log"

    let fileMgr =
        FileManager.newFileManager ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 true

    let logMgr =
        LogManager.newLogManager fileMgr logfilename

    let bufferPool =
        BufferPool.newBufferPool fileMgr logMgr 5 3000

    let buffMgr1 =
        BufferManager.newBufferManager bufferPool 1234562L

    let buffMgr2 =
        BufferManager.newBufferManager bufferPool 1234563L

    let result = System.Text.StringBuilder(200)
    result.Append "start0." |> ignore
    BlockId.newBlockId filename 0L
    |> buffMgr1.Pin
    |> ignore
    result.Append "end0." |> ignore
    result.Append "start1." |> ignore
    BlockId.newBlockId filename 1L
    |> buffMgr1.Pin
    |> ignore
    result.Append "end1." |> ignore
    result.Append "start2." |> ignore
    BlockId.newBlockId filename 2L
    |> buffMgr1.Pin
    |> ignore
    result.Append "end2." |> ignore
    result.Append "start3." |> ignore
    BlockId.newBlockId filename 3L
    |> buffMgr1.Pin
    |> ignore
    result.Append "end3." |> ignore

    result.Append "start4." |> ignore
    BlockId.newBlockId filename 4L
    |> buffMgr2.Pin
    |> ignore
    result.Append "end4." |> ignore

    [ async {
        result.Append "start5." |> ignore
        BlockId.newBlockId filename 5L
        |> buffMgr1.Pin
        |> ignore
        result.Append "end5." |> ignore
        buffMgr1.UnpinAll()
      }
      async {
          do! Async.Sleep 1000
          result.Append "start6." |> ignore
          BlockId.newBlockId filename 6L
          |> buffMgr2.Pin
          |> ignore
          result.Append "end6." |> ignore
          buffMgr2.UnpinAll()
      } ]
    |> Async.Parallel
    |> Async.RunSynchronously
    |> ignore

    Assert.Equal
        ("start0.end0.start1.end1.start2.end2.start3.end3.start4.end4.start5.start6.end6.end5.", result.ToString())
