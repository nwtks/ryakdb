module RyakDB.Test.Buffer.TransactionBufferTest

open Xunit
open FsUnit.Xunit
open RyakDB.Storage
open RyakDB.Storage.File
open RyakDB.Storage.Log
open RyakDB.Buffer.BufferPool
open RyakDB.Buffer.TransactionBuffer

[<Fact>]
let ``available buffers`` () =
    let filename =
        FileManager.TmpFilePrefix
        + "_test_available_buffers"

    let logfilename = "test_available_buffers.log"

    let fileMgr =
        newFileManager ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 true

    let logMgr = newLogManager fileMgr logfilename

    let bufferPool = newBufferPool fileMgr logMgr 10 1000

    let buffMgr = newTransactionBuffer bufferPool

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

    a2 |> should equal (a1 - 1)
    a3 |> should equal (a1 - 2)
    a4 |> should equal (a1 - 3)
    a5 |> should equal (a1 - 2)
    a6 |> should equal (a1 - 1)
    a7 |> should equal a1

[<Fact>]
let ``concurrent buffer manager pin`` () =
    let filename =
        FileManager.TmpFilePrefix
        + "_test_concurrent_buffer_manager_pin"

    let logfilename = "test_concurrent_buffer_manager_pin.log"

    let fileMgr =
        newFileManager ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 true

    let logMgr = newLogManager fileMgr logfilename

    let bufferPool = newBufferPool fileMgr logMgr 5 3000

    let buffMgr1 = newTransactionBuffer bufferPool

    let buffMgr2 = newTransactionBuffer bufferPool

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

    result.ToString()
    |> should equal "start0.end0.start1.end1.start2.end2.start3.end3.start4.end4.start5.start6.end6.end5."
