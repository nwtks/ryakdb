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

    let fileMgr =
        newFileManager ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 true

    let logMgr =
        newLogManager fileMgr "test_available_buffers.log"

    let bufferPool = newBufferPool fileMgr logMgr 10 1000

    let txBuffer = newTransactionBuffer bufferPool

    let a1 = bufferPool.Available()

    let blk1 = BlockId.newBlockId filename 1L
    let buff1 = txBuffer.Pin blk1
    let a2 = bufferPool.Available()

    let blk2 = BlockId.newBlockId filename 2L
    let buff2 = txBuffer.Pin blk2
    let a3 = bufferPool.Available()

    let blk3 = BlockId.newBlockId filename 3L
    let buff3 = txBuffer.Pin blk3
    let a4 = bufferPool.Available()

    txBuffer.Unpin buff1
    let a5 = bufferPool.Available()

    txBuffer.Unpin buff3
    let a6 = bufferPool.Available()

    txBuffer.Unpin buff2
    let a7 = bufferPool.Available()

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

    let fileMgr =
        newFileManager ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 true

    let logMgr =
        newLogManager fileMgr "test_concurrent_buffer_manager_pin.log"

    let bufferPool = newBufferPool fileMgr logMgr 5 3000

    let txBuffer1 = newTransactionBuffer bufferPool

    let txBuffer2 = newTransactionBuffer bufferPool

    let result = System.Text.StringBuilder(200)
    result.Append "start0." |> ignore
    BlockId.newBlockId filename 0L
    |> txBuffer1.Pin
    |> ignore
    result.Append "end0." |> ignore
    result.Append "start1." |> ignore
    BlockId.newBlockId filename 1L
    |> txBuffer1.Pin
    |> ignore
    result.Append "end1." |> ignore
    result.Append "start2." |> ignore
    BlockId.newBlockId filename 2L
    |> txBuffer1.Pin
    |> ignore
    result.Append "end2." |> ignore
    result.Append "start3." |> ignore
    BlockId.newBlockId filename 3L
    |> txBuffer1.Pin
    |> ignore
    result.Append "end3." |> ignore

    result.Append "start4." |> ignore
    BlockId.newBlockId filename 4L
    |> txBuffer2.Pin
    |> ignore
    result.Append "end4." |> ignore

    [ async {
        result.Append "start5." |> ignore
        BlockId.newBlockId filename 5L
        |> txBuffer1.Pin
        |> ignore
        result.Append "end5." |> ignore
        txBuffer1.UnpinAll()
      }
      async {
          do! Async.Sleep 1000
          result.Append "start6." |> ignore
          BlockId.newBlockId filename 6L
          |> txBuffer2.Pin
          |> ignore
          result.Append "end6." |> ignore
          txBuffer2.UnpinAll()
      } ]
    |> Async.Parallel
    |> Async.RunSynchronously
    |> ignore

    result.ToString()
    |> should equal "start0.end0.start1.end1.start2.end2.start3.end3.start4.end4.start5.start6.end6.end5."
