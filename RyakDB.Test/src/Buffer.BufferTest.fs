module RyakDB.Test.Buffer.BufferTest

open Xunit
open FsUnit.Xunit
open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Storage.File
open RyakDB.Storage.Page
open RyakDB.Storage.Log
open RyakDB.Buffer.Buffer
open RyakDB.Buffer.BufferPool
open RyakDB.Buffer.TransactionBuffer

[<Fact>]
let ``set get`` () =
    let filename =
        FileService.TmpFilePrefix + "_test_buffer"

    let fileService =
        newFileService ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 true

    let logService =
        newLogService fileService "test_buffer.log"

    let bufferPool =
        newBufferPool fileService logService 10 1000

    let txBuffer = newTransactionBuffer bufferPool

    let blk = BlockId.newBlockId filename 13L

    let buff1 = txBuffer.Pin blk
    let mutable pos1 = 0
    while pos1 + 4 < buff1.BufferSize do
        let i = IntDbConstant(12345 + pos1)
        buff1.SetVal pos1 i None
        pos1 <- pos1 + 4

        let s =
            DbConstant.newVarchar ("value" + pos1.ToString())

        let slen = Page.size s
        if pos1 + slen < buff1.BufferSize then buff1.SetVal pos1 s None
        pos1 <- pos1 + slen
    txBuffer.Unpin buff1

    let buff2 = txBuffer.Pin blk
    let mutable pos2 = 0
    while pos2 + 4 < buff2.BufferSize do
        let i = IntDbConstant(12345 + pos2)
        buff2.GetVal pos2 IntDbType |> should equal i
        pos2 <- pos2 + 4

        let s =
            DbConstant.newVarchar ("value" + pos2.ToString())

        let slen = Page.size s
        if pos2 + slen < buff2.BufferSize then
            buff2.GetVal pos2 (VarcharDbType 0)
            |> should equal s
        pos2 <- pos2 + slen
    txBuffer.Unpin buff2

[<Fact>]
let ``last LSN`` () =
    let filename =
        FileService.TmpFilePrefix + "_test_last_lsn"

    let fileService =
        newFileService ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 true

    let logService =
        newLogService fileService "test_last_lsn.log"

    let blk = BlockId.newBlockId filename 13L

    let buff = newBuffer fileService logService
    buff.AssignToBlock blk
    buff.SetVal 0 (IntDbConstant 123) (Some(newLogSeqNo 0L 0L))
    buff.SetVal 0 (DbConstant.newVarchar "abcde") (Some(newLogSeqNo 0L 1L))
    buff.SetVal 0 (BigIntDbConstant 986543210L) (Some(newLogSeqNo 0L 2L))
    buff.SetVal 0 (DoubleDbConstant 123.4567) (Some(newLogSeqNo 0L 3L))
    buff.SetVal 0 (DbConstant.newVarchar "") (Some(newLogSeqNo 0L 4L))
    buff.LastLogSeqNo()
    |> should equal (newLogSeqNo 0L 4L)

    buff.Flush()
    let buff2 = newBuffer fileService logService
    buff2.AssignToBlock blk
    buff2.LastLogSeqNo()
    |> should equal (newLogSeqNo 0L 4L)

[<Fact>]
let ``assign new buffer`` () =
    let filename =
        FileService.TmpFilePrefix
        + "_test_assign_new_buffer"

    let fileService =
        newFileService ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 true

    let logService =
        newLogService fileService "test_assign_new_buffer.log"

    let c1 = IntDbConstant 123
    let c2 = DbConstant.newVarchar "abcde"
    let c3 = BigIntDbConstant 986543210L
    let c4 = DoubleDbConstant 123.4567
    let c5 = DbConstant.newVarchar ""

    let format (buff: Buffer) =
        let mutable offset = 0
        buff.SetValue offset c1
        offset <- offset + Page.size c1
        buff.SetValue offset c2
        offset <- offset + Page.size c2
        buff.SetValue offset c3
        offset <- offset + Page.size c3
        buff.SetValue offset c4
        offset <- offset + Page.size c4
        buff.SetValue offset c5

    let buff = newBuffer fileService logService
    buff.AssignToNew filename format

    let mutable offset = 0
    let c11 = buff.GetVal offset IntDbType
    offset <- offset + Page.size c11
    let c12 = buff.GetVal offset (VarcharDbType 0)
    offset <- offset + Page.size c12
    let c13 = buff.GetVal offset BigIntDbType
    offset <- offset + Page.size c13
    let c14 = buff.GetVal offset DoubleDbType
    offset <- offset + Page.size c14
    let c15 = buff.GetVal offset (VarcharDbType 0)
    c11 |> should equal c1
    c12 |> should equal c2
    c13 |> should equal c3
    c14 |> should equal c4
    c15 |> should equal c5

[<Fact>]
let ``concurrent buffer pin`` () =
    let fileService =
        newFileService ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 true

    let logService =
        newLogService fileService "test_concurrent_buffer_pin.log"

    let buff = newBuffer fileService logService

    [ for _ in 0 .. 10 ->
        async {
            for __ in 0 .. 999 do
                buff.Pin()
                buff.Unpin()
        } ]
    |> Async.Parallel
    |> Async.RunSynchronously
    |> ignore

    buff.IsPinned() |> should be False
