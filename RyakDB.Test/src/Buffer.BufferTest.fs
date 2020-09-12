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
        FileManager.TmpFilePrefix + "_test_buffer"

    let logfilename = "test_buffer.log"

    let fileMgr =
        newFileManager ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 true

    let logMgr = newLogManager fileMgr logfilename

    let bufferPool = newBufferPool fileMgr logMgr 10 1000

    let buffMgr = newTransactionBuffer bufferPool 12345L

    let blk = BlockId.newBlockId filename 13L

    let buff1 = buffMgr.Pin blk
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
    buffMgr.Unpin buff1

    let buff2 = buffMgr.Pin blk
    let mutable pos2 = 0
    while pos2 + 4 < buff2.BufferSize do
        let i = IntDbConstant(12345 + pos2)
        Assert.Equal(i, buff2.GetVal pos2 IntDbType)
        pos2 <- pos2 + 4

        let s =
            DbConstant.newVarchar ("value" + pos2.ToString())

        let slen = Page.size s
        if pos2 + slen < buff2.BufferSize
        then Assert.Equal(s, buff2.GetVal pos2 (VarcharDbType 0))
        pos2 <- pos2 + slen
    buffMgr.Unpin buff2

[<Fact>]
let ``last LSN`` () =
    let filename =
        FileManager.TmpFilePrefix + "_test_last_lsn"

    let logfilename = "test_last_lsn.log"

    let fileMgr =
        newFileManager ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 true

    let logMgr = newLogManager fileMgr logfilename

    let blk = BlockId.newBlockId filename 13L

    let buff = newBuffer fileMgr logMgr
    buff.AssignToBlock blk
    buff.SetVal 0 (IntDbConstant 123) (Some(newLogSeqNo 0L 0L))
    buff.SetVal 0 (DbConstant.newVarchar "abcde") (Some(newLogSeqNo 0L 1L))
    buff.SetVal 0 (BigIntDbConstant 986543210L) (Some(newLogSeqNo 0L 2L))
    buff.SetVal 0 (DoubleDbConstant 123.4567) (Some(newLogSeqNo 0L 3L))
    buff.SetVal 0 (DbConstant.newVarchar "") (Some(newLogSeqNo 0L 4L))
    Assert.Equal(newLogSeqNo 0L 4L, buff.LastLogSeqNo())

    buff.Flush()
    let buff2 = newBuffer fileMgr logMgr
    buff2.AssignToBlock blk
    Assert.Equal(newLogSeqNo 0L 4L, buff2.LastLogSeqNo())

[<Fact>]
let ``assign new buffer`` () =
    let filename =
        FileManager.TmpFilePrefix
        + "_test_assign_new_buffer"

    let logfilename = "test_assign_new_buffer.log"

    let fileMgr =
        newFileManager ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 true

    let logMgr = newLogManager fileMgr logfilename

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

    let buff = newBuffer fileMgr logMgr
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
    Assert.Equal(c1, c11)
    Assert.Equal(c2, c12)
    Assert.Equal(c3, c13)
    Assert.Equal(c4, c14)
    Assert.Equal(c5, c15)

[<Fact>]
let ``concurrent buffer pin`` () =
    let logfilename = "test_concurrent_buffer_pin.log"

    let fileMgr =
        newFileManager ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 true

    let logMgr = newLogManager fileMgr logfilename

    let buff = newBuffer fileMgr logMgr

    [ for _ in 0 .. 10 ->
        async {
            for __ in 0 .. 1000 do
                buff.Pin()
                buff.Unpin()
        } ]
    |> Async.Parallel
    |> Async.RunSynchronously
    |> ignore

    Assert.False(buff.IsPinned())
