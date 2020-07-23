namespace RyakDB.Storage.BufferTest

open Xunit
open RyakDB.Sql.Type
open RyakDB.Storage.Type
open RyakDB.Storage.File
open RyakDB.Storage.Log
open RyakDB.Storage.Buffer

module BufferTest =
    [<Fact>]
    let ``set get`` () =
        let filename =
            FileManager.TmpFilePrefix + "_test_buffer"

        let logfilename = "test_buffer.log"

        let fileMgr =
            FileManager.newFileManager ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 true

        let logMgr =
            LogManager.newLogManager fileMgr logfilename

        let bufferPoolMgr =
            BufferPoolManager.newBufferPoolManager fileMgr logMgr 10 1000

        let (buffMgr, _) =
            BufferManager.newBufferManager bufferPoolMgr 12345L

        let blk = BlockId.newBlockId filename 13L

        let buff1 = buffMgr.Pin blk
        let mutable pos1 = 0
        while pos1 + 4 < buff1.BufferSize do
            let i = IntSqlConstant(12345 + pos1)
            buff1.SetVal pos1 i None
            pos1 <- pos1 + 4

            let s =
                SqlConstant.newVarchar ("value" + pos1.ToString())

            let slen = Page.size s
            if pos1 + slen < buff1.BufferSize then buff1.SetVal pos1 s None
            pos1 <- pos1 + slen
        buffMgr.Unpin buff1

        let buff2 = buffMgr.Pin blk
        let mutable pos2 = 0
        while pos2 + 4 < buff2.BufferSize do
            let i = IntSqlConstant(12345 + pos2)
            Assert.Equal(i, buff2.GetVal pos2 IntSqlType)
            pos2 <- pos2 + 4

            let s =
                SqlConstant.newVarchar ("value" + pos2.ToString())

            let slen = Page.size s
            if pos2 + slen < buff2.BufferSize
            then Assert.Equal(s, buff2.GetVal pos2 (VarcharSqlType 0))
            pos2 <- pos2 + slen
        buffMgr.Unpin buff2

    [<Fact>]
    let ``last LSN`` () =
        let filename =
            FileManager.TmpFilePrefix + "_test_last_lsn"

        let logfilename = "test_last_lsn.log"

        let fileMgr =
            FileManager.newFileManager ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 true

        let logMgr =
            LogManager.newLogManager fileMgr logfilename

        let blk = BlockId.newBlockId filename 13L

        let buff = Buffer.newBuffer fileMgr logMgr
        buff.AssignToBlock blk
        buff.SetVal 0 (IntSqlConstant 123) (Some(LogSeqNo.newLogSeqNo 0L 0L))
        buff.SetVal 0 (SqlConstant.newVarchar "abcde") (Some(LogSeqNo.newLogSeqNo 0L 1L))
        buff.SetVal 0 (BigIntSqlConstant 986543210L) (Some(LogSeqNo.newLogSeqNo 0L 2L))
        buff.SetVal 0 (DoubleSqlConstant 123.4567) (Some(LogSeqNo.newLogSeqNo 0L 3L))
        buff.SetVal 0 (SqlConstant.newVarchar "") (Some(LogSeqNo.newLogSeqNo 0L 4L))
        Assert.Equal(LogSeqNo.newLogSeqNo 0L 4L, buff.LastLsn())

        buff.Flush()
        let buff2 = Buffer.newBuffer fileMgr logMgr
        buff2.AssignToBlock blk
        Assert.Equal(LogSeqNo.newLogSeqNo 0L 4L, buff2.LastLsn())

    [<Fact>]
    let ``assign new buffer`` () =
        let filename =
            FileManager.TmpFilePrefix
            + "_test_assign_new_buffer"

        let logfilename = "test_assign_new_buffer.log"

        let fileMgr =
            FileManager.newFileManager ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 true

        let logMgr =
            LogManager.newLogManager fileMgr logfilename

        let c1 = IntSqlConstant 123
        let c2 = SqlConstant.newVarchar "abcde"
        let c3 = BigIntSqlConstant 986543210L
        let c4 = DoubleSqlConstant 123.4567
        let c5 = SqlConstant.newVarchar ""

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

        let buff = Buffer.newBuffer fileMgr logMgr
        buff.AssignToNew filename format

        let mutable offset = 0
        let c11 = buff.GetVal offset IntSqlType
        offset <- offset + Page.size c11
        let c12 = buff.GetVal offset (VarcharSqlType 0)
        offset <- offset + Page.size c12
        let c13 = buff.GetVal offset BigIntSqlType
        offset <- offset + Page.size c13
        let c14 = buff.GetVal offset DoubleSqlType
        offset <- offset + Page.size c14
        let c15 = buff.GetVal offset (VarcharSqlType 0)
        Assert.Equal(c1, c11)
        Assert.Equal(c2, c12)
        Assert.Equal(c3, c13)
        Assert.Equal(c4, c14)
        Assert.Equal(c5, c15)

    [<Fact>]
    let ``concurrent buffer pin`` () =
        let logfilename = "test_concurrent_buffer_pin.log"

        let fileMgr =
            FileManager.newFileManager ("test_dbs_" + System.DateTime.Now.Ticks.ToString()) 1024 true

        let logMgr =
            LogManager.newLogManager fileMgr logfilename

        let buff = Buffer.newBuffer fileMgr logMgr

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

module BufferManagerTest =
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

        let bufferPoolMgr =
            BufferPoolManager.newBufferPoolManager fileMgr logMgr 10 1000

        let (buffMgr, _) =
            BufferManager.newBufferManager bufferPoolMgr 12341L

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

        let bufferPoolMgr =
            BufferPoolManager.newBufferPoolManager fileMgr logMgr 5 3000

        let (buffMgr1, _) =
            BufferManager.newBufferManager bufferPoolMgr 1234562L

        let (buffMgr2, _) =
            BufferManager.newBufferManager bufferPoolMgr 1234563L

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

module BufferPoolManagerTest =
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

        let bufferPoolMgr =
            BufferPoolManager.newBufferPoolManager fileMgr logMgr bufferCount 1000

        let buffers =
            [ for i in 0 .. bufferCount - 1 ->
                let blockId = BlockId.newBlockId filename (int64 i)
                [ for _ in 0 .. 9 ->
                    async {
                        for _ in 0 .. 1000 do
                            bufferPoolMgr.Pin blockId
                            |> Option.get
                            |> bufferPoolMgr.Unpin
                        return bufferPoolMgr.Pin blockId |> Option.get
                    } ] ]
            |> List.collect id
            |> Async.Parallel
            |> Async.RunSynchronously

        for i in 0 .. bufferCount - 1 do
            let buff = buffers.[i * 10]
            for j in 0 .. 9 do
                Assert.Same(buff, buffers.[i * 10 + j])
