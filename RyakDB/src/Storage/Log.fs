namespace RyakDB.Storage.Log

open RyakDB.Sql.Type
open RyakDB.Storage.File

type LogSeqNo = LogSeqNo of blockNo: int64 * offset: int64

type BasicLogRecord =
    { Lsn: LogSeqNo
      NextVal: SqlType -> SqlConstant }

type LogManager =
    { LogFile: string
      Records: unit -> BasicLogRecord seq
      Append: SqlConstant list -> LogSeqNo
      Flush: LogSeqNo -> unit
      RemoveAndCreateNewLog: unit -> unit }

module LogSeqNo =
    let Size = 8 + 8

    let DefaltValue: LogSeqNo = LogSeqNo(-1L, -1L)

    let readFromPage pos (page: Page) =
        LogSeqNo
            (page.GetVal pos BigIntSqlType
             |> SqlConstant.toLong,
             page.GetVal (pos + 8) BigIntSqlType
             |> SqlConstant.toLong)

    let writeToPage pos (page: Page) lsn =
        let (LogSeqNo (blockNo, offset)) = lsn
        BigIntSqlConstant blockNo |> page.SetVal pos
        BigIntSqlConstant offset |> page.SetVal(pos + 8)

    let newLogSeqNo blockNo offset = LogSeqNo(blockNo, offset)

module BasicLogRecord =
    let nextVal sqlType page position =
        let value = page.GetVal position sqlType
        value, position + Page.size value

    let newBasicLogRecord blockNo offset (page: Page) =
        let mutable current = offset

        { Lsn = LogSeqNo.newLogSeqNo blockNo (int64 offset)
          NextVal =
              fun sqlType ->
                  let value, nextpos = nextVal sqlType page current
                  current <- nextpos
                  value }

module LogManager =
    type LogManagerState =
        { LogFile: string
          LogPage: Page
          CurrentBlock: BlockId
          CurrentPos: int32
          LastLsn: LogSeqNo
          LastFlushedLsn: LogSeqNo }

    let LastPosition = 0
    let PointerSize = 4

    let getLastRecordPosition logPage =
        logPage.GetVal LastPosition IntSqlType
        |> SqlConstant.toInt

    let setLastRecordPosition logPage pos =
        IntSqlConstant pos |> logPage.SetVal LastPosition

    let appendNewBlock (state: LogManagerState) =
        setLastRecordPosition state.LogPage 0
        { state with
              CurrentPos = PointerSize + PointerSize
              CurrentBlock = state.LogPage.Append state.LogFile }

    let flushLog (state: LogManagerState) =
        state.LogPage.Write state.CurrentBlock
        { state with
              LastFlushedLsn = state.LastLsn }

    let records (fileMgr: FileManager) (state: LogManagerState) =
        let initBlock (page: Page) state =
            let block = state.CurrentBlock
            page.Read block

            let position =
                page.GetVal LastPosition IntSqlType
                |> SqlConstant.toInt

            block, position, state

        let nextBlock (page: Page) prevBlock =
            let (BlockId (fileName, blockNo)) = prevBlock

            let block =
                BlockId.newBlockId fileName (blockNo - 1L)

            page.Read block

            let position =
                page.GetVal LastPosition IntSqlType
                |> SqlConstant.toInt

            block, position

        let page = Page.newPage fileMgr
        let block, offset, nextstate = flushLog state |> initBlock page
        nextstate,
        seq {
            let mutable b = block
            let mutable (BlockId (_, blockNo)) = b
            let mutable o = offset
            while o > 0 || blockNo > 0L do
                let b1, o1 = if o = 0 then nextBlock page b else b, o
                let (BlockId (_, blockNo1)) = b1
                b <- b1
                blockNo <- blockNo1
                o <- page.GetVal o1 IntSqlType |> SqlConstant.toInt
                yield BasicLogRecord.newBasicLogRecord blockNo (o + PointerSize + PointerSize) page
        }

    let append (fileMgr: FileManager) (state: LogManagerState) constants =
        let setPreviousNextRecordPosition logPage pos =
            let lastpos = getLastRecordPosition logPage
            IntSqlConstant pos
            |> logPage.SetVal(lastpos + PointerSize)

        let setNextRecordPosition logPage pos =
            IntSqlConstant(LastPosition + PointerSize)
            |> logPage.SetVal pos

        let finalizeRecord state =
            getLastRecordPosition state.LogPage
            |> IntSqlConstant
            |> state.LogPage.SetVal state.CurrentPos

            (state.CurrentPos + PointerSize)
            |> setPreviousNextRecordPosition state.LogPage

            state.CurrentPos
            |> setLastRecordPosition state.LogPage

            (state.CurrentPos + PointerSize)
            |> setNextRecordPosition state.LogPage

            { state with
                  CurrentPos = state.CurrentPos + PointerSize + PointerSize }

        let appendVal state value =
            state.LogPage.SetVal state.CurrentPos value
            { state with
                  CurrentPos = state.CurrentPos + (Page.size value) }

        let currentLSN state =
            let (BlockId (_, blockNo)) = state.CurrentBlock
            LogSeqNo.newLogSeqNo blockNo (int64 state.CurrentPos)

        let recsize =
            PointerSize
            + PointerSize
            + (constants |> List.sumBy Page.size)

        let nextstate =
            if state.CurrentPos + recsize >= fileMgr.BlockSize
            then flushLog state |> appendNewBlock
            else state

        let lsn = currentLSN nextstate
        { (constants
           |> List.fold appendVal nextstate
           |> finalizeRecord) with
              LastLsn = lsn },
        lsn

    let flush (state: LogManagerState) lsn =
        if lsn >= state.LastFlushedLsn then flushLog state else state

    let removeAndCreateNewLog (fileMgr: FileManager) (state: LogManagerState) =
        fileMgr.Delete state.LogFile
        appendNewBlock
            { state with
                  LastLsn = LogSeqNo.DefaltValue
                  LastFlushedLsn = LogSeqNo.DefaltValue }

    let newLogManager (fileMgr: FileManager) logFileName =
        let logsize = fileMgr.Size logFileName
        let logPage = Page.newPage fileMgr

        let mutable state =
            if logsize > 0L then
                let currentBlock =
                    BlockId.newBlockId logFileName (logsize - 1L)

                logPage.Read currentBlock
                { LogFile = logFileName
                  LogPage = logPage
                  CurrentBlock = currentBlock
                  CurrentPos =
                      PointerSize
                      + PointerSize
                      + getLastRecordPosition logPage
                  LastLsn = LogSeqNo.DefaltValue
                  LastFlushedLsn = LogSeqNo.DefaltValue }
            else
                let currentBlock = BlockId.newBlockId logFileName 0L
                appendNewBlock
                    { LogFile = logFileName
                      LogPage = logPage
                      CurrentBlock = currentBlock
                      CurrentPos = PointerSize + PointerSize
                      LastLsn = LogSeqNo.DefaltValue
                      LastFlushedLsn = LogSeqNo.DefaltValue }

        { LogFile = logFileName
          Records =
              fun () ->
                  lock logPage (fun () ->
                      let nextstate, recs = records fileMgr state
                      state <- nextstate
                      recs)
          Append =
              fun list ->
                  lock logPage (fun () ->
                      let nextstate, lsn = append fileMgr state list
                      state <- nextstate
                      lsn)
          Flush = fun lsn -> lock logPage (fun () -> state <- flush state lsn)
          RemoveAndCreateNewLog = fun () -> lock logPage (fun () -> state <- removeAndCreateNewLog fileMgr state) }
