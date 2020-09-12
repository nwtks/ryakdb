module RyakDB.Storage.Log

open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Storage.File
open RyakDB.Storage.Page

type LogSeqNo = LogSeqNo of blockNo: int64 * offset: int64

type LogRecord =
    { LogSeqNo: LogSeqNo
      NextVal: DbType -> DbConstant }

type LogManager =
    { LogFile: string
      Records: unit -> LogRecord seq
      Append: DbConstant list -> LogSeqNo
      Flush: LogSeqNo -> unit
      RemoveAndCreateNewLog: unit -> unit }

module LogSeqNo =
    let BlockNoSize = 8
    let OffsetSize = 8
    let Size = BlockNoSize + OffsetSize

    let DefaltValue = LogSeqNo(-1L, -1L)

    let inline readFromPage position page =
        LogSeqNo
            (page.GetVal position BigIntDbType
             |> DbConstant.toLong,
             page.GetVal (position + BlockNoSize) BigIntDbType
             |> DbConstant.toLong)

    let inline writeToPage position page (LogSeqNo (blockNo, offset)) =
        BigIntDbConstant blockNo |> page.SetVal position
        BigIntDbConstant offset
        |> page.SetVal(position + BlockNoSize)

let inline newLogSeqNo blockNo offset = LogSeqNo(blockNo, offset)

module LogRecord =
    let inline nextVal dbType page position =
        let v = page.GetVal position dbType
        v, position + Page.size v

let newLogRecord blockNo offset page =
    let mutable position = offset

    { LogSeqNo = newLogSeqNo blockNo (int64 offset)
      NextVal =
          fun dbType ->
              let value, nextpos = LogRecord.nextVal dbType page position
              position <- nextpos
              value }

module LogManager =
    type LogManagerState =
        { LogFile: string
          LogPage: Page
          CurrentBlockId: BlockId
          CurrentPosition: int32
          LastLogSeqNo: LogSeqNo
          LastFlushedLogSeqNo: LogSeqNo }

    let LastPosition = 0
    let PointerSize = 4

    let inline getLastRecordPosition logPage =
        logPage.GetVal LastPosition IntDbType
        |> DbConstant.toInt

    let inline setLastRecordPosition logPage position =
        IntDbConstant position
        |> logPage.SetVal LastPosition

    let inline appendNewBlock state =
        setLastRecordPosition state.LogPage 0
        { state with
              CurrentPosition = PointerSize + PointerSize
              CurrentBlockId = state.LogPage.Append state.LogFile }

    let inline flushLog state =
        state.LogPage.Write state.CurrentBlockId
        { state with
              LastFlushedLogSeqNo = state.LastLogSeqNo }

    let records fileMgr state =
        let inline initBlock page state =
            let block = state.CurrentBlockId
            page.Read block

            let position =
                page.GetVal LastPosition IntDbType
                |> DbConstant.toInt

            block, position, state

        let inline nextBlock page (BlockId (fileName, blockNo)) =
            let block =
                BlockId.newBlockId fileName (blockNo - 1L)

            page.Read block

            let position =
                page.GetVal LastPosition IntDbType
                |> DbConstant.toInt

            block, position

        let page = newPage fileMgr
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
                o <- page.GetVal o1 IntDbType |> DbConstant.toInt
                yield newLogRecord blockNo (o + PointerSize + PointerSize) page
        }

    let append fileMgr state constants =
        let inline setPreviousNextRecordPosition logPage position =
            let lastpos = getLastRecordPosition logPage
            IntDbConstant position
            |> logPage.SetVal(lastpos + PointerSize)

        let inline setNextRecordPosition logPage position =
            IntDbConstant(LastPosition + PointerSize)
            |> logPage.SetVal position

        let inline finalizeRecord state =
            getLastRecordPosition state.LogPage
            |> IntDbConstant
            |> state.LogPage.SetVal state.CurrentPosition

            (state.CurrentPosition + PointerSize)
            |> setPreviousNextRecordPosition state.LogPage

            state.CurrentPosition
            |> setLastRecordPosition state.LogPage

            (state.CurrentPosition + PointerSize)
            |> setNextRecordPosition state.LogPage

            { state with
                  CurrentPosition = state.CurrentPosition + PointerSize + PointerSize }

        let inline appendVal state value =
            state.LogPage.SetVal state.CurrentPosition value
            { state with
                  CurrentPosition = state.CurrentPosition + (Page.size value) }

        let inline currentLogSeqNo state =
            let (BlockId (_, blockNo)) = state.CurrentBlockId
            newLogSeqNo blockNo (int64 state.CurrentPosition)

        let recsize =
            PointerSize
            + PointerSize
            + (constants |> List.sumBy Page.size)

        let nextstate =
            if state.CurrentPosition
               + recsize
               >= fileMgr.BlockSize then
                flushLog state |> appendNewBlock
            else
                state

        let lsn = currentLogSeqNo nextstate
        { (constants
           |> List.fold appendVal nextstate
           |> finalizeRecord) with
              LastLogSeqNo = lsn },
        lsn

    let inline flush state lsn =
        if lsn >= state.LastFlushedLogSeqNo then flushLog state else state

    let inline removeAndCreateNewLog fileMgr state =
        fileMgr.Delete state.LogFile
        appendNewBlock
            { state with
                  LastLogSeqNo = LogSeqNo.DefaltValue
                  LastFlushedLogSeqNo = LogSeqNo.DefaltValue }

let newLogManager fileMgr logFileName =
    let logsize = fileMgr.Size logFileName
    let logPage = newPage fileMgr

    let mutable state: LogManager.LogManagerState =
        if logsize > 0L then
            let currentBlockId =
                BlockId.newBlockId logFileName (logsize - 1L)

            logPage.Read currentBlockId

            { LogFile = logFileName
              LogPage = logPage
              CurrentBlockId = currentBlockId
              CurrentPosition =
                  LogManager.PointerSize
                  + LogManager.PointerSize
                  + LogManager.getLastRecordPosition logPage
              LastLogSeqNo = LogSeqNo.DefaltValue
              LastFlushedLogSeqNo = LogSeqNo.DefaltValue }
        else
            LogManager.appendNewBlock
                { LogFile = logFileName
                  LogPage = logPage
                  CurrentBlockId = BlockId.newBlockId logFileName 0L
                  CurrentPosition = LogManager.PointerSize + LogManager.PointerSize
                  LastLogSeqNo = LogSeqNo.DefaltValue
                  LastFlushedLogSeqNo = LogSeqNo.DefaltValue }

    { LogFile = logFileName
      Records =
          fun () ->
              lock logPage (fun () ->
                  let nextstate, recs = LogManager.records fileMgr state
                  state <- nextstate
                  recs)
      Append =
          fun constants ->
              lock logPage (fun () ->
                  let nextstate, lsn =
                      LogManager.append fileMgr state constants

                  state <- nextstate
                  lsn)
      Flush = fun lsn -> lock logPage (fun () -> state <- LogManager.flush state lsn)
      RemoveAndCreateNewLog = fun () -> lock logPage (fun () -> state <- LogManager.removeAndCreateNewLog fileMgr state) }
