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

let newLogRecord blockNo offset page =
    let mutable position = offset

    { LogSeqNo = newLogSeqNo blockNo (int64 offset)
      NextVal =
          fun dbType ->
              let value = page.GetVal position dbType
              position <- position + Page.size value
              value }

module LogManager =
    type LogManagerState =
        { CurrentBlockId: BlockId
          LastLogSeqNo: LogSeqNo
          LastFlushedLogSeqNo: LogSeqNo }

    let LastRecordPositionOffset = 0
    let PointerSize = 4

    let inline getLastRecordPosition page =
        page.GetVal LastRecordPositionOffset IntDbType
        |> DbConstant.toInt

    let inline setLastRecordPosition page position =
        IntDbConstant position
        |> page.SetVal LastRecordPositionOffset

    let inline appendNewBlock logFileName (page: Page) =
        setLastRecordPosition page 0
        page.Append logFileName

    let inline flushLog page state =
        page.Write state.CurrentBlockId
        { state with
              LastFlushedLogSeqNo = state.LastLogSeqNo }

    let records fileMgr logPage state =
        let inline readLastRecordPosition page blockId =
            page.Read blockId
            getLastRecordPosition page

        let inline prevBlockRecordPosition page (BlockId (fileName, blockNo)) =
            let prevBlockId =
                BlockId.newBlockId fileName (blockNo - 1L)

            prevBlockId, (readLastRecordPosition page prevBlockId)

        let nextstate = flushLog logPage state
        let readPage = newPage fileMgr
        let initBlockId = nextstate.CurrentBlockId

        let initPosition =
            readLastRecordPosition readPage initBlockId

        nextstate,
        seq {
            let mutable blockId = initBlockId
            let mutable (BlockId (_, blockNo)) = initBlockId
            let mutable position = initPosition
            while position > 0 || blockNo > 0L do
                let blockId1, position1 =
                    if position = 0 then prevBlockRecordPosition readPage blockId else blockId, position

                let (BlockId (_, blockNo1)) = blockId1
                blockId <- blockId1
                blockNo <- blockNo1
                position <-
                    readPage.GetVal position1 IntDbType
                    |> DbConstant.toInt
                yield newLogRecord blockNo (position + PointerSize) readPage
        }

    let append fileMgr logFileName logPage constants state =
        let inline appendVal page position value =
            page.SetVal position value
            position + (Page.size value)

        let inline finalizeRecord page prevLastRecordPosition nextLastRecordPosition =
            IntDbConstant prevLastRecordPosition
            |> page.SetVal nextLastRecordPosition
            setLastRecordPosition page nextLastRecordPosition

        let recordSize =
            (constants |> List.sumBy Page.size) + PointerSize

        let lastRecordPosition = getLastRecordPosition logPage

        let position, nextstate =
            if lastRecordPosition
               + PointerSize
               + recordSize
               >= fileMgr.BlockSize then
                0,
                { flushLog logPage state with
                      CurrentBlockId = appendNewBlock logFileName logPage }
            else
                lastRecordPosition, state

        constants
        |> List.fold (appendVal logPage) (position + PointerSize)
        |> finalizeRecord logPage position

        let (BlockId (_, blockNo)) = nextstate.CurrentBlockId
        { nextstate with
              LastLogSeqNo = newLogSeqNo blockNo (int64 (position + PointerSize)) }

    let inline flush logPage lsn state =
        if lsn >= state.LastFlushedLogSeqNo then flushLog logPage state else state

    let inline createNewLog logFileName logPage =
        { CurrentBlockId = appendNewBlock logFileName logPage
          LastLogSeqNo = LogSeqNo.DefaltValue
          LastFlushedLogSeqNo = LogSeqNo.DefaltValue }

let newLogManager fileMgr logFileName =
    let logsize = fileMgr.Size logFileName
    let logPage = newPage fileMgr

    let mutable state: LogManager.LogManagerState =
        if logsize > 0L then
            let blockId =
                BlockId.newBlockId logFileName (logsize - 1L)

            logPage.Read blockId

            { CurrentBlockId = blockId
              LastLogSeqNo = LogSeqNo.DefaltValue
              LastFlushedLogSeqNo = LogSeqNo.DefaltValue }
        else
            LogManager.createNewLog logFileName logPage

    { LogFile = logFileName
      Records =
          fun () ->
              lock logPage (fun () ->
                  let nextstate, recs = LogManager.records fileMgr logPage state
                  state <- nextstate
                  recs)
      Append =
          fun constants ->
              lock logPage (fun () ->
                  let nextstate =
                      LogManager.append fileMgr logFileName logPage constants state

                  state <- nextstate
                  state.LastLogSeqNo)
      Flush = fun lsn -> lock logPage (fun () -> state <- LogManager.flush logPage lsn state)
      RemoveAndCreateNewLog =
          fun () ->
              lock logPage (fun () ->
                  fileMgr.Delete logFileName
                  state <- LogManager.createNewLog logFileName logPage) }
