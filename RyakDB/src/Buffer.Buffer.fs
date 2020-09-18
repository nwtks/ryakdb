module RyakDB.Buffer.Buffer

open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Storage.File
open RyakDB.Storage.Page
open RyakDB.Storage.Log

type BufferFormatter = Buffer -> unit

and Buffer =
    { BufferSize: int32
      GetVal: int32 -> DbType -> DbConstant
      SetVal: int32 -> DbConstant -> LogSeqNo option -> unit
      LastLogSeqNo: unit -> LogSeqNo
      BlockId: unit -> BlockId
      SetValue: int32 -> DbConstant -> unit
      Pin: unit -> unit
      Unpin: unit -> unit
      IsPinned: unit -> bool
      Flush: unit -> unit
      LockFlushing: (unit -> unit) -> unit
      AssignToBlock: BlockId -> unit
      AssignToNew: string -> BufferFormatter -> unit }

module Buffer =
    type BufferState =
        { BlockId: BlockId
          Pins: int32
          IsNew: bool
          IsModified: bool
          LastLogSeqNo: LogSeqNo }

    let DataStartOffset = LogSeqNo.Size

    let inline setValue (page: Page) bufferSize offset value =
        if 0 > offset || offset >= bufferSize
        then failwith ("Out of bounds:" + offset.ToString())

        page.SetVal (DataStartOffset + offset) value

    let inline getVal (page: Page) bufferSize offset dbType =
        if 0 > offset || offset >= bufferSize
        then failwith ("Out of bounds:" + offset.ToString())

        page.GetVal (DataStartOffset + offset) dbType

    let setVal page bufferSize offset value lsn state =
        if 0 > offset || offset >= bufferSize
        then failwith ("Out of bounds:" + offset.ToString())

        let newstate =
            { state with
                  IsModified = true
                  LastLogSeqNo =
                      match lsn with
                      | Some n when n > state.LastLogSeqNo -> n
                      | _ -> state.LastLogSeqNo }

        newstate.LastLogSeqNo
        |> LogSeqNo.writeToPage 0 page
        page.SetVal (DataStartOffset + offset) value
        newstate

    let inline pin state = { state with Pins = state.Pins + 1 }

    let inline unpin state = { state with Pins = state.Pins - 1 }

    let inline isPinned state = state.Pins > 0

    let inline flush (logMgr: LogManager) page state =
        lock state (fun () ->
            if state.IsNew || state.IsModified then
                logMgr.Flush state.LastLogSeqNo
                page.Write state.BlockId
                { state with
                      IsNew = false
                      IsModified = false }
            else
                state)

    let inline assignToBlock logMgr page blockId state =
        let newstate = flush logMgr page state
        page.Read blockId
        { newstate with
              BlockId = blockId
              Pins = 0
              LastLogSeqNo = LogSeqNo.readFromPage 0 page }

    let inline assignToNew logMgr page buffer fileName formatter state =
        let newstate = flush logMgr page state
        formatter buffer
        { newstate with
              BlockId = page.Append fileName
              Pins = 0
              IsNew = true
              LastLogSeqNo = LogSeqNo.DefaltValue }

let newBuffer fileMgr logMgr =
    let mutable state: Buffer.BufferState =
        { BlockId = BlockId.newBlockId "" -1L
          Pins = 0
          IsNew = false
          IsModified = false
          LastLogSeqNo = LogSeqNo.DefaltValue }

    let page = newPage fileMgr
    let bufferSize = fileMgr.BlockSize - LogSeqNo.Size
    let internalLock = obj ()

    let rec buffer =
        { BufferSize = bufferSize
          GetVal = Buffer.getVal page bufferSize
          SetVal =
              fun offset value lsn ->
                  lock internalLock (fun () -> state <- Buffer.setVal page bufferSize offset value lsn state)
          LastLogSeqNo = fun () -> state.LastLogSeqNo
          BlockId = fun () -> state.BlockId
          SetValue = fun offset value -> lock internalLock (fun () -> Buffer.setValue page bufferSize offset value)
          Pin = fun () -> lock internalLock (fun () -> state <- Buffer.pin state)
          Unpin = fun () -> lock internalLock (fun () -> state <- Buffer.unpin state)
          IsPinned = fun () -> Buffer.isPinned state
          Flush = fun () -> lock internalLock (fun () -> state <- Buffer.flush logMgr page state)
          LockFlushing = fun fn -> lock state fn
          AssignToBlock =
              fun blockId -> lock internalLock (fun () -> state <- Buffer.assignToBlock logMgr page blockId state)
          AssignToNew =
              fun fileName formatter ->
                  lock internalLock (fun () -> state <- Buffer.assignToNew logMgr page buffer fileName formatter state) }

    buffer
