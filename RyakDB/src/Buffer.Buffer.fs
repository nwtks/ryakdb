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
      AssignToBlock: BlockId -> unit
      AssignToNew: string -> BufferFormatter -> unit }

module Buffer =
    type BufferState =
        { BufferSize: int32
          Contents: Page
          BlockId: BlockId
          Pins: int32
          IsNew: bool
          IsModified: bool
          LastLogSeqNo: LogSeqNo
          LogMgr: LogManager }

    let DataStartOffset = LogSeqNo.Size

    let inline setValue state offset value =
        if 0 > offset || offset >= state.BufferSize
        then failwith ("Out of bounds:" + offset.ToString())

        state.Contents.SetVal (DataStartOffset + offset) value

    let inline getVal state offset dbType =
        if 0 > offset || offset >= state.BufferSize
        then failwith ("Out of bounds:" + offset.ToString())

        state.Contents.GetVal (DataStartOffset + offset) dbType

    let setVal state offset value lsn =
        if 0 > offset || offset >= state.BufferSize
        then failwith ("Out of bounds:" + offset.ToString())

        let newstate =
            { state with
                  IsModified = true
                  LastLogSeqNo =
                      match lsn with
                      | Some n when n > state.LastLogSeqNo -> n
                      | _ -> state.LastLogSeqNo }

        newstate.LastLogSeqNo
        |> LogSeqNo.writeToPage 0 newstate.Contents
        newstate.Contents.SetVal (DataStartOffset + offset) value
        newstate

    let inline pin state = { state with Pins = state.Pins + 1 }

    let inline unpin state = { state with Pins = state.Pins - 1 }

    let inline isPinned state = state.Pins > 0

    let inline flush state =
        if state.IsNew || state.IsModified then
            state.LogMgr.Flush state.LastLogSeqNo
            state.Contents.Write state.BlockId
            { state with
                  IsNew = false
                  IsModified = false }
        else
            state

    let inline assignToBlock state blockId =
        let newstate = flush state
        newstate.Contents.Read blockId
        { newstate with
              BlockId = blockId
              Pins = 0
              LastLogSeqNo = LogSeqNo.readFromPage 0 newstate.Contents }

    let inline assignToNew state buffer fileName formatter =
        let newstate = flush state
        formatter buffer
        { newstate with
              BlockId = newstate.Contents.Append fileName
              Pins = 0
              IsNew = true
              LastLogSeqNo = LogSeqNo.DefaltValue }

let newBuffer fileMgr logMgr =
    let bufferSize = fileMgr.BlockSize - LogSeqNo.Size

    let mutable state: Buffer.BufferState =
        { BufferSize = bufferSize
          Contents = newPage fileMgr
          BlockId = BlockId.newBlockId "" -1L
          Pins = 0
          IsNew = false
          IsModified = false
          LastLogSeqNo = LogSeqNo.DefaltValue
          LogMgr = logMgr }

    let mutable callbackBuffer = None
    let internalLock = obj ()

    let buffer =
        { BufferSize = bufferSize
          GetVal = Buffer.getVal state
          SetVal = fun offset value lsn -> lock internalLock (fun () -> state <- Buffer.setVal state offset value lsn)
          LastLogSeqNo = fun () -> state.LastLogSeqNo
          BlockId = fun () -> state.BlockId
          SetValue = fun offset value -> lock internalLock (fun () -> Buffer.setValue state offset value)
          Pin = fun () -> lock internalLock (fun () -> state <- Buffer.pin state)
          Unpin = fun () -> lock internalLock (fun () -> state <- Buffer.unpin state)
          IsPinned = fun () -> Buffer.isPinned state
          Flush = fun () -> lock internalLock (fun () -> state <- Buffer.flush state)
          AssignToBlock = fun blockId -> lock internalLock (fun () -> state <- Buffer.assignToBlock state blockId)
          AssignToNew =
              fun fileName formatter ->
                  lock internalLock (fun () ->
                      state <- Buffer.assignToNew state (callbackBuffer |> Option.get) fileName formatter) }

    callbackBuffer <- Some buffer
    buffer
