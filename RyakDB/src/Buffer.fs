namespace RyakDB.Buffer

open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Storage.File
open RyakDB.Storage.Page
open RyakDB.Storage.Log

type BufferFormatter = Buffer -> unit

and Buffer =
    { BufferSize: int32
      GetVal: int32 -> SqlType -> SqlConstant
      SetVal: int32 -> SqlConstant -> LogSeqNo option -> unit
      LastLsn: unit -> LogSeqNo
      BlockId: unit -> BlockId
      SetValue: int32 -> SqlConstant -> unit
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
          LastLsn: LogSeqNo
          LogMgr: LogManager }

    let DataStartOffset = LogSeqNo.Size

    let setValue state offset value =
        if 0 > offset || offset >= state.BufferSize
        then failwith ("Out of bounds:" + offset.ToString())

        state.Contents.SetVal (DataStartOffset + offset) value

    let getVal state offset sqlType =
        if 0 > offset || offset >= state.BufferSize
        then failwith ("Out of bounds:" + offset.ToString())

        state.Contents.GetVal (DataStartOffset + offset) sqlType

    let setVal state offset value lsn =
        if 0 > offset || offset >= state.BufferSize
        then failwith ("Out of bounds:" + offset.ToString())

        let newstate =
            { state with
                  IsModified = true
                  LastLsn =
                      match lsn with
                      | Some n when n > state.LastLsn -> n
                      | _ -> state.LastLsn }

        newstate.LastLsn
        |> LogSeqNo.writeToPage 0 newstate.Contents
        newstate.Contents.SetVal (DataStartOffset + offset) value
        newstate

    let lastLsn state = state.LastLsn

    let blockId state = state.BlockId

    let pin state = { state with Pins = state.Pins + 1 }

    let unpin state = { state with Pins = state.Pins - 1 }

    let isPinned state = state.Pins > 0

    let flush state =
        if state.IsNew || state.IsModified then
            state.LogMgr.Flush state.LastLsn
            state.Contents.Write state.BlockId
            { state with
                  IsNew = false
                  IsModified = false }
        else
            state

    let assignToBlock state blockId =
        let newstate = flush state
        newstate.Contents.Read blockId
        { newstate with
              BlockId = blockId
              Pins = 0
              LastLsn = LogSeqNo.readFromPage 0 newstate.Contents }

    let assignToNew state buffer fileName formatter =
        let newstate = flush state
        formatter buffer
        { newstate with
              BlockId = newstate.Contents.Append fileName
              Pins = 0
              IsNew = true
              LastLsn = LogSeqNo.DefaltValue }

    let newBuffer fileMgr logMgr =
        let bufferSize = fileMgr.BlockSize - LogSeqNo.Size

        let mutable state =
            { BufferSize = bufferSize
              Contents = Page.newPage fileMgr
              BlockId = BlockId.newBlockId "" -1L
              Pins = 0
              IsNew = false
              IsModified = false
              LastLsn = LogSeqNo.DefaltValue
              LogMgr = logMgr }

        let mutable callbackBuffer = None
        let internalLock = obj ()

        let buffer =
            { BufferSize = bufferSize
              GetVal = getVal state
              SetVal = fun offset value lsn -> lock internalLock (fun () -> state <- setVal state offset value lsn)
              LastLsn = fun () -> lastLsn state
              BlockId = fun () -> blockId state
              SetValue = fun offset value -> lock internalLock (fun () -> setValue state offset value)
              Pin = fun () -> lock internalLock (fun () -> state <- pin state)
              Unpin = fun () -> lock internalLock (fun () -> state <- unpin state)
              IsPinned = fun () -> isPinned state
              Flush = fun () -> lock internalLock (fun () -> state <- flush state)
              AssignToBlock = fun blockId -> lock internalLock (fun () -> state <- assignToBlock state blockId)
              AssignToNew =
                  fun fileName formatter ->
                      lock internalLock (fun () ->
                          state <- assignToNew state (callbackBuffer |> Option.get) fileName formatter) }

        callbackBuffer <- Some buffer
        buffer
