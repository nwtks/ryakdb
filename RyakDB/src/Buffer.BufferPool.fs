module RyakDB.Buffer.BufferPool

open RyakDB.Storage
open RyakDB.Buffer.Buffer

type BufferPool =
    { BufferPoolSize: int32
      WaitTime: int32
      Pin: BlockId -> Buffer option
      PinNew: string -> BufferFormatter -> Buffer option
      Unpin: Buffer -> unit
      FlushAll: unit -> unit
      Available: unit -> int32 }

module BufferPool =
    type BufferPoolState =
        { BufferPool: Buffer []
          BlockMap: System.Collections.Concurrent.ConcurrentDictionary<BlockId, Buffer>
          mutable LastReplacedBuff: int
          mutable Available: int
          Anchors: obj [] }

    let private prepareAnchor (anchors: obj []) a =
        let h = hash a % anchors.Length
        if h < 0 then h + anchors.Length else h
        |> anchors.GetValue

    let inline findExistingBuffer state blockId =
        let mutable buffer = Unchecked.defaultof<Buffer>
        if state.BlockMap.TryGetValue(blockId, &buffer) then
            Some buffer
            |> Option.filter (fun b -> b.BlockId() = blockId)
        else
            None

    let inline flushAll state =
        state.BufferPool
        |> Array.iter (fun b -> b.Flush())

    let unpin state (buffer: Buffer) =
        lock buffer (fun () ->
            buffer.Unpin()
            if not (buffer.IsPinned()) then
                System.Threading.Interlocked.Increment(&state.Available)
                |> ignore)

    let pinBuffer state buffer =
        lock buffer (fun () ->
            if not (buffer.IsPinned()) then
                System.Threading.Interlocked.Decrement(&state.Available)
                |> ignore
            buffer.Pin())

    let pinNewBuffer state assignBuffer =
        let rec clockwiseBuffer pin lastReplacedBuff currBlk =
            let buffer = state.BufferPool.[currBlk]

            let result =
                if System.Threading.Monitor.TryEnter buffer then
                    try
                        if not (buffer.IsPinned()) then
                            System.Threading.Interlocked.Exchange(&state.LastReplacedBuff, currBlk)
                            |> ignore
                            Some(pin buffer)
                        else
                            None
                    finally
                        System.Threading.Monitor.Exit buffer
                else
                    None

            if Option.isSome result
            then result
            elif currBlk = lastReplacedBuff
            then None
            else clockwiseBuffer pin lastReplacedBuff ((currBlk + 1) % state.BufferPool.Length)

        let pinNew assignBuffer =
            fun (buffer: Buffer) ->
                state.BlockMap.TryRemove(buffer.BlockId())
                |> ignore
                assignBuffer buffer
                state.BlockMap.TryAdd(buffer.BlockId(), buffer)
                |> ignore
                pinBuffer state buffer
                buffer

        clockwiseBuffer
            (pinNew assignBuffer)
            state.LastReplacedBuff
            ((state.LastReplacedBuff + 1) % state.BufferPool.Length)

    let rec pin state blockId =
        let inline pinExistBuffer (buffer: Buffer) =
            if blockId = buffer.BlockId() then
                pinBuffer state buffer
                Some buffer
            else
                pin state blockId

        let assignBuffer buffer = buffer.AssignToBlock blockId
        let (BlockId (fileName, _)) = blockId
        lock (prepareAnchor state.Anchors fileName) (fun () ->
            match findExistingBuffer state blockId with
            | Some buffer -> pinExistBuffer buffer
            | None -> pinNewBuffer state assignBuffer)

    let pinNew state fileName formatter =
        let assignBuffer buffer = buffer.AssignToNew fileName formatter
        lock (prepareAnchor state.Anchors fileName) (fun () -> pinNewBuffer state assignBuffer)

let newBufferPool fileMgr logMgr size waitTime =
    let state: BufferPool.BufferPoolState =
        { BufferPool = Array.init size (fun _ -> newBuffer fileMgr logMgr)
          BlockMap = System.Collections.Concurrent.ConcurrentDictionary()
          LastReplacedBuff = 0
          Available = size
          Anchors = Array.init 1019 (fun _ -> obj ()) }

    { BufferPoolSize = size
      Pin = BufferPool.pin state
      PinNew = BufferPool.pinNew state
      Unpin = BufferPool.unpin state
      FlushAll = fun () -> BufferPool.flushAll state
      Available = fun () -> state.Available
      WaitTime = waitTime }
