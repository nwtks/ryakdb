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
          mutable LastReplacedIndex: int
          mutable Available: int
          Anchors: obj [] }

    let private getAnchor a anchors =
        let h = hash a % Array.length anchors
        if h < 0 then h + Array.length anchors else h
        |> anchors.GetValue

    let findExistingBuffer state blockId =
        let mutable buffer = Unchecked.defaultof<Buffer>
        if state.BlockMap.TryGetValue(blockId, &buffer) then
            Some buffer
            |> Option.filter (fun b -> b.BlockId() = blockId)
        else
            None

    let flushAll state =
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
        let rec clockwiseBuffer pin lastReplacedIndex currentIndex =
            let buffer = state.BufferPool.[currentIndex]

            let result =
                if System.Threading.Monitor.TryEnter buffer then
                    try
                        if buffer.IsPinned() then
                            None
                        else
                            System.Threading.Interlocked.Exchange(&state.LastReplacedIndex, currentIndex)
                            |> ignore
                            pin buffer |> Some
                    finally
                        System.Threading.Monitor.Exit buffer
                else
                    None

            if result |> Option.isSome then
                result
            elif currentIndex = lastReplacedIndex then
                None
            else
                (currentIndex + 1) % Array.length state.BufferPool
                |> clockwiseBuffer pin lastReplacedIndex

        let pinNew assignBuffer =
            fun buffer ->
                state.BlockMap.TryRemove(buffer.BlockId())
                |> ignore
                assignBuffer buffer
                state.BlockMap.TryAdd(buffer.BlockId(), buffer)
                |> ignore
                pinBuffer state buffer
                buffer

        (state.LastReplacedIndex + 1) % Array.length state.BufferPool
        |> clockwiseBuffer (pinNew assignBuffer) state.LastReplacedIndex

    let rec pin state blockId =
        let pinExistBuffer buffer =
            if blockId = buffer.BlockId() then
                pinBuffer state buffer
                Some buffer
            else
                pin state blockId

        let assignBuffer buffer = buffer.AssignToBlock blockId

        lock
            (state.Anchors
             |> getAnchor (BlockId.fileName blockId)) (fun () ->
            match findExistingBuffer state blockId with
            | Some buffer -> pinExistBuffer buffer
            | _ -> pinNewBuffer state assignBuffer)

    let pinNew state fileName formatter =
        let assignBuffer buffer = buffer.AssignToNew fileName formatter

        lock (state.Anchors |> getAnchor fileName) (fun () -> pinNewBuffer state assignBuffer)

let newBufferPool fileService logService size waitTime =
    let state: BufferPool.BufferPoolState =
        { BufferPool = Array.init size (fun _ -> newBuffer fileService logService)
          BlockMap = System.Collections.Concurrent.ConcurrentDictionary()
          LastReplacedIndex = 0
          Available = size
          Anchors = Array.init 1019 (fun _ -> obj ()) }

    { BufferPoolSize = size
      Pin = BufferPool.pin state
      PinNew = BufferPool.pinNew state
      Unpin = BufferPool.unpin state
      FlushAll = fun () -> BufferPool.flushAll state
      Available = fun () -> state.Available
      WaitTime = waitTime }
