namespace RyakDB.Storage.Buffer

open RyakDB.Storage.Type
open RyakDB.Storage.File
open RyakDB.Storage.Log

type BufferPoolManager =
    { BufferPoolSize: int32
      WaitTime: int32
      Pin: BlockId -> Buffer option
      PinNew: string -> BufferFormatter -> Buffer option
      Unpin: Buffer -> unit
      FlushAll: unit -> unit
      Available: unit -> int32 }

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

    let assignToNew state (buffer: Buffer) fileName (formatter: BufferFormatter) =
        let newstate = flush state
        formatter buffer
        { newstate with
              BlockId = newstate.Contents.Append fileName
              Pins = 0
              IsNew = true
              LastLsn = LogSeqNo.DefaltValue }

    let newBuffer (fileMgr: FileManager) (logMgr: LogManager) =
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

module BufferPoolManager =
    type BufferPoolManagerState =
        { BufferPool: Buffer []
          BlockMap: System.Collections.Concurrent.ConcurrentDictionary<BlockId, Buffer>
          mutable LastReplacedBuff: int
          mutable Available: int
          Anchors: obj [] }

    let private prepareAnchor (anchors: obj []) a =
        let h = hash a % anchors.Length
        if h < 0 then h + anchors.Length else h
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
        let rec roundrobinBuffer pin lastReplacedBuff currBlk =
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
            else roundrobinBuffer pin lastReplacedBuff ((currBlk + 1) % state.BufferPool.Length)

        let pinNew assignBuffer =
            fun (buffer: Buffer) ->
                state.BlockMap.TryRemove(buffer.BlockId())
                |> ignore
                assignBuffer buffer
                state.BlockMap.TryAdd(buffer.BlockId(), buffer)
                |> ignore
                pinBuffer state buffer
                buffer

        roundrobinBuffer (pinNew assignBuffer) state.LastReplacedBuff
            ((state.LastReplacedBuff + 1) % state.BufferPool.Length)

    let rec pin state blockId =
        let pinExistBuffer (buffer: Buffer) =
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

    let pinNew state fileName (formatter: BufferFormatter) =
        let assignBuffer buffer = buffer.AssignToNew fileName formatter

        lock (prepareAnchor state.Anchors fileName) (fun () -> pinNewBuffer state assignBuffer)

    let newBufferPoolManager (fileMgr: FileManager) (logMgr: LogManager) size waitTime =
        let state =
            { BufferPool = Array.init size (fun _ -> Buffer.newBuffer fileMgr logMgr)
              BlockMap = System.Collections.Concurrent.ConcurrentDictionary()
              LastReplacedBuff = 0
              Available = size
              Anchors = Array.init 1019 (fun _ -> obj ()) }

        { BufferPoolSize = size
          Pin = pin state
          PinNew = pinNew state
          Unpin = unpin state
          FlushAll = fun () -> flushAll state
          Available = fun () -> state.Available
          WaitTime = waitTime }

module BufferManager =
    type PinningBuffer = { Buffer: Buffer; PinCount: int }

    type BufferManagerState =
        { PinningBuffers: Map<BlockId, PinningBuffer>
          BuffersToFlush: Buffer list
          TxNo: int64 }

    let private waitingTooLong (timestamp: System.DateTime) waitTime =
        (System.DateTime.Now.Ticks - timestamp.Ticks)
        / System.TimeSpan.TicksPerMillisecond
        + 50L > (int64 waitTime)

    let rec waitThreads bufferPoolMgr pinBufferPool timestamp (buff: Buffer option) =
        if Option.isSome buff then
            buff
        elif waitingTooLong timestamp bufferPoolMgr.WaitTime then
            None
        else
            lock bufferPoolMgr (fun () ->
                System.Threading.Monitor.Wait(bufferPoolMgr, bufferPoolMgr.WaitTime)
                |> ignore
                pinBufferPool ())
            |> waitThreads bufferPoolMgr pinBufferPool timestamp

    let pinNewBuffer (bufferPoolMgr: BufferPoolManager) (state: BufferManagerState) pinBufferPool pinNext =
        if state.PinningBuffers.Count
           >= bufferPoolMgr.BufferPoolSize then
            failwith "Buffer pool full"

        let buff, waitedBeforeGotBuffer =
            match pinBufferPool () with
            | Some buff -> Some(buff), false
            | _ -> waitThreads bufferPoolMgr pinBufferPool System.DateTime.Now None, true

        let nextstate, buffer =
            match buff with
            | Some b ->
                { state with
                      PinningBuffers = state.PinningBuffers.Add(b.BlockId(), { Buffer = b; PinCount = 1 })
                      BuffersToFlush = b :: state.BuffersToFlush },
                b
            | None -> pinNext state

        if waitedBeforeGotBuffer
        then lock bufferPoolMgr (fun () -> System.Threading.Monitor.PulseAll(bufferPoolMgr))

        nextstate, buffer

    let unpin (bufferPoolMgr: BufferPoolManager) (state: BufferManagerState) (buffer: Buffer) =
        let blockId = buffer.BlockId()
        if state.PinningBuffers.ContainsKey blockId then
            let pinnedBuff = state.PinningBuffers.[blockId]

            let nextPinnedBuff =
                { pinnedBuff with
                      PinCount = pinnedBuff.PinCount - 1 }

            if nextPinnedBuff.PinCount = 0 then
                bufferPoolMgr.Unpin buffer
                lock bufferPoolMgr (fun () -> System.Threading.Monitor.PulseAll(bufferPoolMgr))
                { state with
                      PinningBuffers = state.PinningBuffers.Remove blockId }
            else
                { state with
                      PinningBuffers = state.PinningBuffers.Add(blockId, nextPinnedBuff) }
        else
            state

    let rec pin (bufferPoolMgr: BufferPoolManager) (state: BufferManagerState) blockId =
        let pinExistBuffer state pinnedBuff =
            let nextPinnedBuff =
                { pinnedBuff with
                      PinCount = pinnedBuff.PinCount + 1 }

            let nextstate =
                { state with
                      PinningBuffers = state.PinningBuffers.Add(blockId, nextPinnedBuff) }

            nextstate, nextPinnedBuff.Buffer

        let pinBufferPool () = bufferPoolMgr.Pin blockId

        let pinNext state =
            pin bufferPoolMgr (repin bufferPoolMgr state) blockId

        if state.PinningBuffers.ContainsKey blockId
        then pinExistBuffer state state.PinningBuffers.[blockId]
        else pinNewBuffer bufferPoolMgr state pinBufferPool pinNext

    and repin (bufferPoolMgr: BufferPoolManager) (state: BufferManagerState) =
        let repinningBuffers = state.PinningBuffers

        let newstate =
            repinningBuffers
            |> Map.fold (fun st _ v -> unpin bufferPoolMgr st v.Buffer) state

        lock bufferPoolMgr (fun () ->
            System.Threading.Monitor.Wait(bufferPoolMgr, bufferPoolMgr.WaitTime)
            |> ignore)

        repinningBuffers
        |> Map.fold (fun st k _ ->
            let st1, _ = pin bufferPoolMgr st k
            st1) newstate

    let rec pinNew (bufferPoolMgr: BufferPoolManager) (state: BufferManagerState) fileName (formatter: BufferFormatter) =
        let pinBufferPool () = bufferPoolMgr.PinNew fileName formatter

        let pinNext state =
            pinNew bufferPoolMgr (repin bufferPoolMgr state) fileName formatter

        pinNewBuffer bufferPoolMgr state pinBufferPool pinNext

    let flushAll (bufferPoolMgr: BufferPoolManager) = bufferPoolMgr.FlushAll()

    let flushBuffers (state: BufferManagerState) =
        state.BuffersToFlush
        |> List.iter (fun b -> b.Flush())

    let unpinAll (bufferPoolMgr: BufferPoolManager) (state: BufferManagerState) =
        state.PinningBuffers
        |> Map.iter (fun _ v -> bufferPoolMgr.Unpin v.Buffer)
        lock bufferPoolMgr (fun () -> System.Threading.Monitor.PulseAll(bufferPoolMgr))
        { state with
              PinningBuffers = Map.empty }

    let newBufferManager bufferPoolMgr txNo =
        let mutable state =
            { PinningBuffers = Map.empty
              BuffersToFlush = []
              TxNo = txNo }

        { Pin =
              fun blockId ->
                  let nextstate, buffer = pin bufferPoolMgr state blockId
                  state <- nextstate
                  buffer
          PinNew =
              fun fileName formatter ->
                  let nextstate, buffer =
                      pinNew bufferPoolMgr state fileName formatter

                  state <- nextstate
                  buffer
          Unpin = fun buffer -> state <- unpin bufferPoolMgr state buffer
          UnpinAll = fun () -> state <- unpinAll bufferPoolMgr state
          FlushAll = fun () -> flushAll bufferPoolMgr
          FlushBuffers = fun () -> flushBuffers state
          Available = fun () -> bufferPoolMgr.Available() },
        { OnTxCommit = fun _ -> state <- unpinAll bufferPoolMgr state
          OnTxRollback = fun _ -> state <- unpinAll bufferPoolMgr state
          OnTxEndStatement = fun _ -> () }
