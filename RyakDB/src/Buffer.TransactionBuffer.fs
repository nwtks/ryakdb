module RyakDB.Buffer.TransactionBuffer

open RyakDB.Storage
open RyakDB.Buffer.Buffer
open RyakDB.Buffer.BufferPool

type TransactionBuffer =
    { Pin: BlockId -> Buffer
      PinNew: string -> BufferFormatter -> Buffer
      Unpin: Buffer -> unit
      UnpinAll: unit -> unit
      FlushAll: unit -> unit
      FlushBuffers: unit -> unit
      Available: unit -> int32 }

module TransactionBuffer =
    type PinningBuffer = { Buffer: Buffer; PinCount: int }

    type TransactionBufferState =
        { PinningBuffers: Map<BlockId, PinningBuffer>
          BuffersToFlush: Buffer list
          TxNo: int64 }

    let private waitingTooLong (timestamp: System.DateTime) waitTime =
        (System.DateTime.Now.Ticks - timestamp.Ticks)
        / System.TimeSpan.TicksPerMillisecond
        + 50L > (int64 waitTime)

    let rec waitThreads bufferPool pinBufferPool timestamp buff =
        if Option.isSome buff then
            buff
        elif waitingTooLong timestamp bufferPool.WaitTime then
            None
        else
            lock bufferPool (fun () ->
                System.Threading.Monitor.Wait(bufferPool, bufferPool.WaitTime)
                |> ignore
                pinBufferPool ())
            |> waitThreads bufferPool pinBufferPool timestamp

    let pinNewBuffer (bufferPool: BufferPool) state pinBufferPool pinNext =
        if state.PinningBuffers.Count
           >= bufferPool.BufferPoolSize then
            failwith "Buffer pool full"

        let buff, waitedBeforeGotBuffer =
            match pinBufferPool () with
            | Some buff -> Some(buff), false
            | _ -> waitThreads bufferPool pinBufferPool System.DateTime.Now None, true

        let nextstate, buffer =
            match buff with
            | Some b ->
                { state with
                      PinningBuffers = state.PinningBuffers.Add(b.BlockId(), { Buffer = b; PinCount = 1 })
                      BuffersToFlush = b :: state.BuffersToFlush },
                b
            | None -> pinNext state

        if waitedBeforeGotBuffer
        then lock bufferPool (fun () -> System.Threading.Monitor.PulseAll(bufferPool))

        nextstate, buffer

    let unpin (bufferPool: BufferPool) state buffer =
        let blockId = buffer.BlockId()
        if state.PinningBuffers.ContainsKey blockId then
            let pinnedBuff = state.PinningBuffers.[blockId]

            let nextPinnedBuff =
                { pinnedBuff with
                      PinCount = pinnedBuff.PinCount - 1 }

            if nextPinnedBuff.PinCount = 0 then
                bufferPool.Unpin buffer
                lock bufferPool (fun () -> System.Threading.Monitor.PulseAll(bufferPool))
                { state with
                      PinningBuffers = state.PinningBuffers.Remove blockId }
            else
                { state with
                      PinningBuffers = state.PinningBuffers.Add(blockId, nextPinnedBuff) }
        else
            state

    let rec pin (bufferPool: BufferPool) state blockId =
        let inline pinExistBuffer state pinnedBuff =
            let nextPinnedBuff =
                { pinnedBuff with
                      PinCount = pinnedBuff.PinCount + 1 }

            let nextstate =
                { state with
                      PinningBuffers = state.PinningBuffers.Add(blockId, nextPinnedBuff) }

            nextstate, nextPinnedBuff.Buffer

        let inline pinBufferPool () = bufferPool.Pin blockId

        let inline pinNext state =
            pin bufferPool (repin bufferPool state) blockId

        if state.PinningBuffers.ContainsKey blockId
        then pinExistBuffer state state.PinningBuffers.[blockId]
        else pinNewBuffer bufferPool state pinBufferPool pinNext

    and repin (bufferPool: BufferPool) state =
        let repinningBuffers = state.PinningBuffers

        let newstate =
            repinningBuffers
            |> Map.fold (fun st _ v -> unpin bufferPool st v.Buffer) state

        lock bufferPool (fun () ->
            System.Threading.Monitor.Wait(bufferPool, bufferPool.WaitTime)
            |> ignore)

        repinningBuffers
        |> Map.fold (fun st k _ ->
            let st1, _ = pin bufferPool st k
            st1) newstate

    let rec pinNew (bufferPool: BufferPool) state fileName formatter =
        let inline pinBufferPool () = bufferPool.PinNew fileName formatter

        let inline pinNext state =
            pinNew bufferPool (repin bufferPool state) fileName formatter

        pinNewBuffer bufferPool state pinBufferPool pinNext

    let inline flushAll (bufferPool: BufferPool) = bufferPool.FlushAll()

    let inline flushBuffers state =
        state.BuffersToFlush
        |> List.iter (fun b -> b.Flush())

    let inline unpinAll (bufferPool: BufferPool) state =
        state.PinningBuffers
        |> Map.iter (fun _ v -> bufferPool.Unpin v.Buffer)
        lock bufferPool (fun () -> System.Threading.Monitor.PulseAll(bufferPool))
        { state with
              PinningBuffers = Map.empty }

let newTransactionBuffer bufferPool txNo =
    let mutable state: TransactionBuffer.TransactionBufferState =
        { PinningBuffers = Map.empty
          BuffersToFlush = []
          TxNo = txNo }

    { Pin =
          fun blockId ->
              let nextstate, buffer =
                  TransactionBuffer.pin bufferPool state blockId

              state <- nextstate
              buffer
      PinNew =
          fun fileName formatter ->
              let nextstate, buffer =
                  TransactionBuffer.pinNew bufferPool state fileName formatter

              state <- nextstate
              buffer
      Unpin = fun buffer -> state <- TransactionBuffer.unpin bufferPool state buffer
      UnpinAll = fun () -> state <- TransactionBuffer.unpinAll bufferPool state
      FlushAll = fun () -> TransactionBuffer.flushAll bufferPool
      FlushBuffers = fun () -> TransactionBuffer.flushBuffers state
      Available = fun () -> bufferPool.Available() }
