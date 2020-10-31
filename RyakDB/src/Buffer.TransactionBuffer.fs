module RyakDB.Buffer.TransactionBuffer

open RyakDB.Storage
open RyakDB.Buffer.Buffer
open RyakDB.Buffer.BufferPool

type TransactionBuffer =
    { Pin: BlockId -> Buffer
      PinNew: string -> BufferFormatter -> Buffer
      Unpin: Buffer -> unit
      UnpinAll: unit -> unit }

module TransactionBuffer =
    type PinningBuffer = { Buffer: Buffer; PinCount: int }

    let private waitingTooLong (timestamp: System.DateTime) waitTime =
        (System.DateTime.Now.Ticks - timestamp.Ticks)
        / System.TimeSpan.TicksPerMillisecond
        + 50L > (int64 waitTime)

    let rec waitUnpinningBuffer bufferPool pinBufferPool timestamp buffer =
        if buffer |> Option.isSome then
            buffer
        elif waitingTooLong timestamp bufferPool.WaitTime then
            None
        else
            lock bufferPool (fun () ->
                System.Threading.Monitor.Wait(bufferPool, bufferPool.WaitTime)
                |> ignore)
            pinBufferPool ()
            |> waitUnpinningBuffer bufferPool pinBufferPool timestamp

    let pinNewBuffer (bufferPool: BufferPool) pinBufferPool repinBuffer pinningBuffers =
        if pinningBuffers
           |> Map.count
           >= bufferPool.BufferPoolSize then
            failwith "Buffer pool full"

        let nextBuffers, newBuffer =
            match pinBufferPool ()
                  |> Option.orElseWith (fun () -> waitUnpinningBuffer bufferPool pinBufferPool System.DateTime.Now None) with
            | Some buff ->
                pinningBuffers
                |> Map.add (buff.BlockId()) { Buffer = buff; PinCount = 1 },
                buff
            | _ -> pinningBuffers |> repinBuffer

        lock bufferPool (fun () -> System.Threading.Monitor.PulseAll(bufferPool))

        nextBuffers, newBuffer

    let pinExistBuffer blockId pinningBuffers =
        let pinnedBuff = pinningBuffers |> Map.find blockId

        let nextPinnedBuff =
            { pinnedBuff with
                  PinCount = pinnedBuff.PinCount + 1 }

        pinningBuffers |> Map.add blockId nextPinnedBuff, nextPinnedBuff.Buffer

    let unpin (bufferPool: BufferPool) buffer pinningBuffers =
        let blockId = buffer.BlockId()
        if pinningBuffers |> Map.containsKey blockId then
            let pinnedBuff = pinningBuffers |> Map.find blockId

            let nextPinnedBuff =
                { pinnedBuff with
                      PinCount = pinnedBuff.PinCount - 1 }

            match nextPinnedBuff.PinCount with
            | 0 ->
                bufferPool.Unpin buffer
                lock bufferPool (fun () -> System.Threading.Monitor.PulseAll(bufferPool))
                pinningBuffers |> Map.remove blockId
            | _ -> pinningBuffers |> Map.add blockId nextPinnedBuff
        else
            pinningBuffers

    let rec pin (bufferPool: BufferPool) blockId pinningBuffers =
        let pinBufferPool () = bufferPool.Pin blockId

        let repinBuffer pinningBuffers =
            pinningBuffers
            |> repin bufferPool
            |> pin bufferPool blockId

        if pinningBuffers |> Map.containsKey blockId then
            pinningBuffers |> pinExistBuffer blockId
        else
            pinningBuffers
            |> pinNewBuffer bufferPool pinBufferPool repinBuffer

    and repin (bufferPool: BufferPool) pinningBuffers =
        let repinningBuffers =
            pinningBuffers
            |> Map.fold (fun buffers _ buf -> unpin bufferPool buf.Buffer buffers) pinningBuffers

        lock bufferPool (fun () ->
            System.Threading.Monitor.Wait(bufferPool, bufferPool.WaitTime)
            |> ignore)

        pinningBuffers
        |> Map.fold (fun buffers blockId _ -> pin bufferPool blockId buffers |> fst) repinningBuffers

    let rec pinNew (bufferPool: BufferPool) fileName formatter pinningBuffers =
        let pinBufferPool () = bufferPool.PinNew fileName formatter

        let repinBuffer pinningBuffers =
            pinningBuffers
            |> repin bufferPool
            |> pinNew bufferPool fileName formatter

        pinningBuffers
        |> pinNewBuffer bufferPool pinBufferPool repinBuffer

    let unpinAll (bufferPool: BufferPool) pinningBuffers =
        pinningBuffers
        |> Map.iter (fun _ buf -> bufferPool.Unpin buf.Buffer)
        lock bufferPool (fun () -> System.Threading.Monitor.PulseAll(bufferPool))
        Map.empty

let newTransactionBuffer bufferPool =
    let mutable pinningBuffers = Map.empty

    { Pin =
          fun blockId ->
              let nextBuffers, buffer =
                  pinningBuffers
                  |> TransactionBuffer.pin bufferPool blockId

              pinningBuffers <- nextBuffers
              buffer
      PinNew =
          fun fileName formatter ->
              let nextBuffers, buffer =
                  pinningBuffers
                  |> TransactionBuffer.pinNew bufferPool fileName formatter

              pinningBuffers <- nextBuffers
              buffer
      Unpin =
          fun buffer ->
              pinningBuffers <-
                  pinningBuffers
                  |> TransactionBuffer.unpin bufferPool buffer
      UnpinAll =
          fun () ->
              pinningBuffers <-
                  pinningBuffers
                  |> TransactionBuffer.unpinAll bufferPool }
