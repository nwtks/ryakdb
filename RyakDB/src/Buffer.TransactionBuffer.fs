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
        if Option.isSome buffer then
            buffer
        elif waitingTooLong timestamp bufferPool.WaitTime then
            None
        else
            lock bufferPool (fun () ->
                System.Threading.Monitor.Wait(bufferPool, bufferPool.WaitTime)
                |> ignore)
            pinBufferPool ()
            |> waitUnpinningBuffer bufferPool pinBufferPool timestamp

    let pinNewBuffer (bufferPool: BufferPool) pinningBuffers pinBufferPool repinBuffer =
        if Map.count pinningBuffers
           >= bufferPool.BufferPoolSize then
            failwith "Buffer pool full"

        let nextBuffers, newBuffer =
            match pinBufferPool ()
                  |> Option.orElseWith (fun () -> waitUnpinningBuffer bufferPool pinBufferPool System.DateTime.Now None) with
            | Some buff -> Map.add (buff.BlockId()) { Buffer = buff; PinCount = 1 } pinningBuffers, buff
            | _ -> repinBuffer pinningBuffers

        lock bufferPool (fun () -> System.Threading.Monitor.PulseAll(bufferPool))

        nextBuffers, newBuffer

    let pinExistBuffer pinningBuffers blockId =
        let pinnedBuff = Map.find blockId pinningBuffers

        let nextPinnedBuff =
            { pinnedBuff with
                  PinCount = pinnedBuff.PinCount + 1 }

        Map.add blockId nextPinnedBuff pinningBuffers, nextPinnedBuff.Buffer

    let unpin (bufferPool: BufferPool) pinningBuffers buffer =
        let blockId = buffer.BlockId()
        if Map.containsKey blockId pinningBuffers then
            let pinnedBuff = Map.find blockId pinningBuffers

            let nextPinnedBuff =
                { pinnedBuff with
                      PinCount = pinnedBuff.PinCount - 1 }

            if nextPinnedBuff.PinCount = 0 then
                bufferPool.Unpin buffer
                lock bufferPool (fun () -> System.Threading.Monitor.PulseAll(bufferPool))
                Map.remove blockId pinningBuffers
            else
                Map.add blockId nextPinnedBuff pinningBuffers
        else
            pinningBuffers

    let rec pin (bufferPool: BufferPool) blockId pinningBuffers =
        let pinBufferPool () = bufferPool.Pin blockId

        let repinBuffer pinningBuffers =
            repin bufferPool pinningBuffers
            |> pin bufferPool blockId

        if Map.containsKey blockId pinningBuffers
        then pinExistBuffer pinningBuffers blockId
        else pinNewBuffer bufferPool pinningBuffers pinBufferPool repinBuffer

    and repin (bufferPool: BufferPool) pinningBuffers =
        let repinningBuffers =
            pinningBuffers
            |> Map.fold (fun buffers _ buf -> unpin bufferPool buffers buf.Buffer) pinningBuffers

        lock bufferPool (fun () ->
            System.Threading.Monitor.Wait(bufferPool, bufferPool.WaitTime)
            |> ignore)

        pinningBuffers
        |> Map.fold (fun buffers blockId _ -> pin bufferPool blockId buffers |> fst) repinningBuffers

    let rec pinNew (bufferPool: BufferPool) fileName formatter pinningBuffers =
        let pinBufferPool () = bufferPool.PinNew fileName formatter

        let repinBuffer pinningBuffers =
            repin bufferPool pinningBuffers
            |> pinNew bufferPool fileName formatter

        pinNewBuffer bufferPool pinningBuffers pinBufferPool repinBuffer

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
                  TransactionBuffer.pin bufferPool blockId pinningBuffers

              pinningBuffers <- nextBuffers
              buffer
      PinNew =
          fun fileName formatter ->
              let nextBuffers, buffer =
                  TransactionBuffer.pinNew bufferPool fileName formatter pinningBuffers

              pinningBuffers <- nextBuffers
              buffer
      Unpin = fun buffer -> pinningBuffers <- TransactionBuffer.unpin bufferPool pinningBuffers buffer
      UnpinAll = fun () -> pinningBuffers <- TransactionBuffer.unpinAll bufferPool pinningBuffers }
