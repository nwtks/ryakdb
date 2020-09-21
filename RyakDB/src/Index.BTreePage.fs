module RyakDB.Index.BTreePage

open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Table
open RyakDB.Storage.Page
open RyakDB.Buffer.Buffer
open RyakDB.Buffer.TransactionBuffer
open RyakDB.Concurrency.TransactionConcurrency
open RyakDB.Recovery.TransactionRecovery
open RyakDB.Table.SlottedPage

module BTreePageFormatter =
    let makeDefaultRecord schema (offsetMap: Map<string, int32>) buffer position =
        schema.Fields()
        |> List.iter (fun field ->
            buffer.SetValue (position + offsetMap.[field]) (schema.DbType field |> DbConstant.defaultConstant))

let newBTreePageFormatter schema flags =
    let offsetMap = SlottedPage.offsetMap schema
    fun buffer ->
        let mutable position = 0
        buffer.SetValue position (IntDbConstant 0)
        position <- position + 4
        flags
        |> List.iter (fun f ->
            buffer.SetValue position (BigIntDbConstant f)
            position <- position + 8)
        let slotSize = SlottedPage.slotSize schema
        for pos in position .. slotSize .. (buffer.BufferSize - slotSize) do
            BTreePageFormatter.makeDefaultRecord schema offsetMap buffer pos

type BTreePage =
    { GetCountOfRecords: unit -> int32
      SetCountOfRecords: int32 -> unit
      BlockId: BlockId
      GetVal: int32 -> string -> DbConstant
      SetVal: int32 -> string -> DbConstant -> unit
      GetFlag: int32 -> int64
      SetFlag: int32 -> int64 -> unit
      IsFull: unit -> bool
      IsGettingFull: unit -> bool
      Insert: int32 -> unit
      Delete: int32 -> unit
      TransferRecords: int32 -> BTreePage -> int32 -> int32 -> unit
      Split: int32 -> int64 list -> int64
      CopyRecord: int32 -> int32 -> unit
      SetValueUnchecked: int32 -> string -> DbConstant -> unit
      Close: unit -> unit }

module BTreePage =
    let slotSize schema buffer =
        let size =
            schema.Fields()
            |> List.fold (fun size f -> size + (schema.DbType f |> Page.maxSize)) 0

        if size < 0 || size > buffer.BufferSize
        then failwith ("Slot size overflow:" + size.ToString())
        size

    let getValue (buffer: Buffer) = buffer.GetVal

    let setValue txRecovery (buffer: Buffer) offset value =
        txRecovery.LogSetVal buffer offset value
        |> buffer.SetVal offset value

    let setValueWithoutLogging (buffer: Buffer) offset value = buffer.SetVal offset value None

    let slotPosition headerSize slotSize slot = headerSize + slot * slotSize

    let fieldPosition headerSize slotSize (offsetMap: Map<string, int32>) slot fieldName =
        (slotPosition headerSize slotSize slot)
        + offsetMap.[fieldName]

    let setValueUnchecked txRecovery
                          schema
                          buffer
                          headerSize
                          slotSize
                          (offsetMap: Map<string, int32>)
                          slot
                          fieldName
                          value
                          =
        DbConstant.castTo (schema.DbType fieldName) value
        |> setValue txRecovery buffer (fieldPosition headerSize slotSize offsetMap slot fieldName)

    let getCountOfRecords buffer =
        getValue buffer 0 IntDbType |> DbConstant.toInt

    let setCountOfRecords txRecovery buffer value =
        IntDbConstant value
        |> setValue txRecovery buffer 0

    let setCountOfRecordsWithoutLogging buffer value =
        IntDbConstant value
        |> setValueWithoutLogging buffer 0

    let getVal schema buffer headerSize slotSize (offsetMap: Map<string, int32>) slot fieldName =
        if slot >= getCountOfRecords buffer
        then failwith ("Slot overflow:" + slot.ToString())
        schema.DbType fieldName
        |> getValue buffer (fieldPosition headerSize slotSize offsetMap slot fieldName)

    let setVal txRecovery
               schema
               buffer
               headerSize
               slotSize
               countOfSlots
               (offsetMap: Map<string, int32>)
               slot
               fieldName
               value
               =
        if slot >= countOfSlots
        then failwith ("Slot overflow:" + slot.ToString())
        if slot >= getCountOfRecords buffer
        then failwith ("Slot overflow:" + slot.ToString())
        setValueUnchecked txRecovery schema buffer headerSize slotSize offsetMap slot fieldName value

    let setValWithoutLogging schema buffer headerSize slotSize (offsetMap: Map<string, int32>) slot fieldName value =
        DbConstant.castTo (schema.DbType fieldName) value
        |> setValueWithoutLogging buffer (fieldPosition headerSize slotSize offsetMap slot fieldName)

    let getFlag buffer no =
        getValue buffer (4 + 8 * no) BigIntDbType
        |> DbConstant.toLong

    let setFlag txRecovery buffer no value =
        BigIntDbConstant value
        |> setValue txRecovery buffer (4 + 8 * no)

    let isFull buffer headerSize slotSize =
        (getCountOfRecords buffer)
        + 1
        |> slotPosition headerSize slotSize
        >= buffer.BufferSize

    let isGettingFull buffer headerSize slotSize =
        (getCountOfRecords buffer)
        + 2
        |> slotPosition headerSize slotSize
        >= buffer.BufferSize

    let copyRecord txRecovery schema buffer headerSize slotSize offsetMap fromSlot toSlot =
        schema.Fields()
        |> List.iter (fun field ->
            getVal schema buffer headerSize slotSize offsetMap fromSlot field
            |> setValueUnchecked txRecovery schema buffer headerSize slotSize offsetMap toSlot field)

    let copyRecordWithoutLogging schema buffer headerSize slotSize offsetMap fromSlot toSlot =
        schema.Fields()
        |> List.iter (fun field ->
            getVal schema buffer headerSize slotSize offsetMap fromSlot field
            |> setValWithoutLogging schema buffer headerSize slotSize offsetMap toSlot field)

    let insert schema buffer headerSize slotSize offsetMap countOfSlots slot =
        buffer.LockFlushing(fun () ->
            if slot >= countOfSlots
            then failwith ("Slot overflow:" + slot.ToString())

            let countOfRecords = getCountOfRecords buffer
            if countOfRecords + 1 > countOfSlots
            then failwith ("Slot overflow:" + slot.ToString())

            for i in countOfRecords .. -1 .. slot + 1 do
                copyRecordWithoutLogging schema buffer headerSize slotSize offsetMap (i - 1) i
            setCountOfRecordsWithoutLogging buffer (countOfRecords + 1))

    let delete schema buffer headerSize slotSize offsetMap slot =
        buffer.LockFlushing(fun () ->
            let countOfRecords = getCountOfRecords buffer
            for i in slot + 1 .. countOfRecords - 1 do
                copyRecordWithoutLogging schema buffer headerSize slotSize offsetMap i (i - 1)
            setCountOfRecordsWithoutLogging buffer (countOfRecords - 1))

    let transferRecords txRecovery schema buffer headerSize slotSize offsetMap start destPage destStart count =
        let countOfRecords = getCountOfRecords buffer
        let destCountOfRecords = destPage.GetCountOfRecords()

        let minCount =
            System.Math.Min(count, countOfRecords - start)

        for i in destCountOfRecords - 1 .. -1 .. destStart do
            destPage.CopyRecord i (i + minCount)
        for i in 0 .. minCount - 1 do
            schema.Fields()
            |> List.iter (fun field ->
                getVal schema buffer headerSize slotSize offsetMap (start + i) field
                |> destPage.SetValueUnchecked (destStart + i) field)
        for i in start + minCount .. countOfRecords - 1 do
            copyRecord txRecovery schema buffer headerSize slotSize offsetMap i (i - minCount)
        setCountOfRecords txRecovery buffer (countOfRecords - minCount)
        destPage.SetCountOfRecords(destCountOfRecords + minCount)

    let appendBlock txBuffer txConcurrency schema (BlockId (filename, _)) flags =
        txConcurrency.ModifyFile filename

        let buffer =
            newBTreePageFormatter schema flags
            |> txBuffer.PinNew filename

        txBuffer.Unpin buffer
        buffer.BlockId()

    let split txBuffer
              txConcurrency
              txRecovery
              schema
              blockId
              buffer
              headerSize
              slotSize
              offsetMap
              newBTreePage
              splitSlot
              flags
              =
        let countOfRecords = getCountOfRecords buffer

        let newBlockId =
            appendBlock txBuffer txConcurrency schema blockId flags

        let newPage =
            newBTreePage txBuffer txConcurrency txRecovery schema newBlockId (List.length flags)

        transferRecords
            txRecovery
            schema
            buffer
            headerSize
            slotSize
            offsetMap
            splitSlot
            newPage
            0
            (countOfRecords - splitSlot)
        newPage.Close()
        let (BlockId (_, blockNo)) = newBlockId
        blockNo

    let close txBuffer currentBuffer =
        currentBuffer |> Option.iter txBuffer.Unpin

let rec newBTreePage txBuffer txConcurrency txRecovery schema blockId countOfFlags =
    let buffer = txBuffer.Pin blockId
    let offsetMap = SlottedPage.offsetMap schema
    let slotSize = BTreePage.slotSize schema buffer

    let countOfSlots =
        (buffer.BufferSize - countOfFlags * 8) / slotSize

    let headerSize = 4 + countOfFlags * 8

    let mutable currentBuffer = Some(buffer)
    { GetCountOfRecords =
          fun () ->
              match currentBuffer with
              | Some (buffer) -> BTreePage.getCountOfRecords buffer
              | _ -> failwith "Closed page"
      SetCountOfRecords =
          fun value ->
              match currentBuffer with
              | Some (buffer) -> BTreePage.setCountOfRecords txRecovery buffer value
              | _ -> failwith "Closed page"
      BlockId = blockId
      GetVal =
          fun slot fieldName ->
              match currentBuffer with
              | Some (buffer) -> BTreePage.getVal schema buffer headerSize slotSize offsetMap slot fieldName
              | _ -> failwith "Closed page"
      SetVal =
          fun slot fieldName value ->
              match currentBuffer with
              | Some (buffer) ->
                  BTreePage.setVal
                      txRecovery
                      schema
                      buffer
                      headerSize
                      slotSize
                      countOfSlots
                      offsetMap
                      slot
                      fieldName
                      value
              | _ -> failwith "Closed page"
      GetFlag =
          fun no ->
              match currentBuffer with
              | Some (buffer) -> BTreePage.getFlag buffer no
              | _ -> failwith "Closed page"
      SetFlag =
          fun no value ->
              match currentBuffer with
              | Some (buffer) -> BTreePage.setFlag txRecovery buffer no value
              | _ -> failwith "Closed page"
      IsFull =
          fun () ->
              match currentBuffer with
              | Some (buffer) -> BTreePage.isFull buffer headerSize slotSize
              | _ -> failwith "Closed page"
      IsGettingFull =
          fun () ->
              match currentBuffer with
              | Some (buffer) -> BTreePage.isGettingFull buffer headerSize slotSize
              | _ -> failwith "Closed page"
      Insert =
          fun slot ->
              match currentBuffer with
              | Some (buffer) -> BTreePage.insert schema buffer headerSize slotSize offsetMap countOfSlots slot
              | _ -> failwith "Closed page"
      Delete =
          fun slot ->
              match currentBuffer with
              | Some (buffer) -> BTreePage.delete schema buffer headerSize slotSize offsetMap slot
              | _ -> failwith "Closed page"
      TransferRecords =
          fun start destPage destStart count ->
              match currentBuffer with
              | Some (buffer) ->
                  BTreePage.transferRecords
                      txRecovery
                      schema
                      buffer
                      headerSize
                      slotSize
                      offsetMap
                      start
                      destPage
                      destStart
                      count
              | _ -> failwith "Closed page"
      Split =
          fun splitSlot flags ->
              match currentBuffer with
              | Some (buffer) ->
                  BTreePage.split
                      txBuffer
                      txConcurrency
                      txRecovery
                      schema
                      blockId
                      buffer
                      headerSize
                      slotSize
                      offsetMap
                      newBTreePage
                      splitSlot
                      flags
              | _ -> failwith "Closed page"
      CopyRecord =
          fun fromSlot toSlot ->
              match currentBuffer with
              | Some (buffer) ->
                  BTreePage.copyRecord txRecovery schema buffer headerSize slotSize offsetMap fromSlot toSlot
              | _ -> failwith "Closed page"
      SetValueUnchecked =
          fun slot fieldName value ->
              match currentBuffer with
              | Some (buffer) ->
                  BTreePage.setValueUnchecked
                      txRecovery
                      schema
                      buffer
                      headerSize
                      slotSize
                      offsetMap
                      slot
                      fieldName
                      value
              | _ -> failwith "Closed page"
      Close =
          fun () ->
              BTreePage.close txBuffer currentBuffer
              currentBuffer <- None }
