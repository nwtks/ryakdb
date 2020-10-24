module RyakDB.Index.BTreePage

open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Table
open RyakDB.Storage.Page
open RyakDB.Buffer.Buffer
open RyakDB.Buffer.TransactionBuffer
open RyakDB.Concurrency.TransactionConcurrency
open RyakDB.Recovery.TransactionRecovery
open RyakDB.Table.TablePage

module BTreePageFormatter =
    let makeDefaultRecord schema offsetMap buffer position =
        schema.Fields()
        |> List.iter (fun field ->
            buffer.SetValue (position + (Map.find field offsetMap)) (schema.DbType field |> DbConstant.defaultConstant))

let newBTreePageFormatter schema flags =
    let offsetMap = TablePage.offsetMap schema
    fun buffer ->
        let mutable position = 0
        buffer.SetValue position (IntDbConstant 0)
        position <- position + 4
        flags
        |> List.iter (fun f ->
            buffer.SetValue position (BigIntDbConstant f)
            position <- position + 8)
        let slotSize = TablePage.slotSize schema
        [ position .. slotSize .. buffer.BufferSize - slotSize - 1 ]
        |> List.iter (BTreePageFormatter.makeDefaultRecord schema offsetMap buffer)

type BTreePage =
    { GetCountOfRecords: unit -> int32
      SetCountOfRecords: int32 -> unit
      BlockId: BlockId
      GetVal: int32 -> string -> DbConstant
      SetVal: int32 -> string -> DbConstant -> unit
      GetFlag: int32 -> int64
      SetFlag: int32 -> int64 -> unit
      IsFull: unit -> bool
      WillFull: unit -> bool
      Insert: int32 -> unit
      Delete: int32 -> unit
      TransferRecords: int32 -> BTreePage -> int32 -> int32 -> unit
      Split: int32 -> int64 list -> int64
      CopyRecord: int32 -> int32 -> unit
      SetValueUnchecked: int32 -> string -> DbConstant -> unit
      Close: unit -> unit }
    interface System.IDisposable with
        member this.Dispose() = this.Close()

module BTreePage =
    let appendBlock txBuffer txConcurrency schema fileName flags =
        txConcurrency.ModifyFile fileName

        let buff =
            newBTreePageFormatter schema flags
            |> txBuffer.PinNew fileName

        txBuffer.Unpin buff
        buff.BlockId()

    let slotSize schema buffer =
        let size =
            schema.Fields()
            |> List.fold (fun size f -> size + (schema.DbType f |> Page.maxSize)) 0

        if size < 0 || size > buffer.BufferSize
        then failwith ("Slot size overflow:" + size.ToString())

        size

    let getValue (buffer: Buffer) = buffer.GetVal

    let setValue txRecovery buffer offset value =
        txRecovery.LogSetVal buffer offset value
        |> buffer.SetVal offset value

    let setValueWithoutLogging (buffer: Buffer) offset value = buffer.SetVal offset value None

    let slotPosition headerSize slotSize slotNo = headerSize + slotNo * slotSize

    let fieldPosition headerSize slotSize offsetMap slotNo fieldName =
        (slotPosition headerSize slotSize slotNo)
        + (Map.find fieldName offsetMap)

    let setValueUnchecked txRecovery schema buffer headerSize slotSize offsetMap slotNo fieldName value =
        DbConstant.castTo (schema.DbType fieldName) value
        |> setValue txRecovery buffer (fieldPosition headerSize slotSize offsetMap slotNo fieldName)

    let getCountOfRecords buffer =
        getValue buffer 0 IntDbType |> DbConstant.toInt

    let setCountOfRecords txRecovery buffer value =
        IntDbConstant value
        |> setValue txRecovery buffer 0

    let setCountOfRecordsWithoutLogging buffer value =
        IntDbConstant value
        |> setValueWithoutLogging buffer 0

    let getVal schema buffer headerSize slotSize offsetMap slotNo fieldName =
        if slotNo >= getCountOfRecords buffer
        then failwith ("Slot overflow:" + slotNo.ToString())

        schema.DbType fieldName
        |> getValue buffer (fieldPosition headerSize slotSize offsetMap slotNo fieldName)

    let setVal txRecovery schema buffer headerSize slotSize countOfSlots offsetMap slotNo fieldName value =
        if slotNo >= countOfSlots
        then failwith ("Slot overflow:" + slotNo.ToString())

        if slotNo >= getCountOfRecords buffer
        then failwith ("Slot overflow:" + slotNo.ToString())

        setValueUnchecked txRecovery schema buffer headerSize slotSize offsetMap slotNo fieldName value

    let setValWithoutLogging schema buffer headerSize slotSize offsetMap slotNo fieldName value =
        DbConstant.castTo (schema.DbType fieldName) value
        |> setValueWithoutLogging buffer (fieldPosition headerSize slotSize offsetMap slotNo fieldName)

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

    let willFull buffer headerSize slotSize =
        (getCountOfRecords buffer)
        + 2
        |> slotPosition headerSize slotSize
        >= buffer.BufferSize

    let copyRecord txRecovery schema buffer headerSize slotSize offsetMap fromSlotNo toSlotNo =
        schema.Fields()
        |> List.iter (fun field ->
            getVal schema buffer headerSize slotSize offsetMap fromSlotNo field
            |> setValueUnchecked txRecovery schema buffer headerSize slotSize offsetMap toSlotNo field)

    let copyRecordWithoutLogging schema buffer headerSize slotSize offsetMap fromSlotNo toSlotNo =
        schema.Fields()
        |> List.iter (fun field ->
            getVal schema buffer headerSize slotSize offsetMap fromSlotNo field
            |> setValWithoutLogging schema buffer headerSize slotSize offsetMap toSlotNo field)

    let insert schema buffer headerSize slotSize offsetMap countOfSlots slotNo =
        buffer.LockFlushing(fun () ->
            if slotNo >= countOfSlots
            then failwith ("Slot overflow:" + slotNo.ToString())

            let countOfRecords = getCountOfRecords buffer
            if countOfRecords + 1 > countOfSlots
            then failwith ("Slot overflow:" + slotNo.ToString())

            [ countOfRecords .. -1 .. slotNo + 1 ]
            |> List.iter (fun i -> copyRecordWithoutLogging schema buffer headerSize slotSize offsetMap (i - 1) i)
            setCountOfRecordsWithoutLogging buffer (countOfRecords + 1))

    let delete schema buffer headerSize slotSize offsetMap slotNo =
        buffer.LockFlushing(fun () ->
            let countOfRecords = getCountOfRecords buffer
            [ slotNo + 1 .. countOfRecords - 1 ]
            |> List.iter (fun i -> copyRecordWithoutLogging schema buffer headerSize slotSize offsetMap i (i - 1))
            setCountOfRecordsWithoutLogging buffer (countOfRecords - 1))

    let transferRecords txRecovery
                        schema
                        buffer
                        headerSize
                        slotSize
                        offsetMap
                        startSlotNo
                        destPage
                        destStartSlotNo
                        count
                        =
        let countOfRecords = getCountOfRecords buffer
        let destCountOfRecords = destPage.GetCountOfRecords()

        let minCount =
            System.Math.Min(count, countOfRecords - startSlotNo)

        [ destCountOfRecords - 1 .. -1 .. destStartSlotNo ]
        |> List.iter (fun i -> destPage.CopyRecord i (i + minCount))

        [ 0 .. minCount - 1 ]
        |> List.iter (fun i ->
            schema.Fields()
            |> List.iter (fun field ->
                getVal schema buffer headerSize slotSize offsetMap (startSlotNo + i) field
                |> destPage.SetValueUnchecked (destStartSlotNo + i) field))

        [ startSlotNo + minCount .. countOfRecords - 1 ]
        |> List.iter (fun i -> copyRecord txRecovery schema buffer headerSize slotSize offsetMap i (i - minCount))

        setCountOfRecords txRecovery buffer (countOfRecords - minCount)
        destPage.SetCountOfRecords(destCountOfRecords + minCount)

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
              splitSlotNo
              flags
              =
        let countOfRecords = getCountOfRecords buffer

        let newBlockId =
            appendBlock txBuffer txConcurrency schema (BlockId.fileName blockId) flags

        use newPage =
            newBTreePage txBuffer txConcurrency txRecovery schema newBlockId (List.length flags)

        transferRecords
            txRecovery
            schema
            buffer
            headerSize
            slotSize
            offsetMap
            splitSlotNo
            newPage
            0
            (countOfRecords - splitSlotNo)
        BlockId.blockNo newBlockId

    let close txBuffer currentBuffer =
        currentBuffer |> Option.iter txBuffer.Unpin

let rec newBTreePage txBuffer txConcurrency txRecovery schema blockId countOfFlags =
    let headerSize = 4 + countOfFlags * 8
    let buffer = txBuffer.Pin blockId
    let offsetMap = TablePage.offsetMap schema
    let slotSize = BTreePage.slotSize schema buffer

    let countOfSlots =
        (buffer.BufferSize - headerSize) / slotSize

    let mutable currentBuffer = Some buffer
    { GetCountOfRecords =
          fun () ->
              match currentBuffer with
              | Some buffer -> BTreePage.getCountOfRecords buffer
              | _ -> failwith "Closed page"
      SetCountOfRecords =
          fun value ->
              match currentBuffer with
              | Some buffer -> BTreePage.setCountOfRecords txRecovery buffer value
              | _ -> failwith "Closed page"
      BlockId = blockId
      GetVal =
          fun slotNo fieldName ->
              match currentBuffer with
              | Some buffer -> BTreePage.getVal schema buffer headerSize slotSize offsetMap slotNo fieldName
              | _ -> failwith "Closed page"
      SetVal =
          fun slotNo fieldName value ->
              match currentBuffer with
              | Some buffer ->
                  BTreePage.setVal
                      txRecovery
                      schema
                      buffer
                      headerSize
                      slotSize
                      countOfSlots
                      offsetMap
                      slotNo
                      fieldName
                      value
              | _ -> failwith "Closed page"
      GetFlag =
          fun no ->
              match currentBuffer with
              | Some buffer -> BTreePage.getFlag buffer no
              | _ -> failwith "Closed page"
      SetFlag =
          fun no value ->
              match currentBuffer with
              | Some buffer -> BTreePage.setFlag txRecovery buffer no value
              | _ -> failwith "Closed page"
      IsFull =
          fun () ->
              match currentBuffer with
              | Some buffer -> BTreePage.isFull buffer headerSize slotSize
              | _ -> failwith "Closed page"
      WillFull =
          fun () ->
              match currentBuffer with
              | Some buffer -> BTreePage.willFull buffer headerSize slotSize
              | _ -> failwith "Closed page"
      Insert =
          fun slotNo ->
              match currentBuffer with
              | Some buffer -> BTreePage.insert schema buffer headerSize slotSize offsetMap countOfSlots slotNo
              | _ -> failwith "Closed page"
      Delete =
          fun slotNo ->
              match currentBuffer with
              | Some buffer -> BTreePage.delete schema buffer headerSize slotSize offsetMap slotNo
              | _ -> failwith "Closed page"
      TransferRecords =
          fun startSlotNo destPage destStartSlotNo count ->
              match currentBuffer with
              | Some buffer ->
                  BTreePage.transferRecords
                      txRecovery
                      schema
                      buffer
                      headerSize
                      slotSize
                      offsetMap
                      startSlotNo
                      destPage
                      destStartSlotNo
                      count
              | _ -> failwith "Closed page"
      Split =
          fun splitSlotNo flags ->
              match currentBuffer with
              | Some buffer ->
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
                      splitSlotNo
                      flags
              | _ -> failwith "Closed page"
      CopyRecord =
          fun fromSlotNo toSlotNo ->
              match currentBuffer with
              | Some buffer ->
                  BTreePage.copyRecord txRecovery schema buffer headerSize slotSize offsetMap fromSlotNo toSlotNo
              | _ -> failwith "Closed page"
      SetValueUnchecked =
          fun slotNo fieldName value ->
              match currentBuffer with
              | Some buffer ->
                  BTreePage.setValueUnchecked
                      txRecovery
                      schema
                      buffer
                      headerSize
                      slotSize
                      offsetMap
                      slotNo
                      fieldName
                      value
              | _ -> failwith "Closed page"
      Close =
          fun () ->
              BTreePage.close txBuffer currentBuffer
              currentBuffer <- None }
