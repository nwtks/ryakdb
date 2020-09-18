module RyakDB.Table.SlottedPage

open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Table
open RyakDB.Storage.File
open RyakDB.Storage.Page
open RyakDB.Buffer.Buffer
open RyakDB.Buffer.TransactionBuffer
open RyakDB.Concurrency.TransactionConcurrency
open RyakDB.Recovery.TransactionRecovery

type SlottedPage =
    { Close: unit -> unit
      Next: unit -> bool
      GetVal: string -> DbConstant
      SetVal: string -> DbConstant -> unit
      Delete: RecordId -> unit
      InsertIntoCurrentSlot: unit -> bool
      InsertIntoNextEmptySlot: unit -> bool
      InsertIntoDeletedSlot: unit -> RecordId
      MoveToSlotNo: int32 -> unit
      CurrentSlotNo: unit -> int32
      BlockId: unit -> BlockId
      GetDeletedRecordId: unit -> RecordId
      SetDeletedRecordId: RecordId -> unit }

module SlottedPage =
    let DeletedSlotSize =
        BlockId.BlockNoSize + RecordId.SlotNoSize

    let FlagSize = 4

    let EmptyConst = IntDbConstant(0)
    let InUseConst = IntDbConstant(1)

    let offsetMap schema =
        let offsetMap, _ =
            schema.Fields()
            |> List.fold (fun (map: Map<string, int32>, pos) field ->
                map.Add(field, pos), pos + (schema.DbType field |> Page.maxSize)) (Map.empty, 0)

        offsetMap

    let slotSize schema =
        let pos =
            schema.Fields()
            |> List.fold (fun pos field -> pos + (schema.DbType field |> Page.maxSize)) 0

        FlagSize
        + if pos < DeletedSlotSize then DeletedSlotSize else pos

    let inline currentPosition slotSize currentSlotNo = currentSlotNo * slotSize

    let inline fieldPosition (offsetMap: Map<string, int32>) slotSize currentSlotNo fieldName =
        (currentPosition slotSize currentSlotNo)
        + FlagSize
        + offsetMap.[fieldName]

    let getValue txConcurrency (currentBuffer: Buffer) blockId currentSlotNo offset dbType =
        let (BlockId (fileName, _)) = blockId
        if not (FileManager.isTempFile fileName) then
            RecordId.newRecordId currentSlotNo blockId
            |> txConcurrency.ReadRecord
        currentBuffer.GetVal offset dbType

    let setValue txConcurrency txRecovery doLog currentBuffer blockId offset value =
        let (BlockId (fileName, _)) = blockId
        if not (FileManager.isTempFile fileName) then txConcurrency.ModifyFile fileName
        if doLog
        then txRecovery.LogSetVal currentBuffer offset value
        else None
        |> currentBuffer.SetVal offset value

    let searchFor txConcurrency currentBuffer blockId slotSize currentSlotNo flag =
        let inline isValidSlot slotNo =
            (currentPosition slotSize slotNo)
            + slotSize
            <= currentBuffer.BufferSize

        let rec loopSearchFor blockId slotNo =
            let newSlotNo = slotNo + 1
            if isValidSlot newSlotNo then
                if getValue txConcurrency currentBuffer blockId newSlotNo (currentPosition slotSize newSlotNo) IntDbType =
                    flag then
                    newSlotNo, true
                else
                    loopSearchFor blockId newSlotNo
            else
                newSlotNo, false

        loopSearchFor blockId currentSlotNo

    let inline getVal txConcurrency schema currentBuffer blockId offsetMap slotSize currentSlotNo fieldName =
        getValue
            txConcurrency
            currentBuffer
            blockId
            currentSlotNo
            (fieldPosition offsetMap slotSize currentSlotNo fieldName)
            (schema.DbType fieldName)

    let inline setVal txConcurrency
                      txRecovery
                      doLog
                      currentBuffer
                      blockId
                      offsetMap
                      slotSize
                      currentSlotNo
                      fieldName
                      value
                      =
        setValue
            txConcurrency
            txRecovery
            doLog
            currentBuffer
            blockId
            (fieldPosition offsetMap slotSize currentSlotNo fieldName)
            value

    let inline getDeletedRecordId txConcurrency currentBuffer blockId slotSize currentSlotNo =
        let position =
            (currentPosition slotSize currentSlotNo)
            + FlagSize

        let (BlockId (fileName, _)) = blockId
        RecordId.newBlockRecordId
            (getValue txConcurrency currentBuffer blockId currentSlotNo (position + BlockId.BlockNoSize) IntDbType
             |> DbConstant.toInt)
            fileName
            (getValue txConcurrency currentBuffer blockId currentSlotNo position BigIntDbType
             |> DbConstant.toLong)

    let setDeletedRecordId txConcurrency
                           txRecovery
                           doLog
                           currentBuffer
                           blockId
                           slotSize
                           currentSlotNo
                           (RecordId (slotNo, BlockId (_, blockNo)))
                           =
        let position =
            (currentPosition slotSize currentSlotNo)
            + FlagSize

        setValue txConcurrency txRecovery doLog currentBuffer blockId position (BigIntDbConstant blockNo)
        setValue
            txConcurrency
            txRecovery
            doLog
            currentBuffer
            blockId
            (position + BlockId.BlockNoSize)
            (IntDbConstant slotNo)

    let inline next txConcurrency currentBuffer blockId slotSize currentSlotNo =
        searchFor txConcurrency currentBuffer blockId slotSize currentSlotNo InUseConst

    let insertIntoNextEmptySlot txConcurrency txRecovery doLog currentBuffer blockId slotSize currentSlotNo =
        let newSlotNo, found =
            searchFor txConcurrency currentBuffer blockId slotSize currentSlotNo EmptyConst

        if found
        then setValue
                 txConcurrency
                 txRecovery
                 doLog
                 currentBuffer
                 blockId
                 (currentPosition slotSize newSlotNo)
                 InUseConst
        newSlotNo, found

    let insertIntoCurrentSlot txConcurrency txRecovery doLog currentBuffer blockId slotSize currentSlotNo =
        let isEmpty =
            getValue
                txConcurrency
                currentBuffer
                blockId
                currentSlotNo
                (currentPosition slotSize currentSlotNo)
                IntDbType = EmptyConst

        if isEmpty then
            setValue
                txConcurrency
                txRecovery
                doLog
                currentBuffer
                blockId
                (currentPosition slotSize currentSlotNo)
                InUseConst
        isEmpty

    let insertIntoDeletedSlot txConcurrency txRecovery doLog currentBuffer blockId slotSize currentSlotNo =
        let nextDeletedSlot =
            getDeletedRecordId txConcurrency currentBuffer blockId slotSize currentSlotNo

        setDeletedRecordId
            txConcurrency
            txRecovery
            doLog
            currentBuffer
            blockId
            slotSize
            currentSlotNo
            (RecordId.newBlockRecordId 0 "" 0L)
        setValue
            txConcurrency
            txRecovery
            doLog
            currentBuffer
            blockId
            (currentPosition slotSize currentSlotNo)
            InUseConst
        nextDeletedSlot

    let delete txConcurrency txRecovery doLog currentBuffer blockId slotSize currentSlotNo nextDeletedSlot =
        setValue
            txConcurrency
            txRecovery
            doLog
            currentBuffer
            blockId
            (currentPosition slotSize currentSlotNo)
            EmptyConst
        setDeletedRecordId txConcurrency txRecovery doLog currentBuffer blockId slotSize currentSlotNo nextDeletedSlot

    let inline close txBuffer currentBuffer = currentBuffer |> txBuffer.Unpin

let newSlottedPage txBuffer txConcurrency txRecovery blockId tableInfo doLog =
    let slotSize = SlottedPage.slotSize tableInfo.Schema
    let offsetMap = SlottedPage.offsetMap tableInfo.Schema
    let currentBuffer = txBuffer.Pin blockId

    let mutable currentSlotNo = Some(-1)
    { Close =
          fun () ->
              match currentSlotNo with
              | Some (_) -> SlottedPage.close txBuffer currentBuffer
              | _ -> ()
              currentSlotNo <- None
      Next =
          fun () ->
              match currentSlotNo with
              | Some (slotNo) ->
                  let newSlotNo, result =
                      SlottedPage.next txConcurrency currentBuffer blockId slotSize slotNo

                  currentSlotNo <- Some(newSlotNo)
                  result
              | _ -> failwith "Closed page"
      GetVal =
          fun fieldName ->
              match currentSlotNo with
              | Some (slotNo) ->
                  SlottedPage.getVal
                      txConcurrency
                      tableInfo.Schema
                      currentBuffer
                      blockId
                      offsetMap
                      slotSize
                      slotNo
                      fieldName
              | _ -> failwith "Closed page"
      SetVal =
          fun fieldName value ->
              match currentSlotNo with
              | Some (slotNo) ->
                  SlottedPage.setVal
                      txConcurrency
                      txRecovery
                      doLog
                      currentBuffer
                      blockId
                      offsetMap
                      slotSize
                      slotNo
                      fieldName
                      value
              | _ -> failwith "Closed page"
      Delete =
          fun nextDeletedSlot ->
              match currentSlotNo with
              | Some (slotNo) ->
                  SlottedPage.delete
                      txConcurrency
                      txRecovery
                      doLog
                      currentBuffer
                      blockId
                      slotSize
                      slotNo
                      nextDeletedSlot
              | _ -> failwith "Closed page"
      InsertIntoCurrentSlot =
          fun () ->
              match currentSlotNo with
              | Some (slotNo) ->
                  SlottedPage.insertIntoCurrentSlot txConcurrency txRecovery doLog currentBuffer blockId slotSize slotNo
              | _ -> failwith "Closed page"
      InsertIntoNextEmptySlot =
          fun () ->
              match currentSlotNo with
              | Some (slotNo) ->
                  let newSlotNo, result =
                      SlottedPage.insertIntoNextEmptySlot
                          txConcurrency
                          txRecovery
                          doLog
                          currentBuffer
                          blockId
                          slotSize
                          slotNo

                  currentSlotNo <- Some(newSlotNo)
                  result
              | _ -> failwith "Closed page"
      InsertIntoDeletedSlot =
          fun () ->
              match currentSlotNo with
              | Some (slotNo) ->
                  SlottedPage.insertIntoDeletedSlot txConcurrency txRecovery doLog currentBuffer blockId slotSize slotNo
              | _ -> failwith "Closed page"
      MoveToSlotNo = fun slotNo -> currentSlotNo <- Some(slotNo)
      CurrentSlotNo =
          fun () ->
              match currentSlotNo with
              | Some (slotNo) -> slotNo
              | _ -> failwith "Closed page"
      BlockId = fun () -> blockId
      GetDeletedRecordId =
          fun () ->
              match currentSlotNo with
              | Some (slotNo) -> SlottedPage.getDeletedRecordId txConcurrency currentBuffer blockId slotSize slotNo
              | _ -> failwith "Closed page"
      SetDeletedRecordId =
          fun recordId ->
              match currentSlotNo with
              | Some (slotNo) ->
                  SlottedPage.setDeletedRecordId
                      txConcurrency
                      txRecovery
                      doLog
                      currentBuffer
                      blockId
                      slotSize
                      slotNo
                      recordId
              | _ -> failwith "Closed page" }

module SlottedPageFormatter =
    let makeDefaultSlottedPage tableInfo (offsetMap: Map<string, int32>) buffer position =
        tableInfo.Schema.Fields()
        |> List.iter (fun field ->
            buffer.SetValue
                (position + 4 + offsetMap.[field])
                (tableInfo.Schema.DbType field
                 |> DbConstant.defaultConstant))

let newSlottedPageFormatter tableInfo =
    let offsetMap = SlottedPage.offsetMap tableInfo.Schema
    let slotSize = SlottedPage.slotSize tableInfo.Schema
    fun buffer ->
        for pos in 0 .. slotSize .. (buffer.BufferSize - slotSize) do
            buffer.SetValue pos SlottedPage.EmptyConst
            SlottedPageFormatter.makeDefaultSlottedPage tableInfo offsetMap buffer pos