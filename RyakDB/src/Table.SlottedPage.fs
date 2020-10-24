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
      UndoDelete: unit -> bool
      InsertIntoNextEmptySlot: unit -> bool
      InsertIntoDeletedSlot: unit -> RecordId
      MoveToSlotNo: int32 -> unit
      CurrentSlotNo: unit -> int32
      BlockId: BlockId
      GetDeletedRecordId: unit -> RecordId
      SetDeletedRecordId: RecordId -> unit }
    interface System.IDisposable with
        member this.Dispose() = this.Close()

module SlottedPage =
    let DeletedSlotSize =
        BlockId.BlockNoSize + RecordId.SlotNoSize

    let FlagSize = 4

    let EmptyConst = IntDbConstant 0
    let InUseConst = IntDbConstant 1

    let offsetMap schema =
        schema.Fields()
        |> List.fold (fun (map, pos) field -> Map.add field pos map, pos + (schema.DbType field |> Page.maxSize))
               (Map.empty, 0)
        |> fst

    let slotSize schema =
        let pos =
            schema.Fields()
            |> List.fold (fun pos field -> pos + (schema.DbType field |> Page.maxSize)) 0

        FlagSize
        + if pos < DeletedSlotSize then DeletedSlotSize else pos

    let currentPosition slotSize currentSlotNo = currentSlotNo * slotSize

    let fieldPosition offsetMap slotSize currentSlotNo fieldName =
        (currentPosition slotSize currentSlotNo)
        + FlagSize
        + (Map.find fieldName offsetMap)

    let getValue txConcurrency (currentBuffer: Buffer) blockId currentSlotNo offset dbType =
        if not (BlockId.fileName blockId |> FileService.isTempFile) then
            RecordId.newRecordId currentSlotNo blockId
            |> txConcurrency.ReadRecord

        currentBuffer.GetVal offset dbType

    let setValue txConcurrency txRecovery doLog currentBuffer blockId offset value =
        if not (BlockId.fileName blockId |> FileService.isTempFile) then
            BlockId.fileName blockId
            |> txConcurrency.ModifyFile

        if doLog
        then txRecovery.LogSetVal currentBuffer offset value
        else None
        |> currentBuffer.SetVal offset value

    let searchFor txConcurrency currentBuffer blockId slotSize currentSlotNo flag =
        let isValidSlot slotNo =
            (currentPosition slotSize slotNo)
            + slotSize
            <= currentBuffer.BufferSize

        let rec searchSlotNo blockId slotNo =
            let newSlotNo = slotNo + 1
            if isValidSlot newSlotNo then
                if getValue txConcurrency currentBuffer blockId newSlotNo (currentPosition slotSize newSlotNo) IntDbType =
                    flag then
                    newSlotNo, true
                else
                    searchSlotNo blockId newSlotNo
            else
                newSlotNo, false

        searchSlotNo blockId currentSlotNo

    let getVal txConcurrency schema currentBuffer blockId offsetMap slotSize currentSlotNo fieldName =
        getValue
            txConcurrency
            currentBuffer
            blockId
            currentSlotNo
            (fieldPosition offsetMap slotSize currentSlotNo fieldName)
            (schema.DbType fieldName)

    let setVal txConcurrency txRecovery doLog currentBuffer blockId offsetMap slotSize currentSlotNo fieldName value =
        setValue
            txConcurrency
            txRecovery
            doLog
            currentBuffer
            blockId
            (fieldPosition offsetMap slotSize currentSlotNo fieldName)
            value

    let getDeletedRecordId txConcurrency currentBuffer blockId slotSize currentSlotNo =
        let position =
            (currentPosition slotSize currentSlotNo)
            + FlagSize

        RecordId.newBlockRecordId
            (getValue txConcurrency currentBuffer blockId currentSlotNo (position + BlockId.BlockNoSize) IntDbType
             |> DbConstant.toInt)
            (BlockId.fileName blockId)
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

        BigIntDbConstant blockNo
        |> setValue txConcurrency txRecovery doLog currentBuffer blockId position

        IntDbConstant slotNo
        |> setValue txConcurrency txRecovery doLog currentBuffer blockId (position + BlockId.BlockNoSize)

    let next txConcurrency currentBuffer blockId slotSize currentSlotNo =
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

    let insertIntoDeletedSlot txConcurrency txRecovery doLog currentBuffer blockId slotSize currentSlotNo =
        let nextDeletedSlot =
            getDeletedRecordId txConcurrency currentBuffer blockId slotSize currentSlotNo

        RecordId.newBlockRecordId 0 "" 0L
        |> setDeletedRecordId txConcurrency txRecovery doLog currentBuffer blockId slotSize currentSlotNo

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

    let undoDelete txConcurrency txRecovery doLog currentBuffer blockId slotSize currentSlotNo =
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

    let close txBuffer currentBuffer = currentBuffer |> txBuffer.Unpin

let newSlottedPage txBuffer txConcurrency txRecovery blockId schema doLog =
    let slotSize = SlottedPage.slotSize schema
    let offsetMap = SlottedPage.offsetMap schema
    let currentBuffer = txBuffer.Pin blockId

    let mutable currentSlotNo = Some -1
    { Close =
          fun () ->
              match currentSlotNo with
              | Some _ -> SlottedPage.close txBuffer currentBuffer
              | _ -> ()
              currentSlotNo <- None
      Next =
          fun () ->
              match currentSlotNo with
              | Some slotNo ->
                  let newSlotNo, result =
                      SlottedPage.next txConcurrency currentBuffer blockId slotSize slotNo

                  currentSlotNo <- Some newSlotNo
                  result
              | _ -> false
      GetVal =
          fun fieldName ->
              match currentSlotNo with
              | Some slotNo ->
                  SlottedPage.getVal txConcurrency schema currentBuffer blockId offsetMap slotSize slotNo fieldName
              | _ -> failwith "Closed page"
      SetVal =
          fun fieldName value ->
              match currentSlotNo with
              | Some slotNo ->
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
              | Some slotNo ->
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
      UndoDelete =
          fun () ->
              match currentSlotNo with
              | Some slotNo ->
                  SlottedPage.undoDelete txConcurrency txRecovery doLog currentBuffer blockId slotSize slotNo
              | _ -> failwith "Closed page"
      InsertIntoNextEmptySlot =
          fun () ->
              match currentSlotNo with
              | Some slotNo ->
                  let newSlotNo, result =
                      SlottedPage.insertIntoNextEmptySlot
                          txConcurrency
                          txRecovery
                          doLog
                          currentBuffer
                          blockId
                          slotSize
                          slotNo

                  currentSlotNo <- Some newSlotNo
                  result
              | _ -> failwith "Closed page"
      InsertIntoDeletedSlot =
          fun () ->
              match currentSlotNo with
              | Some slotNo ->
                  SlottedPage.insertIntoDeletedSlot txConcurrency txRecovery doLog currentBuffer blockId slotSize slotNo
              | _ -> failwith "Closed page"
      MoveToSlotNo = fun slotNo -> currentSlotNo <- Some slotNo
      CurrentSlotNo =
          fun () ->
              match currentSlotNo with
              | Some slotNo -> slotNo
              | _ -> failwith "Closed page"
      BlockId = blockId
      GetDeletedRecordId =
          fun () ->
              match currentSlotNo with
              | Some slotNo -> SlottedPage.getDeletedRecordId txConcurrency currentBuffer blockId slotSize slotNo
              | _ -> failwith "Closed page"
      SetDeletedRecordId =
          fun recordId ->
              match currentSlotNo with
              | Some slotNo ->
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
    let makeDefaultSlottedPage schema offsetMap buffer position =
        schema.Fields()
        |> List.iter (fun field ->
            schema.DbType field
            |> DbConstant.defaultConstant
            |> buffer.SetValue(position + 4 + (Map.find field offsetMap)))

let newSlottedPageFormatter schema =
    let offsetMap = SlottedPage.offsetMap schema
    let slotSize = SlottedPage.slotSize schema
    fun buffer ->
        [ 0 .. slotSize .. buffer.BufferSize - slotSize - 1 ]
        |> List.iter (fun position ->
            buffer.SetValue position SlottedPage.EmptyConst
            SlottedPageFormatter.makeDefaultSlottedPage schema offsetMap buffer position)
