module RyakDB.Table.TablePage

open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Table
open RyakDB.Storage.File
open RyakDB.Storage.Page
open RyakDB.Buffer.Buffer
open RyakDB.Transaction

type TablePage =
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

module TablePage =
    let DeletedSlotSize =
        BlockId.BlockNoSize + RecordId.SlotNoSize

    let FlagSize = 4

    let EmptyConst = IntDbConstant 0
    let InUseConst = IntDbConstant 1

    let offsetMap schema =
        schema.Fields()
        |> List.fold (fun (map, pos) field -> map |> Map.add field pos, pos + (field |> schema.DbType |> Page.maxSize))
               (Map.empty, 0)
        |> fst

    let slotSize schema =
        let pos =
            schema.Fields()
            |> List.fold (fun pos field -> pos + (field |> schema.DbType |> Page.maxSize)) 0

        FlagSize
        + if pos < DeletedSlotSize then DeletedSlotSize else pos

    let currentPosition slotSize currentSlotNo = currentSlotNo * slotSize

    let fieldPosition offsetMap slotSize currentSlotNo fieldName =
        FlagSize
        + currentPosition slotSize currentSlotNo
        + Map.find fieldName offsetMap

    let getValue tx (currentBuffer: Buffer) blockId currentSlotNo offset dbType =
        if BlockId.fileName blockId
           |> FileService.isTempFile
           |> not then
            RecordId.newRecordId currentSlotNo blockId
            |> tx.Concurrency.ReadRecord

        currentBuffer.GetVal offset dbType

    let setValue tx doLog currentBuffer blockId offset value =
        if BlockId.fileName blockId
           |> FileService.isTempFile
           |> not then
            BlockId.fileName blockId
            |> tx.Concurrency.ModifyFile

        if doLog
        then tx.Recovery.LogSetVal currentBuffer offset value
        else None
        |> currentBuffer.SetVal offset value

    let searchFor tx currentBuffer blockId slotSize currentSlotNo flag =
        let isValidSlot slotNo =
            slotSize
            + currentPosition slotSize slotNo
            <= currentBuffer.BufferSize

        let rec searchSlotNo blockId slotNo =
            let newSlotNo = slotNo + 1
            if isValidSlot newSlotNo then
                if getValue tx currentBuffer blockId newSlotNo (currentPosition slotSize newSlotNo) IntDbType = flag
                then newSlotNo, true
                else searchSlotNo blockId newSlotNo
            else
                newSlotNo, false

        searchSlotNo blockId currentSlotNo

    let getVal tx schema currentBuffer blockId offsetMap slotSize currentSlotNo fieldName =
        getValue
            tx
            currentBuffer
            blockId
            currentSlotNo
            (fieldPosition offsetMap slotSize currentSlotNo fieldName)
            (schema.DbType fieldName)

    let setVal tx doLog currentBuffer blockId offsetMap slotSize currentSlotNo fieldName value =
        setValue tx doLog currentBuffer blockId (fieldPosition offsetMap slotSize currentSlotNo fieldName) value

    let getDeletedRecordId tx currentBuffer blockId slotSize currentSlotNo =
        let position =
            FlagSize + currentPosition slotSize currentSlotNo

        RecordId.newBlockRecordId
            (getValue tx currentBuffer blockId currentSlotNo (BlockId.BlockNoSize + position) IntDbType
             |> DbConstant.toInt)
            (BlockId.fileName blockId)
            (getValue tx currentBuffer blockId currentSlotNo position BigIntDbType
             |> DbConstant.toLong)

    let setDeletedRecordId tx
                           doLog
                           currentBuffer
                           blockId
                           slotSize
                           currentSlotNo
                           (RecordId (slotNo, BlockId (_, blockNo)))
                           =
        let position =
            FlagSize + currentPosition slotSize currentSlotNo

        BigIntDbConstant blockNo
        |> setValue tx doLog currentBuffer blockId position

        IntDbConstant slotNo
        |> setValue tx doLog currentBuffer blockId (BlockId.BlockNoSize + position)

    let next tx currentBuffer blockId slotSize currentSlotNo =
        searchFor tx currentBuffer blockId slotSize currentSlotNo InUseConst

    let insertIntoNextEmptySlot tx doLog currentBuffer blockId slotSize currentSlotNo =
        let newSlotNo, found =
            searchFor tx currentBuffer blockId slotSize currentSlotNo EmptyConst

        if found
        then setValue tx doLog currentBuffer blockId (currentPosition slotSize newSlotNo) InUseConst
        newSlotNo, found

    let insertIntoDeletedSlot tx doLog currentBuffer blockId slotSize currentSlotNo =
        let nextDeletedSlot =
            getDeletedRecordId tx currentBuffer blockId slotSize currentSlotNo

        RecordId.newBlockRecordId 0 "" 0L
        |> setDeletedRecordId tx doLog currentBuffer blockId slotSize currentSlotNo

        setValue tx doLog currentBuffer blockId (currentPosition slotSize currentSlotNo) InUseConst

        nextDeletedSlot

    let delete tx doLog currentBuffer blockId slotSize currentSlotNo nextDeletedSlot =
        setValue tx doLog currentBuffer blockId (currentPosition slotSize currentSlotNo) EmptyConst

        setDeletedRecordId tx doLog currentBuffer blockId slotSize currentSlotNo nextDeletedSlot

    let undoDelete tx doLog currentBuffer blockId slotSize currentSlotNo =
        let isEmpty =
            getValue tx currentBuffer blockId currentSlotNo (currentPosition slotSize currentSlotNo) IntDbType =
                EmptyConst

        if isEmpty
        then setValue tx doLog currentBuffer blockId (currentPosition slotSize currentSlotNo) InUseConst
        isEmpty

    let close tx currentBuffer = currentBuffer |> tx.Buffer.Unpin

let newTablePage tx blockId schema doLog =
    let slotSize = TablePage.slotSize schema
    let offsetMap = TablePage.offsetMap schema
    let currentBuffer = tx.Buffer.Pin blockId

    let mutable currentSlotNo = Some -1
    { Close =
          fun () ->
              match currentSlotNo with
              | Some _ -> TablePage.close tx currentBuffer
              | _ -> ()
              currentSlotNo <- None
      Next =
          fun () ->
              match currentSlotNo with
              | Some slotNo ->
                  let newSlotNo, result =
                      TablePage.next tx currentBuffer blockId slotSize slotNo

                  currentSlotNo <- Some newSlotNo
                  result
              | _ -> false
      GetVal =
          fun fieldName ->
              match currentSlotNo with
              | Some slotNo -> TablePage.getVal tx schema currentBuffer blockId offsetMap slotSize slotNo fieldName
              | _ -> failwith "Closed page"
      SetVal =
          fun fieldName value ->
              match currentSlotNo with
              | Some slotNo -> TablePage.setVal tx doLog currentBuffer blockId offsetMap slotSize slotNo fieldName value
              | _ -> failwith "Closed page"
      Delete =
          fun nextDeletedSlot ->
              match currentSlotNo with
              | Some slotNo -> TablePage.delete tx doLog currentBuffer blockId slotSize slotNo nextDeletedSlot
              | _ -> failwith "Closed page"
      UndoDelete =
          fun () ->
              match currentSlotNo with
              | Some slotNo -> TablePage.undoDelete tx doLog currentBuffer blockId slotSize slotNo
              | _ -> failwith "Closed page"
      InsertIntoNextEmptySlot =
          fun () ->
              match currentSlotNo with
              | Some slotNo ->
                  let newSlotNo, result =
                      TablePage.insertIntoNextEmptySlot tx doLog currentBuffer blockId slotSize slotNo

                  currentSlotNo <- Some newSlotNo
                  result
              | _ -> failwith "Closed page"
      InsertIntoDeletedSlot =
          fun () ->
              match currentSlotNo with
              | Some slotNo -> TablePage.insertIntoDeletedSlot tx doLog currentBuffer blockId slotSize slotNo
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
              | Some slotNo -> TablePage.getDeletedRecordId tx currentBuffer blockId slotSize slotNo
              | _ -> failwith "Closed page"
      SetDeletedRecordId =
          fun recordId ->
              match currentSlotNo with
              | Some slotNo -> TablePage.setDeletedRecordId tx doLog currentBuffer blockId slotSize slotNo recordId
              | _ -> failwith "Closed page" }

module TablePageFormatter =
    let makeDefaultTablePage schema offsetMap buffer position =
        schema.Fields()
        |> List.iter (fun field ->
            field
            |> schema.DbType
            |> DbConstant.defaultConstant
            |> buffer.SetValue
                (TablePage.FlagSize
                 + position
                 + Map.find field offsetMap))

let newTablePageFormatter schema =
    let offsetMap = TablePage.offsetMap schema
    let slotSize = TablePage.slotSize schema
    fun buffer ->
        [ 0 .. slotSize .. buffer.BufferSize - slotSize - 1 ]
        |> List.iter (fun position ->
            buffer.SetValue position TablePage.EmptyConst
            TablePageFormatter.makeDefaultTablePage schema offsetMap buffer position)
