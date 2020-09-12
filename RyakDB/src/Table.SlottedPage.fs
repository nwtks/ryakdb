module RyakDB.Table.SlottedPage

open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Table
open RyakDB.Storage.File
open RyakDB.Storage.Page
open RyakDB.Buffer.Buffer
open RyakDB.Transaction

type SlottedPage =
    { Close: unit -> unit
      Next: unit -> bool
      GetVal: string -> DbConstant
      SetVal: string -> DbConstant -> unit
      Delete: RecordId -> unit
      InsertIntoTheCurrentSlot: unit -> bool
      InsertIntoNextEmptySlot: unit -> bool
      InsertIntoDeletedSlot: unit -> RecordId
      MoveToSlotNo: int32 -> unit
      CurrentSlotNo: unit -> int32
      BlockId: unit -> BlockId
      GetDeletedRecordId: unit -> RecordId
      SetDeletedRecordId: RecordId -> unit }

module SlottedPage =
    type SlottedPageState =
        { Tx: Transaction
          TableInfo: TableInfo
          BlockId: BlockId
          CurrentBuffer: Buffer
          CurrentSlotNo: int32
          SlotSize: int32
          OffsetMap: Map<string, int32>
          DoLog: bool }

    let DeletedSlotSize =
        BlockId.BlockNoSize + RecordId.SlotNoSize

    let FlagSize = 4

    let EmptyConst = IntDbConstant(0)
    let InUseConst = IntDbConstant(1)

    let inline currentPosition state = state.CurrentSlotNo * state.SlotSize

    let inline fieldPosition fieldName state =
        (state |> currentPosition)
        + FlagSize
        + state.OffsetMap.[fieldName]

    let getValue offset dbType state =
        let (BlockId (fileName, _)) = state.BlockId
        if not (FileManager.isTempFile fileName) then
            RecordId.newRecordId state.CurrentSlotNo state.BlockId
            |> state.Tx.Concurrency.ReadRecord
        state.CurrentBuffer.GetVal offset dbType

    let setValue offset value state =
        let (BlockId (fileName, _)) = state.BlockId
        if not (FileManager.isTempFile fileName) then
            if state.Tx.ReadOnly then failwith "Transaction read only"
            state.Tx.Concurrency.ModifyFile fileName
        if state.DoLog
        then state.Tx.Recovery.LogSetVal state.CurrentBuffer offset value
        else None
        |> state.CurrentBuffer.SetVal offset value

    let searchFor flag state =
        let inline isValidSlot state =
            (state |> currentPosition)
            + state.SlotSize
            <= state.CurrentBuffer.BufferSize

        let rec loopSearchFor state =
            let newstate: SlottedPageState =
                { state with
                      CurrentSlotNo = state.CurrentSlotNo + 1 }

            if isValidSlot newstate then
                if newstate
                   |> getValue (newstate |> currentPosition) IntDbType = flag then
                    newstate, true
                else
                    loopSearchFor newstate
            else
                newstate, false

        loopSearchFor state

    let inline getVal fieldName state =
        state
        |> getValue (state |> fieldPosition fieldName) (state.TableInfo.Schema.DbType fieldName)

    let inline setVal fieldName value state =
        state
        |> setValue (state |> fieldPosition fieldName) value

    let inline getDeletedRecordId state =
        let position = (state |> currentPosition) + FlagSize
        let (BlockId (fileName, _)) = state.BlockId
        RecordId.newBlockRecordId
            (state
             |> getValue (position + BlockId.BlockNoSize) IntDbType
             |> DbConstant.toInt)
            fileName
            (state
             |> getValue position BigIntDbType
             |> DbConstant.toLong)

    let setDeletedRecordId (RecordId (slotNo, BlockId (_, blockNo))) state =
        let position = (state |> currentPosition) + FlagSize
        state
        |> setValue position (BigIntDbConstant blockNo)
        state
        |> setValue (position + BlockId.BlockNoSize) (IntDbConstant slotNo)

    let inline next state = state |> searchFor InUseConst

    let inline moveToSlotNo slotNo state: SlottedPageState = { state with CurrentSlotNo = slotNo }

    let insertIntoNextEmptySlot state =
        let newstate, found = state |> searchFor EmptyConst
        if found then
            newstate
            |> setValue (newstate |> currentPosition) InUseConst
        newstate, found

    let insertIntoTheCurrentSlot state =
        let isEmpty =
            state
            |> getValue (state |> currentPosition) IntDbType = EmptyConst

        if isEmpty then
            state
            |> setValue (state |> currentPosition) InUseConst
        isEmpty

    let insertIntoDeletedSlot state =
        let nextDeletedSlot = state |> getDeletedRecordId
        state
        |> setDeletedRecordId (RecordId.newBlockRecordId 0 "" 0L)
        state
        |> setValue (state |> currentPosition) InUseConst
        nextDeletedSlot

    let delete nextDeletedSlot state =
        state
        |> setValue (state |> currentPosition) EmptyConst
        state |> setDeletedRecordId nextDeletedSlot

    let inline close state =
        state.CurrentBuffer |> state.Tx.Buffer.Unpin

    let offsetMap tableInfo =
        let offsetMap, _ =
            tableInfo.Schema.Fields()
            |> List.fold (fun (map: Map<string, int32>, pos) field ->
                map.Add(field, pos),
                pos
                + (tableInfo.Schema.DbType field |> Page.maxSize)) (Map.empty, 0)

        offsetMap

    let slotSize tableInfo =
        let pos =
            tableInfo.Schema.Fields()
            |> List.fold (fun pos field ->
                pos
                + (tableInfo.Schema.DbType field |> Page.maxSize)) 0

        FlagSize
        + if pos < DeletedSlotSize then DeletedSlotSize else pos

let newSlottedPage tx blockId tableInfo doLog =
    let mutable state: SlottedPage.SlottedPageState option =
        Some
            ({ Tx = tx
               TableInfo = tableInfo
               BlockId = blockId
               CurrentBuffer = tx.Buffer.Pin blockId
               CurrentSlotNo = -1
               SlotSize = SlottedPage.slotSize tableInfo
               OffsetMap = SlottedPage.offsetMap tableInfo
               DoLog = doLog })

    { Close =
          fun () ->
              match state with
              | Some (st) -> SlottedPage.close st
              | _ -> ()
              state <- None
      Next =
          fun () ->
              match state with
              | Some (st) ->
                  let newstate, result = SlottedPage.next st
                  state <- Some(newstate)
                  result
              | _ -> failwith "closed page"
      GetVal =
          fun fieldName ->
              match state with
              | Some (st) -> SlottedPage.getVal fieldName st
              | _ -> failwith "closed page"
      SetVal =
          fun fieldName value ->
              match state with
              | Some (st) -> SlottedPage.setVal fieldName value st
              | _ -> failwith "closed page"
      Delete =
          fun nextDeletedSlot ->
              match state with
              | Some (st) -> SlottedPage.delete nextDeletedSlot st
              | _ -> failwith "closed page"
      InsertIntoTheCurrentSlot =
          fun () ->
              match state with
              | Some (st) -> SlottedPage.insertIntoTheCurrentSlot st
              | _ -> failwith "closed page"
      InsertIntoNextEmptySlot =
          fun () ->
              match state with
              | Some (st) ->
                  let newstate, result = SlottedPage.insertIntoNextEmptySlot st
                  state <- Some(newstate)
                  result
              | _ -> failwith "closed page"
      InsertIntoDeletedSlot =
          fun () ->
              match state with
              | Some (st) -> SlottedPage.insertIntoDeletedSlot st
              | _ -> failwith "closed page"
      MoveToSlotNo =
          fun slotId ->
              match state with
              | Some (st) -> state <- Some(SlottedPage.moveToSlotNo slotId st)
              | _ -> failwith "closed page"
      CurrentSlotNo =
          fun () ->
              match state with
              | Some (st) -> st.CurrentSlotNo
              | _ -> failwith "closed page"
      BlockId =
          fun () ->
              match state with
              | Some (st) -> st.BlockId
              | _ -> failwith "closed page"
      GetDeletedRecordId =
          fun () ->
              match state with
              | Some (st) -> SlottedPage.getDeletedRecordId st
              | _ -> failwith "closed page"
      SetDeletedRecordId =
          fun recordId ->
              match state with
              | Some (st) -> SlottedPage.setDeletedRecordId recordId st
              | _ -> failwith "closed page" }

module SlottedPageFormatter =
    let makeDefaultSlottedPage tableInfo (offsetMap: Map<string, int32>) buffer position =
        tableInfo.Schema.Fields()
        |> List.iter (fun field ->
            buffer.SetValue
                (position + 4 + offsetMap.[field])
                (tableInfo.Schema.DbType field
                 |> DbConstant.defaultConstant))

let newSlottedPageFormatter tableInfo =
    let offsetMap = SlottedPage.offsetMap tableInfo
    let slotSize = SlottedPage.slotSize tableInfo
    fun buffer ->
        for pos in 0 .. slotSize .. (buffer.BufferSize - slotSize) do
            buffer.SetValue pos SlottedPage.EmptyConst
            SlottedPageFormatter.makeDefaultSlottedPage tableInfo offsetMap buffer pos
