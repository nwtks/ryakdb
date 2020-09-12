module RyakDB.Table.TableFile

open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Table
open RyakDB.Table.SlottedPage
open RyakDB.Storage.File
open RyakDB.Storage.Page
open RyakDB.Buffer.Buffer
open RyakDB.Transaction

type TableFile =
    { GetVal: string -> DbConstant option
      SetVal: string -> DbConstant -> unit
      CurrentRecordId: unit -> RecordId option
      BeforeFirst: unit -> unit
      Next: unit -> bool
      MoveToRecordId: RecordId -> unit
      Insert: unit -> unit
      InsertByRecordId: RecordId -> unit
      Delete: unit -> unit
      DeleteByRecordId: RecordId -> unit
      FileSize: unit -> int64
      Close: unit -> unit
      Remove: unit -> unit }

type FileHeaderPageState =
    { Tx: Transaction
      FileName: string
      BlockId: BlockId
      CurrentBuffer: Buffer }

module FileHeaderPage =
    let OffsetLastDeletedBlockId = 0

    let OffsetLastDeletedSlotNo =
        OffsetLastDeletedBlockId + BlockId.BlockNoSize

    let OffsetTailBlockId =
        OffsetLastDeletedSlotNo + RecordId.SlotNoSize

    let OffsetTailSlotNo = OffsetTailBlockId + BlockId.BlockNoSize

    let NoBlockId = -1L
    let NoSlotNo = -1

    let inline getVal offset dbType state =
        if not (FileManager.isTempFile state.FileName)
        then state.Tx.Concurrency.ReadBlock state.BlockId
        state.CurrentBuffer.GetVal offset dbType

    let inline setVal offset value state =
        if not (FileManager.isTempFile state.FileName) then
            if state.Tx.ReadOnly then failwith "Transaction read only"
            state.Tx.Concurrency.ModifyBlock state.BlockId
        state.Tx.Recovery.LogSetVal state.CurrentBuffer offset value
        |> state.CurrentBuffer.SetVal offset value

    let inline getOffsetLastDeletedBlockId state =
        state
        |> getVal OffsetLastDeletedBlockId BigIntDbType
        |> DbConstant.toLong

    let inline getOffsetLastDeletedSlotNo state =
        state
        |> getVal OffsetLastDeletedSlotNo IntDbType
        |> DbConstant.toInt

    let inline getOffsetTailBlockId state =
        state
        |> getVal OffsetTailBlockId BigIntDbType
        |> DbConstant.toLong

    let inline getOffsetTailSlotNo state =
        state
        |> getVal OffsetTailSlotNo IntDbType
        |> DbConstant.toInt

    let inline hasDataRecords state =
        state |> getOffsetTailBlockId <> NoBlockId

    let inline hasDeletedSlots state =
        state |> getOffsetLastDeletedBlockId <> NoBlockId

    let inline getLastDeletedRecordId state =
        RecordId.newBlockRecordId
            (state |> getOffsetLastDeletedSlotNo)
            state.FileName
            (state |> getOffsetLastDeletedBlockId)

    let inline setLastDeletedRecordId (RecordId (slotNo, BlockId (_, blockNo))) state =
        state
        |> setVal OffsetLastDeletedBlockId (BigIntDbConstant blockNo)
        state
        |> setVal OffsetLastDeletedSlotNo (IntDbConstant slotNo)

    let inline getTailRecordId state =
        RecordId.newBlockRecordId (state |> getOffsetTailSlotNo) state.FileName (state |> getOffsetTailBlockId)

    let inline setTailRecordId (RecordId (slotNo, BlockId (_, blockNo))) state =
        state
        |> setVal OffsetTailBlockId (BigIntDbConstant blockNo)
        state
        |> setVal OffsetTailSlotNo (IntDbConstant slotNo)

let inline newFileHeaderPage tx fileName =
    let blockId = BlockId.newBlockId fileName 0L
    { Tx = tx
      FileName = fileName
      BlockId = blockId
      CurrentBuffer = tx.Buffer.Pin blockId }

module FileHeaderFormatter =
    let format buffer =
        BigIntDbConstant FileHeaderPage.NoBlockId
        |> buffer.SetValue FileHeaderPage.OffsetLastDeletedBlockId
        IntDbConstant FileHeaderPage.NoSlotNo
        |> buffer.SetValue FileHeaderPage.OffsetLastDeletedSlotNo
        BigIntDbConstant FileHeaderPage.NoBlockId
        |> buffer.SetValue FileHeaderPage.OffsetTailBlockId
        IntDbConstant FileHeaderPage.NoSlotNo
        |> buffer.SetValue FileHeaderPage.OffsetTailSlotNo

module TableFile =
    type TableFileState =
        { Tx: Transaction
          TableInfo: TableInfo
          FileName: string
          HeaderBlockId: BlockId
          FileHeaderPage: FileHeaderPageState option
          SlottedPage: SlottedPage option
          CurrentBlockNo: int64
          IsBeforeFirsted: bool
          DoLog: bool
          FileMgr: FileManager }

    let inline formatFileHeader fileMgr tx fileName =
        tx.Concurrency.ModifyFile fileName
        if (fileMgr.Size fileName) = 0L then
            tx.Buffer.PinNew fileName FileHeaderFormatter.format
            |> tx.Buffer.Unpin

    let inline fileSize state =
        if not (FileManager.isTempFile state.FileName)
        then state.Tx.Concurrency.ReadFile state.FileName
        state.FileMgr.Size state.FileName

    let moveTo state blockNo =
        state.SlottedPage
        |> Option.iter (fun sp -> sp.Close())
        if blockNo > fileSize state then
            state, false
        else
            { state with
                  CurrentBlockNo = blockNo
                  SlottedPage =
                      Some
                          (newSlottedPage
                              state.Tx
                               (BlockId.newBlockId state.FileName blockNo)
                               state.TableInfo
                               state.DoLog) },
            true

    let inline openHeaderForModification state =
        if not (FileManager.isTempFile state.FileName)
        then state.Tx.Concurrency.LockTableFileHeader state.HeaderBlockId
        newFileHeaderPage state.Tx state.FileName

    let inline closeHeader state =
        if Option.isSome state.FileHeaderPage then
            state.Tx.Concurrency.LockTableFileHeader state.HeaderBlockId
            { state with FileHeaderPage = None }
        else
            state

    let inline close state =
        state.SlottedPage
        |> Option.iter (fun sp -> sp.Close())
        closeHeader state

    let inline getVal state fieldName =
        state.SlottedPage
        |> Option.map (fun sp -> sp.GetVal fieldName)

    let inline setVal state fieldName value =
        if state.Tx.ReadOnly
           && not (FileManager.isTempFile state.FileName) then
            failwith "Transaction read only"

        let fieldType = state.TableInfo.Schema.DbType fieldName
        let v = value |> DbConstant.castTo fieldType
        if Page.size v > Page.maxSize fieldType then failwith "Size over"

        state.SlottedPage
        |> Option.iter (fun sp -> sp.SetVal fieldName v)

    let inline currentRecordId state =
        state.SlottedPage
        |> Option.map (fun sp -> RecordId.newBlockRecordId (sp.CurrentSlotNo()) state.FileName state.CurrentBlockNo)

    let inline beforeFirst state =
        { (close state) with
              CurrentBlockNo = 0L
              IsBeforeFirsted = true }

    let next state =
        let rec loopNext state =
            match state.SlottedPage
                  |> Option.map (fun sp -> sp.Next()) with
            | Some (true) -> state, true
            | _ ->
                let newstate, result = moveTo state (state.CurrentBlockNo + 1L)
                if result then loopNext newstate else newstate, false

        if not (state.IsBeforeFirsted) then failwith "must call beforeFirst()"

        if state.CurrentBlockNo = 0L then
            let newstate, result = moveTo state 1L
            if result then loopNext newstate else newstate, false
        else
            loopNext state

    let inline moveToRecordId state (RecordId (slotId, BlockId (_, blockNo))) =
        let newstate, _ = moveTo state blockNo
        newstate.SlottedPage
        |> Option.iter (fun sp -> sp.MoveToSlotNo slotId)
        newstate

    let inline initHeaderForModification state =
        let newstate =
            if Option.isNone state.FileHeaderPage then
                { state with
                      FileHeaderPage = Some(openHeaderForModification state) }
            else
                state

        newstate, newstate.FileHeaderPage |> Option.get

    let insert state =
        let inline atLastBlock state =
            (fileSize state - 1L) = state.CurrentBlockNo

        let appendBlock state =
            if not (FileManager.isTempFile state.FileName)
            then state.Tx.Concurrency.ModifyFile state.FileName

            let buffer =
                newSlottedPageFormatter state.TableInfo
                |> state.Tx.Buffer.PinNew state.FileName

            state.Tx.Buffer.Unpin buffer
            if not (FileManager.isTempFile state.FileName)
            then state.Tx.Concurrency.InsertBlock(buffer.BlockId())

        let rec loopAppendBlock state =
            match state.SlottedPage
                  |> Option.map (fun sp -> sp.InsertIntoNextEmptySlot()) with
            | Some (false) ->
                if atLastBlock state then appendBlock state
                let newstate, _ = moveTo state (state.CurrentBlockNo + 1L)
                loopAppendBlock newstate
            | _ -> state

        let insertEmptySlot state fhp =
            let newstate =
                if fhp |> FileHeaderPage.hasDataRecords then
                    loopAppendBlock
                        (fhp
                         |> FileHeaderPage.getTailRecordId
                         |> moveToRecordId state)
                else
                    appendBlock state
                    let newstate, _ = moveTo state 1L
                    newstate.SlottedPage
                    |> Option.map (fun sp -> sp.InsertIntoNextEmptySlot())
                    |> ignore
                    newstate

            currentRecordId newstate
            |> Option.iter (fun tailSolt -> fhp |> FileHeaderPage.setTailRecordId tailSolt)
            newstate

        let insertDeletedSlot state fhp =
            let newstate =
                moveToRecordId state (fhp |> FileHeaderPage.getLastDeletedRecordId)

            newstate.SlottedPage
            |> Option.map (fun sp -> sp.InsertIntoDeletedSlot())
            |> Option.iter (fun lastDeletedSlot ->
                fhp
                |> FileHeaderPage.setLastDeletedRecordId lastDeletedSlot)
            newstate

        if not (FileManager.isTempFile state.FileName) then
            if state.Tx.ReadOnly then failwith "Transaction read only"
            state.Tx.Concurrency.ModifyFile state.FileName

        let newstate, fhp = initHeaderForModification state
        newstate.Tx.Recovery.LogLogicalStart() |> ignore

        let newstate =
            if fhp |> FileHeaderPage.hasDeletedSlots then
                insertDeletedSlot newstate fhp
            else
                insertEmptySlot newstate fhp

        currentRecordId newstate
        |> Option.iter (fun (RecordId (slotNo, BlockId (_, blockNo))) ->
            newstate.Tx.Recovery.LogTableFileInsertionEnd newstate.TableInfo.TableName blockNo slotNo
            |> ignore)
        closeHeader newstate

    let insertByRecordId state recordId =
        let rec loopCurrentSlot state (currentSlot: RecordId) lastSlot =
            let (RecordId (_, BlockId (_, blockNo))) = currentSlot
            if currentSlot
               <> recordId
               && blockNo <> FileHeaderPage.NoBlockId then
                let newstate = moveToRecordId state currentSlot

                let nextSlot =
                    newstate.SlottedPage
                    |> Option.map (fun sp -> sp.GetDeletedRecordId())
                    |> Option.get

                loopCurrentSlot newstate nextSlot (Some currentSlot)
            else
                currentSlot, lastSlot

        let setHeaderLastDeletedSlotId state fhp currentSlot =
            let newstate = moveToRecordId state currentSlot
            newstate.SlottedPage
            |> Option.map (fun sp -> sp.GetDeletedRecordId())
            |> Option.iter (fun nextSlot ->
                fhp
                |> FileHeaderPage.setLastDeletedRecordId nextSlot)
            newstate

        let setPageNextDeletedSlotId state currentSlot lastSlot =
            let newstate = moveToRecordId state currentSlot
            newstate.SlottedPage
            |> Option.map (fun sp -> sp.GetDeletedRecordId())
            |> Option.map (fun nextSlot ->
                let newstate =
                    lastSlot
                    |> Option.map (moveToRecordId newstate)
                    |> Option.defaultValue newstate

                newstate.SlottedPage
                |> Option.iter (fun sp -> sp.SetDeletedRecordId nextSlot)
                newstate)
            |> Option.defaultValue newstate

        if not (FileManager.isTempFile state.FileName) then
            if state.Tx.ReadOnly then failwith "Transaction read only"
            state.Tx.Concurrency.ModifyFile state.FileName

        let newstate, fhp = initHeaderForModification state
        newstate.Tx.Recovery.LogLogicalStart() |> ignore
        let newstate = moveToRecordId newstate recordId

        if not
            (newstate.SlottedPage
             |> Option.map (fun sp -> sp.InsertIntoTheCurrentSlot())
             |> Option.defaultValue false) then
            failwith
                ("the specified slot: "
                 + recordId.ToString()
                 + " is in used")

        let currentSlot, lastSlot =
            loopCurrentSlot newstate (fhp |> FileHeaderPage.getLastDeletedRecordId) None

        let newstate =
            let (RecordId (_, BlockId (_, blockNo))) = currentSlot
            if Option.isNone lastSlot
            then setHeaderLastDeletedSlotId newstate fhp currentSlot
            elif blockNo <> FileHeaderPage.NoBlockId
            then setPageNextDeletedSlotId newstate currentSlot lastSlot
            else newstate

        let (RecordId (slotId, BlockId (_, blockNo))) = recordId
        newstate.Tx.Recovery.LogTableFileInsertionEnd newstate.TableInfo.TableName blockNo slotId
        |> ignore
        closeHeader newstate

    let delete state =
        let delete state fhp (sp: SlottedPage) =
            let (RecordId (slotNo, BlockId (_, blockNo))) = currentRecordId state |> Option.get
            state.Tx.Recovery.LogLogicalStart() |> ignore
            sp.Delete(fhp |> FileHeaderPage.getLastDeletedRecordId)
            fhp
            |> FileHeaderPage.setLastDeletedRecordId (currentRecordId state |> Option.get)
            state.Tx.Recovery.LogTableFileDeletionEnd state.TableInfo.TableName blockNo slotNo
            |> ignore

        if state.Tx.ReadOnly
           && not (FileManager.isTempFile state.FileName) then
            failwith "Transaction read only"

        let newstate, fhp = initHeaderForModification state
        newstate.SlottedPage
        |> Option.iter (fun sp -> delete newstate fhp sp)
        closeHeader newstate

    let inline deleteByRecordId state recordId = moveToRecordId state recordId |> delete

    let inline remove state =
        let newstate = close state
        newstate.FileMgr.Delete newstate.FileName
        newstate

let newTableFile fileMgr tx doLog (tableInfo: TableInfo) =
    let fileName = tableInfo.FileName

    let mutable state: TableFile.TableFileState =
        { Tx = tx
          TableInfo = tableInfo
          FileName = fileName
          HeaderBlockId = BlockId.newBlockId fileName 0L
          FileHeaderPage = None
          SlottedPage = None
          CurrentBlockNo = 0L
          IsBeforeFirsted = false
          DoLog = doLog
          FileMgr = fileMgr }

    { GetVal = fun fieldName -> TableFile.getVal state fieldName
      SetVal = fun fieldName value -> TableFile.setVal state fieldName value
      CurrentRecordId = fun () -> TableFile.currentRecordId state
      BeforeFirst = fun () -> state <- TableFile.beforeFirst state
      Next =
          fun () ->
              let newstate, result = TableFile.next state
              state <- newstate
              result
      MoveToRecordId = fun recordId -> state <- TableFile.moveToRecordId state recordId
      Insert = fun () -> state <- TableFile.insert state
      InsertByRecordId = fun recordId -> state <- TableFile.insertByRecordId state recordId
      Delete = fun () -> state <- TableFile.delete state
      DeleteByRecordId = fun recordId -> state <- TableFile.deleteByRecordId state recordId
      FileSize = fun () -> TableFile.fileSize state
      Close = fun () -> state <- TableFile.close state
      Remove = fun () -> state <- TableFile.remove state }
