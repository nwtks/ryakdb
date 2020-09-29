module RyakDB.Table.TableFile

open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Table
open RyakDB.Storage.File
open RyakDB.Storage.Page
open RyakDB.Buffer.Buffer
open RyakDB.Buffer.TransactionBuffer
open RyakDB.Concurrency.TransactionConcurrency
open RyakDB.Recovery.TransactionRecovery
open RyakDB.Table.SlottedPage

type TableFile =
    { GetVal: string -> DbConstant
      SetVal: string -> DbConstant -> unit
      CurrentRecordId: unit -> RecordId
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
    { TxBuffer: TransactionBuffer
      TxConcurrency: TransactionConcurrency
      TxRecovery: TransactionRecovery
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

    let getVal offset dbType state =
        if not (FileManager.isTempFile state.FileName)
        then state.TxConcurrency.ReadBlock state.BlockId
        state.CurrentBuffer.GetVal offset dbType

    let setVal offset value state =
        if not (FileManager.isTempFile state.FileName)
        then state.TxConcurrency.ModifyBlock state.BlockId
        state.TxRecovery.LogSetVal state.CurrentBuffer offset value
        |> state.CurrentBuffer.SetVal offset value

    let getOffsetLastDeletedBlockId state =
        state
        |> getVal OffsetLastDeletedBlockId BigIntDbType
        |> DbConstant.toLong

    let getOffsetLastDeletedSlotNo state =
        state
        |> getVal OffsetLastDeletedSlotNo IntDbType
        |> DbConstant.toInt

    let getOffsetTailBlockId state =
        state
        |> getVal OffsetTailBlockId BigIntDbType
        |> DbConstant.toLong

    let getOffsetTailSlotNo state =
        state
        |> getVal OffsetTailSlotNo IntDbType
        |> DbConstant.toInt

    let hasDataRecords state = state |> getOffsetTailBlockId <> -1L

    let hasDeletedSlots state =
        state |> getOffsetLastDeletedBlockId <> -1L

    let getLastDeletedRecordId state =
        RecordId.newBlockRecordId
            (state |> getOffsetLastDeletedSlotNo)
            state.FileName
            (state |> getOffsetLastDeletedBlockId)

    let setLastDeletedRecordId (RecordId (slotNo, BlockId (_, blockNo))) state =
        state
        |> setVal OffsetLastDeletedBlockId (BigIntDbConstant blockNo)
        state
        |> setVal OffsetLastDeletedSlotNo (IntDbConstant slotNo)

    let getTailRecordId state =
        RecordId.newBlockRecordId (state |> getOffsetTailSlotNo) state.FileName (state |> getOffsetTailBlockId)

    let setTailRecordId (RecordId (slotNo, BlockId (_, blockNo))) state =
        state
        |> setVal OffsetTailBlockId (BigIntDbConstant blockNo)
        state
        |> setVal OffsetTailSlotNo (IntDbConstant slotNo)

let newFileHeaderPage txBuffer txConcurrency txRecovery fileName =
    let blockId = BlockId.newBlockId fileName 0L
    { TxBuffer = txBuffer
      TxConcurrency = txConcurrency
      TxRecovery = txRecovery
      FileName = fileName
      BlockId = blockId
      CurrentBuffer = txBuffer.Pin blockId }

module FileHeaderFormatter =
    let format buffer =
        BigIntDbConstant -1L
        |> buffer.SetValue FileHeaderPage.OffsetLastDeletedBlockId
        IntDbConstant -1
        |> buffer.SetValue FileHeaderPage.OffsetLastDeletedSlotNo
        BigIntDbConstant -1L
        |> buffer.SetValue FileHeaderPage.OffsetTailBlockId
        IntDbConstant -1
        |> buffer.SetValue FileHeaderPage.OffsetTailSlotNo

module TableFile =
    type TableFileState =
        { FileHeaderPage: FileHeaderPageState option
          SlottedPage: SlottedPage option
          CurrentBlockNo: int64 }

    let formatFileHeader fileMgr txBuffer txConcurrency fileName =
        txConcurrency.ModifyFile fileName
        if (fileMgr.Size fileName) = 0L then
            txBuffer.PinNew fileName FileHeaderFormatter.format
            |> txBuffer.Unpin

    let fileSize fileMgr txConcurrency (tableInfo: TableInfo) =
        if not (FileManager.isTempFile tableInfo.FileName)
        then txConcurrency.ReadFile tableInfo.FileName
        fileMgr.Size tableInfo.FileName

    let moveTo fileMgr txBuffer txConcurrency txRecovery doLog tableInfo state blockNo =
        state.SlottedPage
        |> Option.iter (fun sp -> sp.Close())

        if blockNo > fileSize fileMgr txConcurrency tableInfo then
            { state with
                  CurrentBlockNo = 0L
                  SlottedPage = None },
            false
        else
            { state with
                  CurrentBlockNo = blockNo
                  SlottedPage =
                      newSlottedPage
                          txBuffer
                          txConcurrency
                          txRecovery
                          (BlockId.newBlockId tableInfo.FileName blockNo)
                          tableInfo
                          doLog
                      |> Some },
            true

    let closeHeader txConcurrency (tableInfo: TableInfo) state =
        if Option.isSome state.FileHeaderPage then
            BlockId.newBlockId tableInfo.FileName 0L
            |> txConcurrency.LockTableFileHeader
        { state with FileHeaderPage = None }

    let close txConcurrency tableInfo state =
        state.SlottedPage
        |> Option.iter (fun sp -> sp.Close())
        { state with SlottedPage = None }
        |> closeHeader txConcurrency tableInfo

    let getVal state fieldName =
        match state.SlottedPage with
        | Some sp -> sp.GetVal fieldName
        | _ -> failwith "Must call next()"

    let setVal txReadOnly (tableInfo: TableInfo) state fieldName value =
        if txReadOnly
           && not (FileManager.isTempFile tableInfo.FileName) then
            failwith "Transaction read only"

        let fieldType = tableInfo.Schema.DbType fieldName
        let v = value |> DbConstant.castTo fieldType
        if Page.size v > Page.maxSize fieldType
        then failwith ("Size over:" + (Page.size v).ToString())

        match state.SlottedPage with
        | Some sp -> sp.SetVal fieldName v
        | _ -> failwith "Must call next()"

    let currentRecordId (tableInfo: TableInfo) state =
        match state.SlottedPage with
        | Some sp -> RecordId.newBlockRecordId (sp.CurrentSlotNo()) tableInfo.FileName state.CurrentBlockNo
        | _ -> failwith "Must call next()"

    let beforeFirst txConcurrency tableInfo state =
        { close txConcurrency tableInfo state with
              CurrentBlockNo = 0L }

    let next fileMgr txBuffer txConcurrency txRecovery doLog tableInfo state =
        let rec loopNext state =
            match state.SlottedPage
                  |> Option.map (fun sp -> sp.Next()) with
            | Some true -> state, true
            | _ ->
                let newstate, result =
                    moveTo fileMgr txBuffer txConcurrency txRecovery doLog tableInfo state (state.CurrentBlockNo + 1L)

                if result then loopNext newstate else newstate, false

        if state.CurrentBlockNo = 0L then
            let newstate, result =
                moveTo fileMgr txBuffer txConcurrency txRecovery doLog tableInfo state 1L

            if result then loopNext newstate else newstate, false
        else
            loopNext state

    let moveToRecordId fileMgr
                       txBuffer
                       txConcurrency
                       txRecovery
                       doLog
                       tableInfo
                       state
                       (RecordId (slotId, BlockId (_, blockNo)))
                       =
        let newstate, _ =
            moveTo fileMgr txBuffer txConcurrency txRecovery doLog tableInfo state blockNo

        match newstate.SlottedPage with
        | Some sp -> sp.MoveToSlotNo slotId
        | _ -> failwith "Must call next()"

        newstate

    let initHeaderForModification txBuffer txConcurrency txRecovery (tableInfo: TableInfo) state =
        let openHeaderForModification txBuffer txConcurrency txRecovery (tableInfo: TableInfo) =
            if not (FileManager.isTempFile tableInfo.FileName) then
                BlockId.newBlockId tableInfo.FileName 0L
                |> txConcurrency.LockTableFileHeader
            newFileHeaderPage txBuffer txConcurrency txRecovery tableInfo.FileName

        state.FileHeaderPage
        |> Option.defaultWith (fun () -> openHeaderForModification txBuffer txConcurrency txRecovery tableInfo)

    let insert fileMgr txBuffer txConcurrency txRecovery txReadOnly doLog tableInfo state =
        let atLastBlock state =
            (fileSize fileMgr txConcurrency tableInfo - 1L) = state.CurrentBlockNo

        let appendBlock () =
            if not (FileManager.isTempFile tableInfo.FileName)
            then txConcurrency.ModifyFile tableInfo.FileName

            let buffer =
                newSlottedPageFormatter tableInfo
                |> txBuffer.PinNew tableInfo.FileName

            txBuffer.Unpin buffer
            if not (FileManager.isTempFile tableInfo.FileName)
            then buffer.BlockId() |> txConcurrency.InsertBlock

        let rec loopAppendBlock state =
            match state.SlottedPage
                  |> Option.map (fun sp -> sp.InsertIntoNextEmptySlot()) with
            | Some false ->
                if atLastBlock state then appendBlock ()

                let newstate, _ =
                    moveTo fileMgr txBuffer txConcurrency txRecovery doLog tableInfo state (state.CurrentBlockNo + 1L)

                loopAppendBlock newstate
            | _ -> state

        let insertEmptySlot state =
            let newstate =
                if state.FileHeaderPage
                   |> Option.get
                   |> FileHeaderPage.hasDataRecords then
                    state.FileHeaderPage
                    |> Option.get
                    |> FileHeaderPage.getTailRecordId
                    |> moveToRecordId fileMgr txBuffer txConcurrency txRecovery doLog tableInfo state
                    |> loopAppendBlock
                else
                    appendBlock ()

                    let newstate, _ =
                        moveTo fileMgr txBuffer txConcurrency txRecovery doLog tableInfo state 1L

                    newstate.SlottedPage
                    |> Option.map (fun sp -> sp.InsertIntoNextEmptySlot())
                    |> ignore

                    newstate

            newstate.FileHeaderPage
            |> Option.get
            |> FileHeaderPage.setTailRecordId (currentRecordId tableInfo newstate)

            newstate

        let insertDeletedSlot state =
            let newstate =
                state.FileHeaderPage
                |> Option.get
                |> FileHeaderPage.getLastDeletedRecordId
                |> moveToRecordId fileMgr txBuffer txConcurrency txRecovery doLog tableInfo state

            newstate.SlottedPage
            |> Option.map (fun sp -> sp.InsertIntoDeletedSlot())
            |> Option.iter (fun lastDeletedSlot ->
                newstate.FileHeaderPage
                |> Option.get
                |> FileHeaderPage.setLastDeletedRecordId lastDeletedSlot)

            newstate

        if not (FileManager.isTempFile tableInfo.FileName) then
            if txReadOnly then failwith "Transaction read only"
            txConcurrency.ModifyFile tableInfo.FileName

        let fhp =
            initHeaderForModification txBuffer txConcurrency txRecovery tableInfo state

        txRecovery.LogLogicalStart() |> ignore

        let newstate =
            if fhp |> FileHeaderPage.hasDeletedSlots
            then insertDeletedSlot { state with FileHeaderPage = Some fhp }
            else insertEmptySlot { state with FileHeaderPage = Some fhp }

        let (RecordId (slotNo, BlockId (_, blockNo))) = currentRecordId tableInfo newstate
        txRecovery.LogTableFileInsertionEnd tableInfo.TableName blockNo slotNo
        |> ignore
        closeHeader txConcurrency tableInfo newstate

    let insertByRecordId fileMgr txBuffer txConcurrency txRecovery txReadOnly doLog tableInfo state recordId =
        let rec loopCurrentSlot state (currentSlot: RecordId) lastSlot =
            let (RecordId (_, BlockId (_, blockNo))) = currentSlot
            if currentSlot <> recordId && blockNo <> -1L then
                let newstate =
                    moveToRecordId fileMgr txBuffer txConcurrency txRecovery doLog tableInfo state currentSlot

                let nextSlot =
                    newstate.SlottedPage
                    |> Option.map (fun sp -> sp.GetDeletedRecordId())
                    |> Option.get

                loopCurrentSlot newstate nextSlot (Some currentSlot)
            else
                currentSlot, lastSlot

        let setHeaderLastDeletedSlotId state currentSlot =
            let newstate =
                moveToRecordId fileMgr txBuffer txConcurrency txRecovery doLog tableInfo state currentSlot

            newstate.SlottedPage
            |> Option.map (fun sp -> sp.GetDeletedRecordId())
            |> Option.iter (fun nextSlot ->
                newstate.FileHeaderPage
                |> Option.get
                |> FileHeaderPage.setLastDeletedRecordId nextSlot)

            newstate

        let setPageNextDeletedSlotId state currentSlot lastSlot =
            let newstate =
                moveToRecordId fileMgr txBuffer txConcurrency txRecovery doLog tableInfo state currentSlot

            newstate.SlottedPage
            |> Option.map (fun sp -> sp.GetDeletedRecordId())
            |> Option.map (fun nextSlot ->
                let newstate =
                    lastSlot
                    |> Option.map (moveToRecordId fileMgr txBuffer txConcurrency txRecovery doLog tableInfo newstate)
                    |> Option.defaultValue newstate

                newstate.SlottedPage
                |> Option.iter (fun sp -> sp.SetDeletedRecordId nextSlot)
                newstate)
            |> Option.defaultValue newstate

        if not (FileManager.isTempFile tableInfo.FileName) then
            if txReadOnly then failwith "Transaction read only"
            txConcurrency.ModifyFile tableInfo.FileName

        let fhp =
            initHeaderForModification txBuffer txConcurrency txRecovery tableInfo state

        txRecovery.LogLogicalStart() |> ignore

        let newstate =
            moveToRecordId
                fileMgr
                txBuffer
                txConcurrency
                txRecovery
                doLog
                tableInfo
                { state with FileHeaderPage = Some fhp }
                recordId

        if not
            (newstate.SlottedPage
             |> Option.map (fun sp -> sp.InsertIntoCurrentSlot())
             |> Option.defaultValue false) then
            failwith
                ("Specified slot: "
                 + recordId.ToString()
                 + " is in used")

        let currentSlot, lastSlot =
            loopCurrentSlot
                newstate
                (newstate.FileHeaderPage
                 |> Option.get
                 |> FileHeaderPage.getLastDeletedRecordId)
                None

        let newstate =
            let (RecordId (_, BlockId (_, blockNo))) = currentSlot
            if Option.isNone lastSlot
            then setHeaderLastDeletedSlotId newstate currentSlot
            elif blockNo <> -1L
            then setPageNextDeletedSlotId newstate currentSlot lastSlot
            else newstate

        let (RecordId (slotId, BlockId (_, blockNo))) = recordId
        txRecovery.LogTableFileInsertionEnd tableInfo.TableName blockNo slotId
        |> ignore
        closeHeader txConcurrency tableInfo newstate

    let delete txBuffer txConcurrency txRecovery txReadOnly tableInfo state =
        let deletePage state (sp: SlottedPage) =
            let (RecordId (slotNo, BlockId (_, blockNo))) = currentRecordId tableInfo state

            txRecovery.LogLogicalStart() |> ignore
            state.FileHeaderPage
            |> Option.get
            |> FileHeaderPage.getLastDeletedRecordId
            |> sp.Delete
            state.FileHeaderPage
            |> Option.get
            |> FileHeaderPage.setLastDeletedRecordId (currentRecordId tableInfo state)
            txRecovery.LogTableFileDeletionEnd tableInfo.TableName blockNo slotNo
            |> ignore

        if txReadOnly
           && not (FileManager.isTempFile tableInfo.FileName) then
            failwith "Transaction read only"

        let fhp =
            initHeaderForModification txBuffer txConcurrency txRecovery tableInfo state

        let newstate = { state with FileHeaderPage = Some fhp }
        newstate.SlottedPage
        |> Option.iter (fun sp -> deletePage newstate sp)
        closeHeader txConcurrency tableInfo newstate

    let deleteByRecordId fileMgr txBuffer txConcurrency txRecovery txReadOnly doLog tableInfo state recordId =
        moveToRecordId fileMgr txBuffer txConcurrency txRecovery doLog tableInfo state recordId
        |> delete txBuffer txConcurrency txRecovery txReadOnly tableInfo

    let remove (fileMgr: FileManager) txConcurrency (tableInfo: TableInfo) state =
        let newstate = close txConcurrency tableInfo state
        fileMgr.Delete tableInfo.FileName
        newstate

let newTableFile fileMgr txBuffer txConcurrency txRecovery txReadOnly doLog (tableInfo: TableInfo) =
    let mutable state: TableFile.TableFileState =
        { FileHeaderPage = None
          SlottedPage = None
          CurrentBlockNo = 0L }

    { GetVal = fun fieldName -> TableFile.getVal state fieldName
      SetVal = fun fieldName value -> TableFile.setVal txReadOnly tableInfo state fieldName value
      CurrentRecordId = fun () -> TableFile.currentRecordId tableInfo state
      BeforeFirst = fun () -> state <- TableFile.beforeFirst txConcurrency tableInfo state
      Next =
          fun () ->
              let newstate, result =
                  TableFile.next fileMgr txBuffer txConcurrency txRecovery doLog tableInfo state

              state <- newstate
              result
      MoveToRecordId =
          fun recordId ->
              state <- TableFile.moveToRecordId fileMgr txBuffer txConcurrency txRecovery doLog tableInfo state recordId
      Insert =
          fun () -> state <- TableFile.insert fileMgr txBuffer txConcurrency txRecovery txReadOnly doLog tableInfo state
      InsertByRecordId =
          fun recordId ->
              state <-
                  TableFile.insertByRecordId
                      fileMgr
                      txBuffer
                      txConcurrency
                      txRecovery
                      txReadOnly
                      doLog
                      tableInfo
                      state
                      recordId
      Delete = fun () -> state <- TableFile.delete txBuffer txConcurrency txRecovery txReadOnly tableInfo state
      DeleteByRecordId =
          fun recordId ->
              state <-
                  TableFile.deleteByRecordId
                      fileMgr
                      txBuffer
                      txConcurrency
                      txRecovery
                      txReadOnly
                      doLog
                      tableInfo
                      state
                      recordId
      FileSize = fun () -> TableFile.fileSize fileMgr txConcurrency tableInfo
      Close = fun () -> state <- TableFile.close txConcurrency tableInfo state
      Remove = fun () -> state <- TableFile.remove fileMgr txConcurrency tableInfo state }
