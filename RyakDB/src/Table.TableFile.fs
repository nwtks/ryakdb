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
open RyakDB.Table.TablePage

type TableFile =
    { GetVal: string -> DbConstant
      SetVal: string -> DbConstant -> unit
      CurrentRecordId: unit -> RecordId
      BeforeFirst: unit -> unit
      Next: unit -> bool
      MoveToRecordId: RecordId -> unit
      Insert: unit -> unit
      Delete: unit -> unit
      UndoInsert: RecordId -> unit
      UndoDelete: RecordId -> unit
      FileSize: unit -> int64
      Close: unit -> unit
      Remove: unit -> unit }
    interface System.IDisposable with
        member this.Dispose() = this.Close()

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
        if state.FileName |> FileService.isTempFile |> not
        then state.TxConcurrency.ReadBlock state.BlockId

        state.CurrentBuffer.GetVal offset dbType

    let setVal offset value state =
        if state.FileName |> FileService.isTempFile |> not
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
          TablePage: TablePage option
          CurrentBlockNo: int64 }

    let formatFileHeader fileService txBuffer txConcurrency fileName =
        txConcurrency.ModifyFile fileName

        if fileService.Size fileName = 0L then
            txBuffer.PinNew fileName FileHeaderFormatter.format
            |> txBuffer.Unpin

    let fileSize fileService txConcurrency tableFileName =
        if tableFileName |> FileService.isTempFile |> not
        then txConcurrency.ReadFile tableFileName

        fileService.Size tableFileName

    let moveTo fileService txBuffer txConcurrency txRecovery doLog tableFileName schema state blockNo =
        state.TablePage
        |> Option.iter (fun tp -> tp.Close())

        if blockNo > fileSize fileService txConcurrency tableFileName then
            { state with
                  CurrentBlockNo = -1L
                  TablePage = None },
            false
        else
            { state with
                  CurrentBlockNo = blockNo
                  TablePage =
                      newTablePage
                          txBuffer
                          txConcurrency
                          txRecovery
                          (BlockId.newBlockId tableFileName blockNo)
                          schema
                          doLog
                      |> Some },
            true

    let closeHeader txConcurrency tableFileName state =
        if state.FileHeaderPage |> Option.isSome then
            BlockId.newBlockId tableFileName 0L
            |> txConcurrency.LockTableFileHeader

        { state with FileHeaderPage = None }

    let close txConcurrency tableFileName state =
        state.TablePage
        |> Option.iter (fun tp -> tp.Close())

        { state with
              CurrentBlockNo = -1L
              TablePage = None }
        |> closeHeader txConcurrency tableFileName

    let getVal state fieldName =
        match state.TablePage with
        | Some tp -> tp.GetVal fieldName
        | _ -> failwith "Must call next()"

    let setVal txReadOnly (TableInfo (_, schema, tableFileName)) state fieldName value =
        if tableFileName
           |> FileService.isTempFile
           |> not
           && txReadOnly then
            failwith "Transaction read only"

        let fieldType = schema.DbType fieldName
        let v = value |> DbConstant.castTo fieldType

        if Page.size v > Page.maxSize fieldType
        then failwith ("Size over:" + (Page.size v).ToString())

        match state.TablePage with
        | Some tp -> tp.SetVal fieldName v
        | _ -> failwith "Must call next()"

    let currentRecordId tableFileName state =
        match state.TablePage with
        | Some tp -> RecordId.newBlockRecordId (tp.CurrentSlotNo()) tableFileName state.CurrentBlockNo
        | _ -> failwith "Must call next()"

    let beforeFirst txConcurrency tableFileName state =
        { close txConcurrency tableFileName state with
              CurrentBlockNo = 0L }

    let next fileService txBuffer txConcurrency txRecovery doLog (TableInfo (_, schema, tableFileName)) state =
        let rec searchNext state =
            match state.TablePage
                  |> Option.map (fun tp -> tp.Next()) with
            | Some true -> state, true
            | _ ->
                match moveTo
                          fileService
                          txBuffer
                          txConcurrency
                          txRecovery
                          doLog
                          tableFileName
                          schema
                          state
                          (state.CurrentBlockNo + 1L) with
                | nextstate, true -> searchNext nextstate
                | result -> result

        match state.CurrentBlockNo with
        | 0L ->
            match moveTo fileService txBuffer txConcurrency txRecovery doLog tableFileName schema state 1L with
            | nextstate, true -> searchNext nextstate
            | result -> result
        | _ -> searchNext state

    let moveToRecordId fileService
                       txBuffer
                       txConcurrency
                       txRecovery
                       doLog
                       (TableInfo (_, schema, tableFileName))
                       state
                       (RecordId (slotNo, BlockId (_, blockNo)))
                       =
        let newstate =
            moveTo fileService txBuffer txConcurrency txRecovery doLog tableFileName schema state blockNo
            |> fst

        match newstate.TablePage with
        | Some tp -> tp.MoveToSlotNo slotNo
        | _ -> failwith "Must call next()"

        newstate

    let initHeaderForModification txBuffer txConcurrency txRecovery tableFileName state =
        let openHeaderForModification txBuffer txConcurrency txRecovery tableFileName =
            if tableFileName |> FileService.isTempFile |> not then
                BlockId.newBlockId tableFileName 0L
                |> txConcurrency.LockTableFileHeader
            newFileHeaderPage txBuffer txConcurrency txRecovery tableFileName

        state.FileHeaderPage
        |> Option.defaultWith (fun () -> openHeaderForModification txBuffer txConcurrency txRecovery tableFileName)

    let insert fileService txBuffer txConcurrency txRecovery txReadOnly doLog tableInfo state =
        let (TableInfo (tableName, schema, tableFileName)) = tableInfo

        let atLastBlock state =
            fileSize fileService txConcurrency tableFileName
            - 1L = state.CurrentBlockNo

        let appendBlock () =
            if tableFileName |> FileService.isTempFile |> not
            then txConcurrency.ModifyFile tableFileName

            let buffer =
                newTablePageFormatter schema
                |> txBuffer.PinNew tableFileName

            txBuffer.Unpin buffer

            if tableFileName |> FileService.isTempFile |> not
            then buffer.BlockId() |> txConcurrency.InsertBlock

        let rec appendSlot state =
            match state.TablePage
                  |> Option.map (fun tp -> tp.InsertIntoNextEmptySlot()) with
            | Some false ->
                if atLastBlock state then appendBlock ()

                let newstate =
                    moveTo
                        fileService
                        txBuffer
                        txConcurrency
                        txRecovery
                        doLog
                        tableFileName
                        schema
                        state
                        (state.CurrentBlockNo + 1L)
                    |> fst

                appendSlot newstate
            | _ -> state

        let insertEmptySlot fhp state =
            let newstate =
                if fhp |> FileHeaderPage.hasDataRecords then
                    fhp
                    |> FileHeaderPage.getTailRecordId
                    |> moveToRecordId fileService txBuffer txConcurrency txRecovery doLog tableInfo state
                    |> appendSlot
                else
                    appendBlock ()

                    let newstate =
                        moveTo fileService txBuffer txConcurrency txRecovery doLog tableFileName schema state 1L
                        |> fst

                    newstate.TablePage
                    |> Option.map (fun tp -> tp.InsertIntoNextEmptySlot())
                    |> ignore

                    newstate

            fhp
            |> FileHeaderPage.setTailRecordId (currentRecordId tableFileName newstate)

            newstate

        let insertDeletedSlot fhp state =
            let newstate =
                fhp
                |> FileHeaderPage.getLastDeletedRecordId
                |> moveToRecordId fileService txBuffer txConcurrency txRecovery doLog tableInfo state

            newstate.TablePage
            |> Option.map (fun tp -> tp.InsertIntoDeletedSlot())
            |> Option.iter (fun lastDeletedSlot ->
                fhp
                |> FileHeaderPage.setLastDeletedRecordId lastDeletedSlot)

            newstate

        if tableFileName |> FileService.isTempFile |> not then
            if txReadOnly then failwith "Transaction read only"

            txConcurrency.ModifyFile tableFileName

        let fhp =
            initHeaderForModification txBuffer txConcurrency txRecovery tableFileName state

        txRecovery.LogLogicalStart() |> ignore

        let newstate =
            if fhp |> FileHeaderPage.hasDeletedSlots
            then insertDeletedSlot fhp { state with FileHeaderPage = Some fhp }
            else insertEmptySlot fhp { state with FileHeaderPage = Some fhp }

        let (RecordId (slotNo, BlockId (_, blockNo))) = currentRecordId tableFileName newstate
        txRecovery.LogTableFileInsertionEnd tableName blockNo slotNo
        |> ignore
        closeHeader txConcurrency tableFileName newstate

    let delete txBuffer txConcurrency txRecovery txReadOnly (TableInfo (tableName, _, tableFileName)) state =
        let deleteSlot fhp state (tp: TablePage) =
            let recordId = currentRecordId tableFileName state

            txRecovery.LogLogicalStart() |> ignore

            fhp
            |> FileHeaderPage.getLastDeletedRecordId
            |> tp.Delete

            fhp
            |> FileHeaderPage.setLastDeletedRecordId recordId

            txRecovery.LogTableFileDeletionEnd tableName (RecordId.blockNo recordId) (RecordId.slotNo recordId)
            |> ignore

        if tableFileName
           |> FileService.isTempFile
           |> not
           && txReadOnly then
            failwith "Transaction read only"

        let fhp =
            initHeaderForModification txBuffer txConcurrency txRecovery tableFileName state

        let newstate = { state with FileHeaderPage = Some fhp }
        newstate.TablePage
        |> Option.iter (deleteSlot fhp newstate)

        closeHeader txConcurrency tableFileName newstate

    let undoInsert fileService txBuffer txConcurrency txRecovery txReadOnly doLog tableInfo state recordId =
        moveToRecordId fileService txBuffer txConcurrency txRecovery doLog tableInfo state recordId
        |> delete txBuffer txConcurrency txRecovery txReadOnly tableInfo

    let undoDelete fileService txBuffer txConcurrency txRecovery txReadOnly doLog tableInfo state recordId =
        let (TableInfo (tableName, _, tableFileName)) = tableInfo

        let rec moveToDeletedSlot state deletedRecordId lastDeletedRecordId =
            let (RecordId (_, BlockId (_, blockNo))) = deletedRecordId
            if deletedRecordId <> recordId && blockNo > 0L then
                let newstate =
                    moveToRecordId fileService txBuffer txConcurrency txRecovery doLog tableInfo state deletedRecordId

                let prevDeletedRecordId =
                    newstate.TablePage
                    |> Option.map (fun tp -> tp.GetDeletedRecordId())
                    |> Option.get

                moveToDeletedSlot newstate prevDeletedRecordId (Some deletedRecordId)
            else
                deletedRecordId, lastDeletedRecordId

        let setLastDeletedRecordId fhp state deletedRecordId =
            let newstate =
                moveToRecordId fileService txBuffer txConcurrency txRecovery doLog tableInfo state deletedRecordId

            newstate.TablePage
            |> Option.map (fun tp -> tp.GetDeletedRecordId())
            |> Option.iter (fun prevDeletedRecordId ->
                fhp
                |> FileHeaderPage.setLastDeletedRecordId prevDeletedRecordId)

            newstate

        let setPreviousDeletedRecordId state deletedRecordId lastDeletedRecordId =
            let newstate =
                moveToRecordId fileService txBuffer txConcurrency txRecovery doLog tableInfo state deletedRecordId

            newstate.TablePage
            |> Option.map (fun tp -> tp.GetDeletedRecordId())
            |> Option.map (fun prevDeletedRecordId ->
                let newstate =
                    lastDeletedRecordId
                    |> Option.map
                        (moveToRecordId fileService txBuffer txConcurrency txRecovery doLog tableInfo newstate)
                    |> Option.defaultValue newstate

                newstate.TablePage
                |> Option.iter (fun tp -> tp.SetDeletedRecordId prevDeletedRecordId)
                newstate)
            |> Option.defaultValue newstate

        if tableFileName |> FileService.isTempFile |> not then
            if txReadOnly then failwith "Transaction read only"

            txConcurrency.ModifyFile tableFileName

        let fhp =
            initHeaderForModification txBuffer txConcurrency txRecovery tableFileName state

        txRecovery.LogLogicalStart() |> ignore

        let newstate =
            moveToRecordId
                fileService
                txBuffer
                txConcurrency
                txRecovery
                doLog
                tableInfo
                { state with FileHeaderPage = Some fhp }
                recordId

        if newstate.TablePage
           |> Option.map (fun tp -> tp.UndoDelete())
           |> Option.defaultValue false
           |> not then
            failwith
                ("Specified slot: "
                 + recordId.ToString()
                 + " is in used")

        let deletedRecordId, lastDeletedRecordId =
            moveToDeletedSlot newstate (fhp |> FileHeaderPage.getLastDeletedRecordId) None

        let newstate =
            if Option.isNone lastDeletedRecordId then
                setLastDeletedRecordId fhp newstate deletedRecordId
            elif RecordId.blockNo deletedRecordId > 0L then
                setPreviousDeletedRecordId newstate deletedRecordId lastDeletedRecordId
            else
                newstate

        txRecovery.LogTableFileInsertionEnd
            tableName
            (RecordId.blockNo deletedRecordId)
            (RecordId.slotNo deletedRecordId)
        |> ignore
        closeHeader txConcurrency tableFileName newstate

    let remove (fileService: FileService) txConcurrency tableFileName state =
        let newstate = close txConcurrency tableFileName state
        fileService.Delete tableFileName
        newstate

let newTableFile fileService txBuffer txConcurrency txRecovery txReadOnly doLog tableInfo =
    let tableFileName = TableInfo.tableFileName tableInfo

    let mutable state: TableFile.TableFileState =
        { FileHeaderPage = None
          TablePage = None
          CurrentBlockNo = -1L }

    { GetVal = fun fieldName -> TableFile.getVal state fieldName
      SetVal = fun fieldName value -> TableFile.setVal txReadOnly tableInfo state fieldName value
      CurrentRecordId = fun () -> TableFile.currentRecordId tableFileName state
      BeforeFirst = fun () -> state <- TableFile.beforeFirst txConcurrency tableFileName state
      Next =
          fun () ->
              let newstate, result =
                  TableFile.next fileService txBuffer txConcurrency txRecovery doLog tableInfo state

              state <- newstate
              result
      MoveToRecordId =
          fun recordId ->
              state <-
                  TableFile.moveToRecordId fileService txBuffer txConcurrency txRecovery doLog tableInfo state recordId
      Insert =
          fun () ->
              state <- TableFile.insert fileService txBuffer txConcurrency txRecovery txReadOnly doLog tableInfo state
      Delete = fun () -> state <- TableFile.delete txBuffer txConcurrency txRecovery txReadOnly tableInfo state
      UndoInsert =
          fun recordId ->
              state <-
                  TableFile.undoInsert
                      fileService
                      txBuffer
                      txConcurrency
                      txRecovery
                      txReadOnly
                      doLog
                      tableInfo
                      state
                      recordId
      UndoDelete =
          fun recordId ->
              state <-
                  TableFile.undoDelete
                      fileService
                      txBuffer
                      txConcurrency
                      txRecovery
                      txReadOnly
                      doLog
                      tableInfo
                      state
                      recordId
      FileSize = fun () -> TableFile.fileSize fileService txConcurrency tableFileName
      Close = fun () -> state <- TableFile.close txConcurrency tableFileName state
      Remove = fun () -> state <- TableFile.remove fileService txConcurrency tableFileName state }
