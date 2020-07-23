namespace RyakDB.Storage.Record

open RyakDB.Sql.Type
open RyakDB.Storage.Type
open RyakDB.Storage.File

type FileHeaderPage =
    { Tx: Transaction
      FileName: string
      BlockId: BlockId
      CurrentBuffer: Buffer }

type RecordPage =
    { Close: unit -> unit
      Next: unit -> bool
      GetVal: string -> SqlConstant option
      SetVal: string -> SqlConstant -> unit
      Delete: RecordId -> unit
      InsertIntoTheCurrentSlot: unit -> bool
      InsertIntoNextEmptySlot: unit -> bool
      InsertIntoDeletedSlot: unit -> RecordId option
      MoveToId: int32 -> unit
      CurrentSlot: unit -> int32
      BlockId: unit -> BlockId option
      GetNextDeletedSlotId: unit -> RecordId option
      SetNextDeletedSlotId: RecordId -> unit }

module RecordId =
    let newRecordId id blockId = RecordId(id, blockId)

    let newBlockRecordId id fileName blockNo =
        RecordId(id, BlockId.newBlockId fileName blockNo)

module FileHeaderPage =
    let OffsetLdsBlockId = 0
    let OffsetLdsRecordId = 8
    let OffsetTsBlockId = 8 + 4
    let OffsetTsRecordId = 8 + 4 + 8

    let NoSlotBlockId = -1L
    let NoSlotRecordId = -1

    let getVal offset sqlType (state: FileHeaderPage) =
        if not (FileManager.isTempFile state.FileName)
        then state.Tx.ConcurMgr.ReadBlock state.BlockId
        state.CurrentBuffer.GetVal offset sqlType

    let setVal offset value (state: FileHeaderPage) =
        if not (FileManager.isTempFile state.FileName) then
            if state.Tx.ReadOnly then failwith "Transaction read only"
            state.Tx.ConcurMgr.ModifyBlock state.BlockId
        state.Tx.RecoveryMgr.LogSetVal state.CurrentBuffer offset value
        |> state.CurrentBuffer.SetVal offset value

    let getOffsetLdsBlockId (state: FileHeaderPage) =
        state
        |> getVal OffsetLdsBlockId BigIntSqlType
        |> SqlConstant.toLong

    let getOffsetLdsRecordId (state: FileHeaderPage) =
        state
        |> getVal OffsetLdsRecordId IntSqlType
        |> SqlConstant.toInt

    let getOffsetTsBlockId (state: FileHeaderPage) =
        state
        |> getVal OffsetTsBlockId BigIntSqlType
        |> SqlConstant.toLong

    let getOffsetTsRecordId (state: FileHeaderPage) =
        state
        |> getVal OffsetTsRecordId IntSqlType
        |> SqlConstant.toInt

    let hasDataRecords (state: FileHeaderPage) =
        state |> getOffsetTsBlockId <> NoSlotBlockId

    let hasDeletedSlots (state: FileHeaderPage) =
        state |> getOffsetLdsBlockId <> NoSlotBlockId

    let getLastDeletedSlotId (state: FileHeaderPage) =
        RecordId.newBlockRecordId (state |> getOffsetLdsRecordId) state.FileName (state |> getOffsetLdsBlockId)

    let setLastDeletedSlotId (RecordId (rid, BlockId (_, blockNo))) (state: FileHeaderPage) =
        state
        |> setVal OffsetLdsBlockId (BigIntSqlConstant blockNo)
        state
        |> setVal OffsetLdsRecordId (IntSqlConstant rid)

    let getTailSlotId (state: FileHeaderPage) =
        RecordId.newBlockRecordId (state |> getOffsetTsRecordId) state.FileName (state |> getOffsetTsBlockId)

    let setTailSlotId (RecordId (rid, BlockId (_, blockNo))) (state: FileHeaderPage) =
        state
        |> setVal OffsetTsBlockId (BigIntSqlConstant blockNo)
        state
        |> setVal OffsetTsRecordId (IntSqlConstant rid)

    let newFileHeaderPage (tx: Transaction) fileName =
        let blockId = BlockId.newBlockId fileName 0L
        { Tx = tx
          FileName = fileName
          BlockId = blockId
          CurrentBuffer = tx.BufferMgr.Pin blockId }

module FileHeaderFormatter =
    let format (buffer: Buffer) =
        BigIntSqlConstant FileHeaderPage.NoSlotBlockId
        |> buffer.SetValue FileHeaderPage.OffsetLdsBlockId
        IntSqlConstant FileHeaderPage.NoSlotRecordId
        |> buffer.SetValue FileHeaderPage.OffsetLdsRecordId
        BigIntSqlConstant FileHeaderPage.NoSlotBlockId
        |> buffer.SetValue FileHeaderPage.OffsetTsBlockId
        IntSqlConstant FileHeaderPage.NoSlotRecordId
        |> buffer.SetValue FileHeaderPage.OffsetTsRecordId

module RecordPage =
    type RecordPageState =
        { Tx: Transaction
          TableInfo: TableInfo
          BlockId: BlockId option
          CurrentBuffer: Buffer option
          CurrentSlot: int32
          SlotSize: int32
          OffsetMap: Map<string, int32>
          DoLog: bool }

    let MinRecordSize = 12
    let FlagSize = 4

    let EmptyConst = IntSqlConstant(0)
    let InUseConst = IntSqlConstant(1)

    let currentPos state = state.CurrentSlot * state.SlotSize

    let fieldPos fieldName state =
        (state |> currentPos)
        + FlagSize
        + state.OffsetMap.[fieldName]

    let getValue offset sqlType state =
        state.BlockId
        |> Option.iter (fun blockId ->
            let (BlockId (fileName, _)) = blockId
            if not (FileManager.isTempFile fileName) then
                RecordId.newRecordId state.CurrentSlot blockId
                |> state.Tx.ConcurMgr.ReadRecord)
        state.CurrentBuffer
        |> Option.map (fun buffer -> buffer.GetVal offset sqlType)

    let setValue offset value state =
        state.BlockId
        |> Option.iter (fun (BlockId (fileName, _)) ->
            if not (FileManager.isTempFile fileName) then
                if state.Tx.ReadOnly then failwith "Transaction read only"
                state.Tx.ConcurMgr.ModifyFile fileName)
        state.CurrentBuffer
        |> Option.iter (fun buffer ->
            if state.DoLog
            then state.Tx.RecoveryMgr.LogSetVal buffer offset value
            else None
            |> buffer.SetVal offset value)

    let searchFor flag state =
        let isValidSlot state =
            state.CurrentBuffer
            |> Option.map (fun buff ->
                (state |> currentPos)
                + state.SlotSize
                <= buff.BufferSize)
            |> Option.defaultValue false

        let rec loopSearchFor state =
            let newstate: RecordPageState =
                { state with
                      CurrentSlot = state.CurrentSlot + 1 }

            if isValidSlot newstate then
                if newstate
                   |> getValue (newstate |> currentPos) IntSqlType
                   |> Option.map (fun v -> v = flag)
                   |> Option.defaultValue false then
                    newstate, true
                else
                    loopSearchFor newstate
            else
                newstate, false

        loopSearchFor state

    let getVal fieldName state =
        state
        |> getValue (state |> fieldPos fieldName) (state.TableInfo.Schema.SqlType fieldName)

    let setVal fieldName value state =
        state
        |> setValue (state |> fieldPos fieldName) value

    let getNextDeletedSlotId state =
        let position = (state |> currentPos) + FlagSize
        match state.BlockId, state |> getValue position BigIntSqlType, state |> getValue (position + 8) IntSqlType with
        | Some (BlockId (fileName, _)), Some (blockNo), Some (recordId) ->
            Some(RecordId.newBlockRecordId (recordId |> SqlConstant.toInt) fileName (blockNo |> SqlConstant.toLong))
        | _ -> None

    let setNextDeletedSlotId (RecordId (rid, BlockId (_, blockNo))) state =
        let position = (state |> currentPos) + FlagSize
        state
        |> setValue position (BigIntSqlConstant blockNo)
        state
        |> setValue (position + 8) (IntSqlConstant rid)

    let next state = state |> searchFor InUseConst

    let moveToId id state: RecordPageState = { state with CurrentSlot = id }

    let insertIntoNextEmptySlot state =
        let newstate, found = state |> searchFor EmptyConst
        if found then
            newstate
            |> setValue (newstate |> currentPos) InUseConst
        newstate, found

    let insertIntoTheCurrentSlot state =
        let isEmpty =
            state
            |> getValue (state |> currentPos) IntSqlType
            |> Option.map (fun flag -> flag = EmptyConst)
            |> Option.defaultValue false

        if isEmpty
        then state |> setValue (state |> currentPos) InUseConst
        isEmpty

    let insertIntoDeletedSlot state =
        let nextDeletedSlot = state |> getNextDeletedSlotId
        state
        |> setNextDeletedSlotId (RecordId.newBlockRecordId 0 "" 0L)
        state |> setValue (state |> currentPos) InUseConst
        nextDeletedSlot

    let delete (nextDeletedSlot: RecordId) state =
        state |> setValue (state |> currentPos) EmptyConst
        state |> setNextDeletedSlotId nextDeletedSlot

    let close state =
        if Option.isSome state.BlockId then
            state.CurrentBuffer
            |> Option.iter state.Tx.BufferMgr.Unpin
            { state with
                  BlockId = None
                  CurrentBuffer = None }
        else
            state

    let newRecordPage (tx: Transaction) blockId (tableInfo: TableInfo) doLog =
        let offsetMap, pos =
            tableInfo.Schema.Fields()
            |> List.fold (fun (map: Map<string, int32>, pos) field ->
                map.Add(field, pos),
                pos
                + (tableInfo.Schema.SqlType field |> Page.maxSize)) (Map.empty, 0)

        let slotSize =
            FlagSize
            + if pos < MinRecordSize then MinRecordSize else pos

        let mutable state =
            { Tx = tx
              TableInfo = tableInfo
              BlockId = Some blockId
              CurrentBuffer = Some(tx.BufferMgr.Pin blockId)
              CurrentSlot = -1
              SlotSize = slotSize
              OffsetMap = offsetMap
              DoLog = doLog }

        { Close = fun () -> state <- close state
          Next =
              fun () ->
                  let newstate, result = next state
                  state <- newstate
                  result
          GetVal = fun fieldName -> getVal fieldName state
          SetVal = fun fieldName value -> setVal fieldName value state
          Delete = fun nextDeletedSlot -> delete nextDeletedSlot state
          InsertIntoTheCurrentSlot = fun () -> insertIntoTheCurrentSlot state
          InsertIntoNextEmptySlot =
              fun () ->
                  let newstate, result = insertIntoNextEmptySlot state
                  state <- newstate
                  result
          InsertIntoDeletedSlot = fun () -> insertIntoDeletedSlot state
          MoveToId = fun id -> state <- moveToId id state
          CurrentSlot = fun () -> state.CurrentSlot
          BlockId = fun () -> state.BlockId
          GetNextDeletedSlotId = fun () -> getNextDeletedSlotId state
          SetNextDeletedSlotId = fun recordId -> setNextDeletedSlotId recordId state }

module RecordFormatter =
    let offsetMap tableInfo =
        let offsetMap, _ =
            tableInfo.Schema.Fields()
            |> List.fold (fun (map: Map<string, int32>, pos) field ->
                map.Add(field, pos),
                pos
                + (tableInfo.Schema.SqlType field |> Page.maxSize)) (Map.empty, 0)

        offsetMap

    let recordSize tableInfo =
        let pos =
            tableInfo.Schema.Fields()
            |> List.fold (fun pos field ->
                pos
                + (tableInfo.Schema.SqlType field |> Page.maxSize)) 0

        if pos < RecordPage.MinRecordSize then RecordPage.MinRecordSize else pos

    let slotSize tableInfo = (recordSize tableInfo) + 4

    let makeDefaultRecord tableInfo (offsetMap: Map<string, int32>) (buffer: Buffer) pos =
        tableInfo.Schema.Fields()
        |> List.iter (fun field ->
            buffer.SetValue (pos + 4 + offsetMap.[field])
                (tableInfo.Schema.SqlType field
                 |> SqlConstant.defaultConstant))

    let newFormatter tableInfo =
        let myOffsetMap = offsetMap tableInfo
        let slotSize = slotSize tableInfo
        fun buffer ->
            for pos in 0 .. slotSize .. (buffer.BufferSize - slotSize) do
                buffer.SetValue pos RecordPage.EmptyConst
                makeDefaultRecord tableInfo myOffsetMap buffer pos

module RecordFile =
    type RecordFileState =
        { Tx: Transaction
          TableInfo: TableInfo
          FileName: string
          HeaderBlockId: BlockId
          FileHeaderPage: FileHeaderPage option
          RecordPage: RecordPage option
          CurrentBlockNo: int64
          IsBeforeFirsted: bool
          DoLog: bool
          FileMgr: FileManager }

    let formatFileHeader (fileMgr: FileManager) (tx: Transaction) fileName =
        tx.ConcurMgr.ModifyFile fileName
        if (fileMgr.Size fileName) = 0L then
            tx.BufferMgr.PinNew fileName FileHeaderFormatter.format
            |> tx.BufferMgr.Unpin

    let fileSize state =
        if not (FileManager.isTempFile state.FileName)
        then state.Tx.ConcurMgr.ReadFile state.FileName
        state.FileMgr.Size state.FileName

    let moveTo state blockNo =
        state.RecordPage
        |> Option.iter (fun rp -> rp.Close())
        if blockNo > fileSize state then
            state, false
        else
            { state with
                  CurrentBlockNo = blockNo
                  RecordPage =
                      Some
                          (RecordPage.newRecordPage state.Tx (BlockId.newBlockId state.FileName blockNo) state.TableInfo
                               state.DoLog) },
            true

    let openHeaderForModification state =
        if not (FileManager.isTempFile state.FileName)
        then state.Tx.ConcurMgr.LockRecordFileHeader state.HeaderBlockId
        FileHeaderPage.newFileHeaderPage state.Tx state.FileName

    let closeHeader state =
        if Option.isSome state.FileHeaderPage then
            state.Tx.ConcurMgr.LockRecordFileHeader state.HeaderBlockId
            { state with FileHeaderPage = None }
        else
            state

    let close state =
        state.RecordPage
        |> Option.iter (fun rp -> rp.Close())
        closeHeader state

    let getVal state fieldName =
        state.RecordPage
        |> Option.bind (fun rp -> rp.GetVal fieldName)

    let setVal state fieldName value =
        if state.Tx.ReadOnly
           && not (FileManager.isTempFile state.FileName) then
            failwith "Transaction read only"
        let fieldType = state.TableInfo.Schema.SqlType fieldName
        let v = value |> SqlConstant.castTo fieldType
        if Page.size v > Page.maxSize fieldType then failwith "Size over"
        state.RecordPage
        |> Option.iter (fun rp -> rp.SetVal fieldName v)

    let currentRecordId state =
        state.RecordPage
        |> Option.map (fun rp -> RecordId.newBlockRecordId (rp.CurrentSlot()) state.FileName state.CurrentBlockNo)

    let beforeFirst state =
        { (close state) with
              CurrentBlockNo = 0L
              IsBeforeFirsted = true }

    let next state =
        let rec loopNext state =
            match state.RecordPage
                  |> Option.map (fun rp -> rp.Next()) with
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

    let moveToRecordId state (RecordId (rid, BlockId (_, blockNo))) =
        let newstate, _ = moveTo state blockNo
        newstate.RecordPage
        |> Option.iter (fun rp -> rp.MoveToId rid)
        newstate

    let initHeaderForModification state =
        let newstate =
            if Option.isNone state.FileHeaderPage then
                { state with
                      FileHeaderPage = Some(openHeaderForModification state) }
            else
                state

        newstate, newstate.FileHeaderPage |> Option.get

    let insert state =
        let atLastBlock state =
            (fileSize state - 1L) = state.CurrentBlockNo

        let appendBlock state =
            if not (FileManager.isTempFile state.FileName)
            then state.Tx.ConcurMgr.ModifyFile state.FileName

            let buffer =
                RecordFormatter.newFormatter state.TableInfo
                |> state.Tx.BufferMgr.PinNew state.FileName

            state.Tx.BufferMgr.Unpin buffer
            if not (FileManager.isTempFile state.FileName)
            then state.Tx.ConcurMgr.InsertBlock(buffer.BlockId())

        let rec loopAppendBlock state =
            match state.RecordPage
                  |> Option.map (fun rp -> rp.InsertIntoNextEmptySlot()) with
            | Some (false) ->
                if atLastBlock state then appendBlock state
                let newstate2, _ = moveTo state (state.CurrentBlockNo + 1L)
                loopAppendBlock newstate2
            | _ -> state

        let insertEmptySlot state fhp =
            let newstate =
                if fhp |> FileHeaderPage.hasDataRecords then
                    loopAppendBlock
                        (fhp
                         |> FileHeaderPage.getTailSlotId
                         |> moveToRecordId state)
                else
                    appendBlock state
                    let newstate2, _ = moveTo state 1L
                    newstate2.RecordPage
                    |> Option.map (fun rp -> rp.InsertIntoNextEmptySlot())
                    |> ignore
                    newstate2

            currentRecordId newstate
            |> Option.iter (fun tailSolt -> fhp |> FileHeaderPage.setTailSlotId tailSolt)
            newstate

        let insertDeletedSlot state fhp =
            let newstate =
                moveToRecordId state (fhp |> FileHeaderPage.getLastDeletedSlotId)

            newstate.RecordPage
            |> Option.bind (fun rp -> rp.InsertIntoDeletedSlot())
            |> Option.iter (fun lastDeletedSlot ->
                fhp
                |> FileHeaderPage.setLastDeletedSlotId lastDeletedSlot)
            newstate

        if not (FileManager.isTempFile state.FileName) then
            if state.Tx.ReadOnly then failwith "Transaction read only"
            state.Tx.ConcurMgr.ModifyFile state.FileName
        let newstate, fhp = initHeaderForModification state
        newstate.Tx.RecoveryMgr.LogLogicalStart()
        |> ignore

        let newstate2 =
            if fhp |> FileHeaderPage.hasDeletedSlots then
                insertDeletedSlot newstate fhp
            else
                insertEmptySlot newstate fhp

        currentRecordId newstate2
        |> Option.iter (fun (RecordId (rid, BlockId (_, blockNo))) ->
            newstate2.Tx.RecoveryMgr.LogRecordFileInsertionEnd newstate2.TableInfo.TableName blockNo rid
            |> ignore)
        closeHeader newstate2

    let insertByRecordId state recordId =
        let rec loopCurrentSlot state (currentSlot: RecordId) lastSlot =
            let (RecordId (_, BlockId (_, blockNo))) = currentSlot
            if currentSlot
               <> recordId
               && blockNo <> FileHeaderPage.NoSlotBlockId then
                let newstate = moveToRecordId state currentSlot

                let nextSlot =
                    newstate.RecordPage
                    |> Option.bind (fun rp -> rp.GetNextDeletedSlotId())
                    |> Option.get

                loopCurrentSlot newstate nextSlot (Some currentSlot)
            else
                currentSlot, lastSlot

        let setHeaderLastDeletedSlotId state fhp currentSlot =
            let newstate = moveToRecordId state currentSlot
            newstate.RecordPage
            |> Option.bind (fun rp -> rp.GetNextDeletedSlotId())
            |> Option.iter (fun nextSlot ->
                fhp
                |> FileHeaderPage.setLastDeletedSlotId nextSlot)
            newstate

        let setPageNextDeletedSlotId state currentSlot lastSlot =
            let newstate = moveToRecordId state currentSlot
            newstate.RecordPage
            |> Option.bind (fun rp -> rp.GetNextDeletedSlotId())
            |> Option.map (fun nextSlot ->
                let newstate2 =
                    lastSlot
                    |> Option.map (moveToRecordId newstate)
                    |> Option.defaultValue newstate

                newstate2.RecordPage
                |> Option.iter (fun rp -> rp.SetNextDeletedSlotId nextSlot)
                newstate2)
            |> Option.defaultValue newstate

        if not (FileManager.isTempFile state.FileName) then
            if state.Tx.ReadOnly then failwith "Transaction read only"
            state.Tx.ConcurMgr.ModifyFile state.FileName
        let newstate, fhp = initHeaderForModification state
        newstate.Tx.RecoveryMgr.LogLogicalStart()
        |> ignore
        let newstate2 = moveToRecordId newstate recordId
        if not
            (newstate2.RecordPage
             |> Option.map (fun rp -> rp.InsertIntoTheCurrentSlot())
             |> Option.defaultValue false) then
            failwith
                ("the specified slot: "
                 + recordId.ToString()
                 + " is in used")

        let currentSlot, lastSlot =
            loopCurrentSlot newstate2 (fhp |> FileHeaderPage.getLastDeletedSlotId) None

        let newstate3 =
            let (RecordId (_, BlockId (_, blockNo))) = currentSlot
            if Option.isNone lastSlot
            then setHeaderLastDeletedSlotId newstate2 fhp currentSlot
            elif blockNo <> FileHeaderPage.NoSlotBlockId
            then setPageNextDeletedSlotId newstate2 currentSlot lastSlot
            else newstate2

        let (RecordId (rid, BlockId (_, blockNo))) = recordId
        newstate3.Tx.RecoveryMgr.LogRecordFileInsertionEnd newstate3.TableInfo.TableName blockNo rid
        |> ignore
        closeHeader newstate3

    let delete state =
        let delete state fhp rp =
            let (RecordId (rid, BlockId (_, blockNo))) = currentRecordId state |> Option.get
            state.Tx.RecoveryMgr.LogLogicalStart() |> ignore
            rp.Delete(fhp |> FileHeaderPage.getLastDeletedSlotId)
            fhp
            |> FileHeaderPage.setLastDeletedSlotId (currentRecordId state |> Option.get)
            state.Tx.RecoveryMgr.LogRecordFileDeletionEnd state.TableInfo.TableName blockNo rid
            |> ignore

        if state.Tx.ReadOnly
           && not (FileManager.isTempFile state.FileName) then
            failwith "Transaction read only"
        let newstate, fhp = initHeaderForModification state
        newstate.RecordPage
        |> Option.iter (fun rp -> delete newstate fhp rp)
        closeHeader newstate

    let deleteByRecordId state recordId = moveToRecordId state recordId |> delete

    let remove state =
        let newstate = close state
        newstate.FileMgr.Delete newstate.FileName
        newstate

    let newRecordFile (fileMgr: FileManager) (tx: Transaction) doLog (tableInfo: TableInfo) =
        let fileName = tableInfo.FileName

        let mutable state =
            { Tx = tx
              TableInfo = tableInfo
              FileName = fileName
              HeaderBlockId = BlockId.newBlockId fileName 0L
              FileHeaderPage = None
              RecordPage = None
              CurrentBlockNo = 0L
              IsBeforeFirsted = false
              DoLog = doLog
              FileMgr = fileMgr }

        { GetVal = fun fieldName -> getVal state fieldName
          SetVal = fun fieldName value -> setVal state fieldName value
          CurrentRecordId = fun () -> currentRecordId state
          BeforeFirst = fun () -> state <- beforeFirst state
          Next =
              fun () ->
                  let newstate, result = next state
                  state <- newstate
                  result
          MoveToRecordId = fun recordId -> state <- moveToRecordId state recordId
          Insert = fun () -> state <- insert state
          InsertByRecordId = fun recordId -> state <- insertByRecordId state recordId
          Delete = fun () -> state <- delete state
          DeleteByRecordId = fun recordId -> state <- deleteByRecordId state recordId
          FileSize = fun () -> fileSize state
          Close = fun () -> state <- close state
          Remove = fun () -> state <- remove state }
