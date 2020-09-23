module RyakDB.Index.BTreeLeaf

open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Table
open RyakDB.Index
open RyakDB.Concurrency.TransactionConcurrency
open RyakDB.Recovery.TransactionRecovery
open RyakDB.Index.BTreePage
open RyakDB.Index.BTreeBranch

type BTreeLeaf =
    { Next: unit -> bool
      GetDataRecordId: unit -> RecordId
      GetCountOfRecords: unit -> int32
      Insert: RecordId -> BTreeBranchEntry option
      Delete: RecordId -> unit
      Close: unit -> unit }

module BTreeLeaf =
    type BTreeLeafState =
        { CurrentPage: BTreePage
          CurrentSlot: int32
          IsOverflowing: bool
          OverflowFrom: int64
          MoveFrom: int64 }

    let FieldBlockNo = "block_no"
    let FieldSlotNo = "slot_no"
    let KeyPrefix = "key"

    let getFileName indexName = indexName + "_leaf.idx"

    let initBTreePage txBuffer txConcurrency txRecovery schema blockId =
        newBTreePage txBuffer txConcurrency txRecovery schema blockId 2

    let keyTypeToSchema (SearchKeyType types) =
        let sch = Schema.newSchema ()
        sch.AddField FieldBlockNo BigIntDbType
        sch.AddField FieldSlotNo IntDbType
        types
        |> List.iteri (fun i t -> sch.AddField (KeyPrefix + i.ToString()) t)
        sch

    let getKey page slot (SearchKeyType (keys)) =
        keys
        |> List.mapi (fun i _ -> page.GetVal slot (KeyPrefix + i.ToString()))
        |> SearchKey.newSearchKey

    let getDataRecordId filename page slot =
        RecordId.newBlockRecordId
            (page.GetVal slot FieldSlotNo |> DbConstant.toInt)
            filename
            (page.GetVal slot FieldBlockNo |> DbConstant.toLong)

    let insertSlot txRecovery keyType page (SearchKey keys) (RecordId (slotNo, BlockId (_, blockNo))) slot =
        txRecovery.LogIndexPageInsertion false page.BlockId keyType slot
        |> ignore
        page.Insert slot
        page.SetVal slot FieldBlockNo (BigIntDbConstant blockNo)
        page.SetVal slot FieldSlotNo (IntDbConstant slotNo)
        keys
        |> List.iteri (fun i k -> page.SetVal slot (KeyPrefix + i.ToString()) k)

    let deleteSlot txRecovery keyType page slot =
        txRecovery.LogIndexPageDeletion false page.BlockId keyType slot
        |> ignore
        page.Delete slot

    let getOverflowFlag page = page.GetFlag 0

    let setOverflowFlag page = page.SetFlag 0

    let getSiblingFlag page = page.GetFlag 1

    let setSiblingFlag page = page.SetFlag 1

    let moveSlotBefore keyType searchRange page =
        let rec loopSlot startSlot endSlot searchMin =
            let middleSlot = (startSlot + endSlot) / 2
            if startSlot = middleSlot then
                startSlot, endSlot
            else if SearchKey.compare (getKey page middleSlot keyType) searchMin < 0 then
                loopSlot middleSlot endSlot searchMin
            else
                loopSlot startSlot middleSlot searchMin

        let countOfRecords = page.GetCountOfRecords()
        if countOfRecords <= 0 then
            -1
        else
            let searchMin = searchRange.GetMin()

            let startSlot, endSlot =
                loopSlot 0 (countOfRecords - 1) searchMin

            if SearchKey.compare (getKey page endSlot keyType) searchMin < 0
            then endSlot
            else if SearchKey.compare (getKey page startSlot keyType) searchMin < 0
            then startSlot
            else -1

    let moveTo txBuffer txConcurrency txRecovery schema currentPage blockNo =
        let (BlockId (filename, _)) = currentPage.BlockId
        let blockId = BlockId.newBlockId filename blockNo
        txConcurrency.ReadLeafBlock blockId
        currentPage.Close()
        initBTreePage txBuffer txConcurrency txRecovery schema blockId

    let next txBuffer txConcurrency txRecovery schema keyType searchRange state =
        let rec loopNext (currentPage: BTreePage) currentSlot isOverflowing overflowFrom moveFrom =
            let slot = currentSlot + 1
            if isOverflowing then
                if slot >= currentPage.GetCountOfRecords() then
                    let (BlockId (_, currentBlockNo)) = currentPage.BlockId

                    let page =
                        getOverflowFlag currentPage
                        |> moveTo txBuffer txConcurrency txRecovery schema currentPage

                    let (BlockId (_, nextBlockNo)) = page.BlockId

                    let overflowing, ofFrom =
                        if nextBlockNo = overflowFrom then false, -1L else isOverflowing, overflowFrom

                    true,
                    { CurrentPage = page
                      CurrentSlot = 0
                      IsOverflowing = overflowing
                      OverflowFrom = ofFrom
                      MoveFrom = currentBlockNo }
                else
                    true,
                    { CurrentPage = currentPage
                      CurrentSlot = slot
                      IsOverflowing = isOverflowing
                      OverflowFrom = overflowFrom
                      MoveFrom = moveFrom }
            else if slot >= currentPage.GetCountOfRecords() then
                let sibling = getSiblingFlag currentPage
                if sibling <> -1L then
                    let (BlockId (_, currentBlockNo)) = currentPage.BlockId
                    loopNext
                        (moveTo txBuffer txConcurrency txRecovery schema currentPage sibling)
                        -1
                        isOverflowing
                        overflowFrom
                        currentBlockNo
                else
                    false,
                    { CurrentPage = currentPage
                      CurrentSlot = slot
                      IsOverflowing = isOverflowing
                      OverflowFrom = overflowFrom
                      MoveFrom = moveFrom }
            else if getKey currentPage slot keyType
                    |> searchRange.MatchsKey then
                let overflow = getOverflowFlag currentPage
                if slot = 0 && overflow <> -1L then
                    let (BlockId (_, currentBlockNo)) = currentPage.BlockId
                    true,
                    { CurrentPage = moveTo txBuffer txConcurrency txRecovery schema currentPage overflow
                      CurrentSlot = 0
                      IsOverflowing = true
                      OverflowFrom = currentBlockNo
                      MoveFrom = currentBlockNo }
                else
                    true,
                    { CurrentPage = currentPage
                      CurrentSlot = slot
                      IsOverflowing = isOverflowing
                      OverflowFrom = overflowFrom
                      MoveFrom = moveFrom }
            else
                false,
                { CurrentPage = currentPage
                  CurrentSlot = slot
                  IsOverflowing = isOverflowing
                  OverflowFrom = overflowFrom
                  MoveFrom = moveFrom }

        loopNext state.CurrentPage state.CurrentSlot state.IsOverflowing state.OverflowFrom state.MoveFrom

    let insert txRecovery keyType searchRange state recordId =
        if not (searchRange.IsSingleValue()) then failwith "Not supported"
        let searchKey = searchRange.ToSearchKey()
        let slot = state.CurrentSlot + 1
        insertSlot txRecovery keyType state.CurrentPage searchKey recordId slot

        if slot = 0
           && (getOverflowFlag state.CurrentPage) <> -1L
           && SearchKey.compare (getKey state.CurrentPage 1 keyType) searchKey
              <> 0 then
            let splitKey = getKey state.CurrentPage 1 keyType

            let newBlockNo =
                state.CurrentPage.Split
                    1
                    [ getOverflowFlag state.CurrentPage
                      getSiblingFlag state.CurrentPage ]

            setOverflowFlag state.CurrentPage -1L
            setSiblingFlag state.CurrentPage newBlockNo
            Some(newBTreeBranchEntry splitKey newBlockNo), slot
        else if state.CurrentPage.IsFull() then
            let firstKey = getKey state.CurrentPage 0 keyType

            let lastKey =
                getKey state.CurrentPage (state.CurrentPage.GetCountOfRecords() - 1) keyType

            if SearchKey.compare firstKey lastKey = 0 then
                let (BlockId (_, currentBlockNo)) = state.CurrentPage.BlockId
                let overflow = getOverflowFlag state.CurrentPage
                state.CurrentPage.Split
                    1
                    [ if overflow = -1L then
                        currentBlockNo
                      else
                          overflow
                          -1L ]
                |> setOverflowFlag state.CurrentPage
                None, slot
            else
                let mutable splitPos =
                    state.CurrentPage.GetCountOfRecords() / 2

                let splitKey =
                    getKey state.CurrentPage splitPos keyType

                let newSplitKey =
                    if SearchKey.compare splitKey firstKey = 0 then
                        while SearchKey.compare splitKey (getKey state.CurrentPage splitPos keyType) = 0 do
                            splitPos <- splitPos + 1
                        getKey state.CurrentPage splitPos keyType
                    else
                        while SearchKey.compare splitKey (getKey state.CurrentPage (splitPos - 1) keyType) = 0 do
                            splitPos <- splitPos - 1
                        splitKey

                let newBlockNo =
                    state.CurrentPage.Split
                        splitPos
                        [ -1L
                          getSiblingFlag state.CurrentPage ]

                setSiblingFlag state.CurrentPage newBlockNo
                Some(newBTreeBranchEntry newSplitKey newBlockNo), slot
        else
            None, slot

    let delete txBuffer txConcurrency txRecovery dataFileName schema keyType searchRange state recordId =
        let rec loopDelete state =
            let result, newstate =
                next txBuffer txConcurrency txRecovery schema keyType searchRange state

            if result then
                if recordId = getDataRecordId dataFileName newstate.CurrentPage newstate.CurrentSlot then
                    deleteSlot txRecovery keyType newstate.CurrentPage newstate.CurrentSlot
                    newstate
                else
                    loopDelete newstate
            else
                newstate

        if not (searchRange.IsSingleValue()) then failwith "Not supported"
        let state = loopDelete state
        if state.IsOverflowing then
            if state.CurrentPage.GetCountOfRecords() = 0 then
                let (BlockId (filename, _)) = state.CurrentPage.BlockId

                let prevPage =
                    BlockId.newBlockId filename state.MoveFrom
                    |> initBTreePage txBuffer txConcurrency txRecovery schema

                let overflow = getOverflowFlag state.CurrentPage
                let (BlockId (_, blockNo)) = prevPage.BlockId
                setOverflowFlag prevPage (if overflow = blockNo then -1L else overflow)
                prevPage.Close()
        else if (getOverflowFlag state.CurrentPage) <> -1L then
            let (BlockId (filename, blockNo)) = state.CurrentPage.BlockId

            let blockId =
                getOverflowFlag state.CurrentPage
                |> BlockId.newBlockId filename

            txConcurrency.ModifyLeafBlock blockId

            let overflowPage =
                initBTreePage txBuffer txConcurrency txRecovery schema blockId

            if state.CurrentPage.GetCountOfRecords() = 0
               || (overflowPage.GetCountOfRecords()
                   <> 0
                   && SearchKey.compare (getKey state.CurrentPage 0 keyType) (getKey overflowPage 0 keyType)
                      <> 0) then
                overflowPage.TransferRecords (overflowPage.GetCountOfRecords() - 1) state.CurrentPage 0 1
                if overflowPage.GetCountOfRecords() = 0 then
                    let overflow = getOverflowFlag overflowPage
                    setOverflowFlag state.CurrentPage (if overflow = blockNo then -1L else overflow)
                overflowPage.Close()
        state

let newBTreeLeaf txBuffer txConcurrency txRecovery dataFileName blockId keyType searchRange =
    let schema = BTreeLeaf.keyTypeToSchema keyType

    let page =
        BTreeLeaf.initBTreePage txBuffer txConcurrency txRecovery schema blockId

    let mutable state: BTreeLeaf.BTreeLeafState =
        { CurrentPage = page
          CurrentSlot = BTreeLeaf.moveSlotBefore keyType searchRange page
          IsOverflowing = false
          OverflowFrom = -1L
          MoveFrom = -1L }

    { Next =
          fun () ->
              let result, newstate =
                  BTreeLeaf.next txBuffer txConcurrency txRecovery schema keyType searchRange state

              state <- newstate
              result
      GetDataRecordId = fun () -> BTreeLeaf.getDataRecordId dataFileName page state.CurrentSlot
      GetCountOfRecords = fun () -> state.CurrentPage.GetCountOfRecords()
      Insert =
          fun recordId ->
              let result, nextSlot =
                  BTreeLeaf.insert txRecovery keyType searchRange state recordId

              state <- { state with CurrentSlot = nextSlot }
              result
      Delete =
          fun recordId ->
              state <-
                  BTreeLeaf.delete
                      txBuffer
                      txConcurrency
                      txRecovery
                      dataFileName
                      schema
                      keyType
                      searchRange
                      state
                      recordId
      Close = fun () -> state.CurrentPage.Close() }
