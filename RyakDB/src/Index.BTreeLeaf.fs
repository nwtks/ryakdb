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
    { BeforeFirst: SearchRange -> unit
      Next: unit -> bool
      GetDataRecordId: unit -> RecordId
      Insert: SearchKey -> RecordId -> BTreeBranchEntry option
      Delete: SearchKey -> RecordId -> unit
      Close: unit -> unit }
    interface System.IDisposable with
        member this.Dispose() = this.Close()

module BTreeLeaf =
    type BTreeLeafState =
        { CurrentPage: BTreePage
          CurrentSlot: int32
          MoveFrom: int64
          OverflowFrom: int64
          SearchRange: SearchRange }

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

    let getKey page slotNo (SearchKeyType keys) =
        keys
        |> List.mapi (fun i _ -> page.GetVal slotNo (KeyPrefix + i.ToString()))
        |> SearchKey.newSearchKey

    let getDataRecordId filename page slotNo =
        RecordId.newBlockRecordId
            (page.GetVal slotNo FieldSlotNo |> DbConstant.toInt)
            filename
            (page.GetVal slotNo FieldBlockNo
             |> DbConstant.toLong)

    let insertSlot txRecovery keyType page (SearchKey keys) (RecordId (recordSlotNo, BlockId (_, recordBlockNo))) slotNo =
        txRecovery.LogIndexPageInsertion false keyType page.BlockId slotNo
        |> ignore

        page.Insert slotNo

        BigIntDbConstant recordBlockNo
        |> page.SetVal slotNo FieldBlockNo

        IntDbConstant recordSlotNo
        |> page.SetVal slotNo FieldSlotNo

        keys
        |> List.iteri (fun i k -> page.SetVal slotNo (KeyPrefix + i.ToString()) k)

    let deleteSlot txRecovery keyType page slotNo =
        txRecovery.LogIndexPageDeletion false keyType page.BlockId slotNo
        |> ignore

        page.Delete slotNo

    let getOverflowBlockNo page = page.GetFlag 0

    let setOverflowBlockNo page = page.SetFlag 0

    let getSiblingBlockNo page = page.GetFlag 1

    let setSiblingBlockNo page = page.SetFlag 1

    let moveSlotBefore keyType searchRange page =
        let rec binarySearch startSlot endSlot target =
            if startSlot < endSlot then
                let middleSlot = startSlot + (endSlot - startSlot) / 2

                let comp =
                    SearchKey.compare (getKey page middleSlot keyType) target

                if comp < 0
                then binarySearch (middleSlot + 1) endSlot target
                else binarySearch startSlot middleSlot target
            else
                startSlot

        let countOfRecords = page.GetCountOfRecords()
        if countOfRecords = 0 then
            -1
        else
            match searchRange.GetLow() with
            | Some searchMin ->
                if SearchKey.compare (getKey page (countOfRecords - 1) keyType) searchMin < 0 then
                    countOfRecords - 1
                else
                    let slotNo = binarySearch 0 countOfRecords searchMin
                    if SearchKey.compare (getKey page slotNo keyType) searchMin
                       >= 0 then
                        slotNo - 1
                    else
                        slotNo
            | _ -> -1

    let beforeFirst txBuffer txConcurrency txRecovery schema blockId keyType searchRange =
        let page =
            initBTreePage txBuffer txConcurrency txRecovery schema blockId

        { CurrentPage = page
          CurrentSlot = moveSlotBefore keyType searchRange page
          MoveFrom = -1L
          OverflowFrom = -1L
          SearchRange = searchRange }

    let next txBuffer txConcurrency txRecovery schema keyType state =
        let moveTo txBuffer txConcurrency txRecovery schema currentPage blockNo =
            let blockId =
                BlockId.newBlockId (BlockId.fileName currentPage.BlockId) blockNo

            txConcurrency.ReadLeafBlock blockId

            currentPage.Close()

            initBTreePage txBuffer txConcurrency txRecovery schema blockId

        let rec searchNext state =
            let nextSlot = state.CurrentSlot + 1

            let fromBlockNo =
                BlockId.blockNo state.CurrentPage.BlockId

            if state.OverflowFrom >= 0L then
                true,
                (if nextSlot >= state.CurrentPage.GetCountOfRecords() then
                    let nextBlockNo = getOverflowBlockNo state.CurrentPage

                    let nextPage =
                        moveTo txBuffer txConcurrency txRecovery schema state.CurrentPage nextBlockNo

                    if nextBlockNo = state.OverflowFrom then
                        { state with
                              CurrentPage = nextPage
                              CurrentSlot = 0
                              MoveFrom = fromBlockNo
                              OverflowFrom = -1L }
                    else
                        { state with
                              CurrentPage = nextPage
                              CurrentSlot = 0
                              MoveFrom = fromBlockNo }
                 else
                     { state with CurrentSlot = nextSlot })
            elif nextSlot >= state.CurrentPage.GetCountOfRecords() then
                let siblingBlockNo = getSiblingBlockNo state.CurrentPage
                if siblingBlockNo >= 0L then
                    searchNext
                        { state with
                              CurrentPage =
                                  moveTo txBuffer txConcurrency txRecovery schema state.CurrentPage siblingBlockNo
                              CurrentSlot = -1
                              MoveFrom = fromBlockNo }
                else
                    false, { state with CurrentSlot = nextSlot }
            elif getKey state.CurrentPage nextSlot keyType
                 |> state.SearchRange.MatchsKey then
                let overflowBlockNo = getOverflowBlockNo state.CurrentPage
                true,
                (if nextSlot = 0 && overflowBlockNo >= 0L then
                    { state with
                          CurrentPage =
                              moveTo txBuffer txConcurrency txRecovery schema state.CurrentPage overflowBlockNo
                          CurrentSlot = 0
                          MoveFrom = fromBlockNo
                          OverflowFrom = fromBlockNo }
                 else
                     { state with CurrentSlot = nextSlot })
            elif getKey state.CurrentPage nextSlot keyType
                 |> state.SearchRange.BetweenLowHigh then
                searchNext { state with CurrentSlot = nextSlot }
            else
                false, { state with CurrentSlot = nextSlot }

        searchNext state

    let insert txBuffer txConcurrency txRecovery schema blockId keyType key recordId =
        let getSlotNo keyType searchKey page =
            let rec binarySearch startSlot endSlot target =
                if startSlot < endSlot then
                    let middleSlot = startSlot + (endSlot - startSlot) / 2

                    let comp =
                        SearchKey.compare (getKey page middleSlot keyType) target

                    if comp > 0
                    then binarySearch startSlot middleSlot target
                    else binarySearch (middleSlot + 1) endSlot target
                else
                    endSlot - 1

            let countOfRecords = page.GetCountOfRecords()
            if countOfRecords = 0 then
                0
            elif SearchKey.compare (getKey page 0 keyType) searchKey > 0 then
                0
            else
                let slotNo = binarySearch 0 countOfRecords searchKey
                if SearchKey.compare (getKey page slotNo keyType) searchKey
                   <= 0 then
                    slotNo + 1
                else
                    slotNo

        let splitOverflow page =
            let overflowBlockNo = getOverflowBlockNo page

            let overflowFrom =
                if overflowBlockNo >= 0L then overflowBlockNo else BlockId.blockNo page.BlockId

            page.Split 1 [ overflowFrom; -1L ]
            |> setOverflowBlockNo page

        let splitSibling page =
            let searchSibling (page: BTreePage) =
                let mutable slotNo = page.GetCountOfRecords() / 2
                let key = getKey page slotNo keyType
                if SearchKey.compare key (getKey page 0 keyType) = 0 then
                    while SearchKey.compare key (getKey page slotNo keyType) = 0 do
                        slotNo <- slotNo + 1
                else
                    while SearchKey.compare key (getKey page (slotNo - 1) keyType) = 0 do
                        slotNo <- slotNo - 1
                slotNo

            let siblingSlot = searchSibling page
            let siblingKey = getKey page siblingSlot keyType

            let siblingBlockNo =
                page.Split siblingSlot [ -1L; getSiblingBlockNo page ]

            setSiblingBlockNo page siblingBlockNo

            newBTreeBranchEntry siblingKey siblingBlockNo

        let page =
            initBTreePage txBuffer txConcurrency txRecovery schema blockId

        getSlotNo keyType key page
        |> insertSlot txRecovery keyType page key recordId

        if page.IsFull() then
            if SearchKey.compare (getKey page 0 keyType) (getKey page (page.GetCountOfRecords() - 1) keyType) = 0 then
                splitOverflow page
                None
            else
                splitSibling page |> Some
        else
            None

    let delete txBuffer txConcurrency txRecovery dataFileName schema blockId keyType key recordId =
        let rec searchDelete state =
            let result, newstate =
                next txBuffer txConcurrency txRecovery schema keyType state

            if result then
                if recordId = getDataRecordId dataFileName newstate.CurrentPage newstate.CurrentSlot then
                    deleteSlot txRecovery keyType newstate.CurrentPage newstate.CurrentSlot
                    true, newstate
                else
                    searchDelete newstate
            else
                false, newstate

        let fixOverflowFlag page fromBlockNo =
            use fromPage =
                BlockId.newBlockId (BlockId.fileName page.BlockId) fromBlockNo
                |> initBTreePage txBuffer txConcurrency txRecovery schema

            let overflowBlockNo = getOverflowBlockNo page
            if overflowBlockNo = fromBlockNo then -1L else overflowBlockNo
            |> setOverflowBlockNo fromPage

        let deleted, state =
            SearchRange.newSearchRangeBySearchKey key
            |> beforeFirst txBuffer txConcurrency txRecovery schema blockId keyType
            |> searchDelete

        if deleted
           && state.OverflowFrom
           >= 0L
           && state.CurrentPage.GetCountOfRecords() = 0 then
            fixOverflowFlag state.CurrentPage state.MoveFrom

let newBTreeLeaf txBuffer txConcurrency txRecovery dataFileName blockId keyType =
    let schema = BTreeLeaf.keyTypeToSchema keyType
    let mutable state: BTreeLeaf.BTreeLeafState option = None

    { BeforeFirst =
          fun searchRange ->
              state <-
                  BTreeLeaf.beforeFirst txBuffer txConcurrency txRecovery schema blockId keyType searchRange
                  |> Some
      Next =
          fun () ->
              match state with
              | Some st ->
                  let result, newstate =
                      BTreeLeaf.next txBuffer txConcurrency txRecovery schema keyType st

                  state <- Some newstate
                  result
              | _ -> false
      GetDataRecordId =
          fun () ->
              match state with
              | Some st -> BTreeLeaf.getDataRecordId dataFileName st.CurrentPage st.CurrentSlot
              | _ -> failwith "Closed leaf"
      Insert =
          fun key recordId -> BTreeLeaf.insert txBuffer txConcurrency txRecovery schema blockId keyType key recordId
      Delete =
          fun key recordId ->
              BTreeLeaf.delete txBuffer txConcurrency txRecovery dataFileName schema blockId keyType key recordId
      Close =
          fun () ->
              state
              |> Option.iter (fun st -> st.CurrentPage.Close())
              state <- None }
