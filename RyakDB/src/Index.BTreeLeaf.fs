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
          MoveFrom: int64
          IsOverflowing: bool
          OverflowFrom: int64 }

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

    let getOverflowBlockNo page = page.GetFlag 0

    let setOverflowBlockNo page = page.SetFlag 0

    let getSiblingBlockNo page = page.GetFlag 1

    let setSiblingBlockNo page = page.SetFlag 1

    let moveSlotBefore keyType searchRange page =
        let rec binarySearch startSlot endSlot taret =
            if startSlot < endSlot then
                let middleSlot = startSlot + (endSlot - startSlot) / 2

                let comp =
                    SearchKey.compare (getKey page middleSlot keyType) taret

                if comp < 0
                then binarySearch (middleSlot + 1) endSlot taret
                else binarySearch startSlot middleSlot taret
            else
                startSlot

        let countOfRecords = page.GetCountOfRecords()
        if countOfRecords <= 0 then
            -1
        else
            let searchMin = searchRange.GetMin()

            let slot =
                binarySearch 0 (countOfRecords - 1) searchMin

            if SearchKey.compare (getKey page slot keyType) searchMin
               >= 0 then
                slot - 1
            else
                slot

    let moveTo txBuffer txConcurrency txRecovery schema currentPage blockNo =
        let (BlockId (filename, _)) = currentPage.BlockId
        let blockId = BlockId.newBlockId filename blockNo
        txConcurrency.ReadLeafBlock blockId
        currentPage.Close()
        initBTreePage txBuffer txConcurrency txRecovery schema blockId

    let next txBuffer txConcurrency txRecovery schema keyType searchRange state =
        let rec loopNext state =
            let nextSlot = state.CurrentSlot + 1
            if state.IsOverflowing then
                if nextSlot >= state.CurrentPage.GetCountOfRecords() then
                    let (BlockId (_, currentBlockNo)) = state.CurrentPage.BlockId

                    let nextPage =
                        getOverflowBlockNo state.CurrentPage
                        |> moveTo txBuffer txConcurrency txRecovery schema state.CurrentPage

                    let (BlockId (_, nextBlockNo)) = nextPage.BlockId
                    if nextBlockNo = state.OverflowFrom then
                        true,
                        { CurrentPage = nextPage
                          CurrentSlot = 0
                          MoveFrom = currentBlockNo
                          IsOverflowing = false
                          OverflowFrom = -1L }
                    else
                        true,
                        { state with
                              CurrentPage = nextPage
                              CurrentSlot = 0
                              MoveFrom = currentBlockNo }
                else
                    true, { state with CurrentSlot = nextSlot }
            else if nextSlot >= state.CurrentPage.GetCountOfRecords() then
                if getSiblingBlockNo state.CurrentPage >= 0L then
                    let (BlockId (_, currentBlockNo)) = state.CurrentPage.BlockId
                    loopNext
                        { state with
                              CurrentPage =
                                  getSiblingBlockNo state.CurrentPage
                                  |> moveTo txBuffer txConcurrency txRecovery schema state.CurrentPage
                              CurrentSlot = -1
                              MoveFrom = currentBlockNo }
                else
                    false, { state with CurrentSlot = nextSlot }
            else if getKey state.CurrentPage nextSlot keyType
                    |> searchRange.MatchsKey then
                if nextSlot = 0
                   && getOverflowBlockNo state.CurrentPage >= 0L then
                    let (BlockId (_, currentBlockNo)) = state.CurrentPage.BlockId
                    true,
                    { CurrentPage =
                          getOverflowBlockNo state.CurrentPage
                          |> moveTo txBuffer txConcurrency txRecovery schema state.CurrentPage
                      CurrentSlot = 0
                      MoveFrom = currentBlockNo
                      IsOverflowing = true
                      OverflowFrom = currentBlockNo }
                else
                    true, { state with CurrentSlot = nextSlot }
            else
                false, { state with CurrentSlot = nextSlot }

        loopNext state

    let insert txRecovery keyType searchRange page slot recordId =
        let splitOverflow page =
            let (BlockId (_, currentBlockNo)) = page.BlockId
            let overflow = getOverflowBlockNo page

            let splitOverflow =
                if overflow >= 0L then overflow else currentBlockNo

            page.Split 1 [ splitOverflow; -1L ]
            |> setOverflowBlockNo page

        let splitSibling (page: BTreePage) firstKey =
            let mutable splitPos = page.GetCountOfRecords() / 2

            let splitKey = getKey page splitPos keyType

            let newSplitKey =
                if SearchKey.compare splitKey firstKey = 0 then
                    while SearchKey.compare splitKey (getKey page splitPos keyType) = 0 do
                        splitPos <- splitPos + 1
                    getKey page splitPos keyType
                else
                    while SearchKey.compare splitKey (getKey page (splitPos - 1) keyType) = 0 do
                        splitPos <- splitPos - 1
                    splitKey

            let siblingBlockNo =
                page.Split splitPos [ -1L; getSiblingBlockNo page ]

            setSiblingBlockNo page siblingBlockNo
            newBTreeBranchEntry newSplitKey siblingBlockNo

        if not (searchRange.IsSingleValue()) then failwith "Not supported"

        insertSlot txRecovery keyType page (searchRange.ToSearchKey()) recordId slot
        if page.IsFull() then
            let firstKey = getKey page 0 keyType

            let lastKey =
                getKey page (page.GetCountOfRecords() - 1) keyType

            if SearchKey.compare firstKey lastKey = 0 then
                splitOverflow page
                None
            else
                Some(splitSibling page firstKey)
        else
            None

    let delete txBuffer txConcurrency txRecovery dataFileName schema keyType searchRange state recordId =
        let rec loopDelete state =
            let result, newstate =
                next txBuffer txConcurrency txRecovery schema keyType searchRange state

            if result then
                if recordId = getDataRecordId dataFileName newstate.CurrentPage newstate.CurrentSlot then
                    deleteSlot txRecovery keyType newstate.CurrentPage newstate.CurrentSlot
                    true, newstate
                else
                    loopDelete newstate
            else
                false, newstate

        let transferOverflow page =
            let (BlockId (filename, blockNo)) = page.BlockId

            let overflowPageBlockId =
                getOverflowBlockNo page
                |> BlockId.newBlockId filename

            txConcurrency.ModifyLeafBlock overflowPageBlockId

            let overflowPage =
                initBTreePage txBuffer txConcurrency txRecovery schema overflowPageBlockId

            if page.GetCountOfRecords() = 0
               || (overflowPage.GetCountOfRecords() > 0
                   && SearchKey.compare (getKey page 0 keyType) (getKey overflowPage 0 keyType)
                      <> 0) then
                overflowPage.TransferRecords (overflowPage.GetCountOfRecords() - 1) page 0 1
                if overflowPage.GetCountOfRecords() = 0 then
                    let overflow = getOverflowBlockNo overflowPage
                    setOverflowBlockNo page (if overflow = blockNo then -1L else overflow)
                overflowPage.Close()

        let recoverOverflowFlag page moveFrom =
            let (BlockId (filename, _)) = page.BlockId

            let fromPage =
                BlockId.newBlockId filename moveFrom
                |> initBTreePage txBuffer txConcurrency txRecovery schema

            let overflow = getOverflowBlockNo page
            let (BlockId (_, blockNo)) = fromPage.BlockId
            setOverflowBlockNo fromPage (if overflow = blockNo then -1L else overflow)
            fromPage.Close()

        if not (searchRange.IsSingleValue()) then failwith "Not supported"

        let deleted, newstate = loopDelete state
        if deleted then
            if newstate.IsOverflowing then
                if newstate.CurrentPage.GetCountOfRecords() = 0
                then recoverOverflowFlag newstate.CurrentPage newstate.MoveFrom
            else if getOverflowBlockNo newstate.CurrentPage >= 0L then
                transferOverflow newstate.CurrentPage

        newstate

let newBTreeLeaf txBuffer txConcurrency txRecovery dataFileName blockId keyType searchRange =
    let schema = BTreeLeaf.keyTypeToSchema keyType

    let page =
        BTreeLeaf.initBTreePage txBuffer txConcurrency txRecovery schema blockId

    let mutable state: BTreeLeaf.BTreeLeafState option =
        Some
            { CurrentPage = page
              CurrentSlot = BTreeLeaf.moveSlotBefore keyType searchRange page
              MoveFrom = -1L
              IsOverflowing = false
              OverflowFrom = -1L }

    { Next =
          fun () ->
              match state with
              | Some (st) ->
                  let result, newstate =
                      BTreeLeaf.next txBuffer txConcurrency txRecovery schema keyType searchRange st

                  state <- Some newstate
                  result
              | _ -> failwith "Closed leaf"
      GetDataRecordId =
          fun () ->
              match state with
              | Some (st) -> BTreeLeaf.getDataRecordId dataFileName st.CurrentPage st.CurrentSlot
              | _ -> failwith "Closed leaf"
      GetCountOfRecords =
          fun () ->
              match state with
              | Some (st) -> st.CurrentPage.GetCountOfRecords()
              | _ -> failwith "Closed leaf"
      Insert =
          fun recordId ->
              match state with
              | Some (st) ->
                  let result =
                      BTreeLeaf.insert txRecovery keyType searchRange st.CurrentPage (st.CurrentSlot + 1) recordId

                  result
              | _ -> failwith "Closed leaf"
      Delete =
          fun recordId ->
              match state with
              | Some (st) ->
                  state <-
                      Some
                          (BTreeLeaf.delete
                              txBuffer
                               txConcurrency
                               txRecovery
                               dataFileName
                               schema
                               keyType
                               searchRange
                               st
                               recordId)
              | _ -> failwith "Closed leaf"
      Close =
          fun () ->
              state
              |> Option.iter (fun st -> st.CurrentPage.Close())
              state <- None }
