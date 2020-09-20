module RyakDB.Index.BTreeBranch

open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Table
open RyakDB.Index
open RyakDB.Concurrency.TransactionConcurrency
open RyakDB.Recovery.TransactionRecovery
open RyakDB.Index.BTreePage

type BTreeBranchEntry = BTreeBranchEntry of key: SearchKey * blockNo: int64

let newBTreeBranchEntry key blockNo = BTreeBranchEntry(key, blockNo)

type SearchPurpose =
    | Read
    | Insert
    | Delete

type BTreeBranch =
    { GetCountOfRecords: unit -> int32
      BranchesMayBeUpdated: unit -> BlockId list
      Search: string -> SearchKey -> SearchPurpose -> BlockId
      Insert: BTreeBranchEntry -> BTreeBranchEntry option
      MakeNewRoot: BTreeBranchEntry -> unit
      Close: unit -> unit }

module BTreeBranch =
    let ChildBlockNo = "child"
    let KeyPrefix = "key"

    let getFileName indexName = indexName + "_branch.idx"

    let keyTypeToSchema (SearchKeyType types) =
        let sch = Schema.newSchema ()
        sch.AddField ChildBlockNo BigIntDbType
        types
        |> List.iteri (fun i t -> sch.AddField (KeyPrefix + i.ToString()) t)
        sch

    let initBTreePage txBuffer txConcurrency txRecovery schema blockId =
        newBTreePage txBuffer txConcurrency txRecovery schema blockId 1

    let getKey page slot (SearchKeyType (keys)) =
        keys
        |> List.mapi (fun i _ -> page.GetVal slot (KeyPrefix + i.ToString()))
        |> SearchKey.newSearchKey

    let getChildBlockNo page slot =
        page.GetVal slot ChildBlockNo |> DbConstant.toLong

    let insertSlot txRecovery keyType page (SearchKey keys) blockNo slot =
        txRecovery.LogIndexPageInsertion false page.BlockId keyType slot
        |> ignore
        page.Insert slot
        page.SetVal slot ChildBlockNo (BigIntDbConstant blockNo)
        keys
        |> List.iteri (fun i k -> page.SetVal slot (KeyPrefix + i.ToString()) k)

    let getLevelFlag page = page.GetFlag 0

    let setLevelFlag page = page.SetFlag 0

    let findMatchingSlot keyType page searchKey =
        let rec loopSlot startSlot endSlot =
            let middleSlot = (startSlot + endSlot) / 2
            if startSlot <> middleSlot then
                let key = getKey page middleSlot keyType
                if key < searchKey then loopSlot middleSlot endSlot else loopSlot startSlot middleSlot
            else
                startSlot, endSlot

        let endSlot = page.GetCountOfRecords() - 1
        if endSlot < 0 then
            0
        else
            let startSlot, endSlot = loopSlot 0 endSlot
            let key = getKey page endSlot keyType
            if key <= searchKey then endSlot else startSlot

    let findChildBlockNo keyType page searchKey =
        findMatchingSlot keyType page searchKey
        |> getChildBlockNo page

    let searchForInsert txBuffer txConcurrency txRecovery schema keyType page leafFileName searchKey =
        let rec loopSearch currentPage childBlockNo branchesMayBeUpdated =
            if getLevelFlag currentPage > 0L then
                let (BlockId (pagefile, _)) = currentPage.BlockId
                let childBlockId = BlockId.newBlockId pagefile childBlockNo
                txConcurrency.CrabDownBranchBlockForModification childBlockId

                let childPage =
                    initBTreePage txBuffer txConcurrency txRecovery schema childBlockId

                let branchesMayBeUpdated =
                    if childPage.IsGettingFull() then
                        childBlockId :: branchesMayBeUpdated
                    else
                        branchesMayBeUpdated
                        |> List.iter txConcurrency.CrabBackBranchBlockForModification
                        [ childBlockId ]

                currentPage.Close()
                loopSearch childPage (findChildBlockNo keyType childPage searchKey) branchesMayBeUpdated
            else
                currentPage, childBlockNo, branchesMayBeUpdated

        txConcurrency.CrabDownBranchBlockForModification page.BlockId

        let currentPage, childBlockNo, branchesMayBeUpdated =
            loopSearch page (findChildBlockNo keyType page searchKey) [ page.BlockId ]

        let leafBlockId =
            BlockId.newBlockId leafFileName childBlockNo

        txConcurrency.ModifyLeafBlock leafBlockId
        leafBlockId, currentPage, branchesMayBeUpdated |> List.rev

    let searchForRead txBuffer txConcurrency txRecovery schema keyType page leafFileName searchKey =
        let rec loopSearch currentPage childBlockNo =
            if getLevelFlag currentPage > 0L then
                let (BlockId (pagefile, _)) = currentPage.BlockId
                let childBlockId = BlockId.newBlockId pagefile childBlockNo
                txConcurrency.CrabDownBranchBlockForRead childBlockId

                let childPage =
                    initBTreePage txBuffer txConcurrency txRecovery schema childBlockId

                txConcurrency.CrabBackBranchBlockForRead currentPage.BlockId
                currentPage.Close()
                loopSearch childPage (findChildBlockNo keyType childPage searchKey)
            else
                currentPage, childBlockNo

        txConcurrency.CrabDownBranchBlockForRead page.BlockId

        let currentPage, childBlockNo =
            loopSearch page (findChildBlockNo keyType page searchKey)

        let leafBlockId =
            BlockId.newBlockId leafFileName childBlockNo

        txConcurrency.ReadLeafBlock leafBlockId
        txConcurrency.CrabDownBranchBlockForRead currentPage.BlockId
        leafBlockId, currentPage, []

    let search txBuffer txConcurrency txRecovery schema keyType page leafFileName searchKey purpose =
        match purpose with
        | Insert -> searchForInsert txBuffer txConcurrency txRecovery schema keyType page leafFileName searchKey
        | Delete
        | Read -> searchForRead txBuffer txConcurrency txRecovery schema keyType page leafFileName searchKey

    let insert txRecovery keyType (page: BTreePage) (BTreeBranchEntry (key, blockNo)) =
        let newSlot =
            if page.GetCountOfRecords() > 0 then (findMatchingSlot keyType page key) + 1 else 0

        insertSlot txRecovery keyType page key blockNo newSlot
        if page.IsFull() then
            let splitPos = page.GetCountOfRecords() / 2
            let splitKey = getKey page splitPos keyType

            let splitBlockNo =
                page.Split splitPos [ getLevelFlag page ]

            Some(newBTreeBranchEntry splitKey splitBlockNo)
        else
            None

    let makeNewRoot txBuffer txConcurrency txRecovery schema keyType page entry =
        let (BlockId (fileName, blockNo)) = page.BlockId

        let currentPage =
            if blockNo <> 0L then
                page.Close()
                initBTreePage txBuffer txConcurrency txRecovery schema (BlockId.newBlockId fileName 0L)
            else
                page

        let firstKey = getKey currentPage 0 keyType
        let level = getLevelFlag currentPage
        let splitBlockNo = currentPage.Split 0 [ level ]

        let oldRoot =
            newBTreeBranchEntry firstKey splitBlockNo

        insert txRecovery keyType currentPage oldRoot
        |> ignore
        insert txRecovery keyType currentPage entry
        |> ignore
        setLevelFlag currentPage (level + 1L)
        currentPage

let newBTreeBranch txBuffer txConcurrency txRecovery blockId keyType =
    let schema = BTreeBranch.keyTypeToSchema keyType

    let mutable page =
        BTreeBranch.initBTreePage txBuffer txConcurrency txRecovery schema blockId

    let mutable branchesMayBeUpdated = []

    { GetCountOfRecords = fun () -> page.GetCountOfRecords()
      BranchesMayBeUpdated = fun () -> branchesMayBeUpdated
      Search =
          fun leafFileName searchKey purpose ->
              let leafBlockId, nextPage, nextMayBeUpdated =
                  BTreeBranch.search
                      txBuffer
                      txConcurrency
                      txRecovery
                      schema
                      keyType
                      page
                      leafFileName
                      searchKey
                      purpose

              page <- nextPage
              branchesMayBeUpdated <- nextMayBeUpdated
              leafBlockId
      Insert = BTreeBranch.insert txRecovery keyType page
      MakeNewRoot =
          fun entry -> page <- BTreeBranch.makeNewRoot txBuffer txConcurrency txRecovery schema keyType page entry
      Close =
          fun () ->
              page.Close()
              branchesMayBeUpdated <- [] }
