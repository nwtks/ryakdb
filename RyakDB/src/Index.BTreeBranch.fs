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
      Search: SearchPurpose -> string -> SearchKey option -> BlockId
      Insert: BTreeBranchEntry -> BTreeBranchEntry option
      MakeNewRoot: BTreeBranchEntry -> unit
      Close: unit -> unit }

module BTreeBranch =
    let ChildBlockNo = "child"
    let KeyPrefix = "key"

    let getFileName indexName = indexName + "_branch.idx"

    let initBTreePage txBuffer txConcurrency txRecovery schema blockId =
        newBTreePage txBuffer txConcurrency txRecovery schema blockId 1

    let keyTypeToSchema (SearchKeyType types) =
        let sch = Schema.newSchema ()
        sch.AddField ChildBlockNo BigIntDbType
        types
        |> List.iteri (fun i t -> sch.AddField (KeyPrefix + i.ToString()) t)
        sch

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

    let getLevel page = page.GetFlag 0

    let setLevel page = page.SetFlag 0

    let findMatchingSlot keyType page searchKey =
        let rec binarySearch startSlot endSlot taret =
            if startSlot <= endSlot then
                let middleSlot = startSlot + (endSlot - startSlot) / 2

                let comp =
                    SearchKey.compare (getKey page middleSlot keyType) taret

                if comp < 0
                then binarySearch (middleSlot + 1) endSlot taret
                elif comp > 0
                then binarySearch startSlot (middleSlot - 1) taret
                else middleSlot
            else
                endSlot

        let countOfRecords = page.GetCountOfRecords()
        if countOfRecords = 0 then
            0
        else
            match searchKey with
            | Some key -> binarySearch 0 (countOfRecords - 1) key
            | _ -> 0

    let findChildBlockNo keyType page searchKey =
        findMatchingSlot keyType page searchKey
        |> getChildBlockNo page

    let searchForInsert txBuffer txConcurrency txRecovery schema keyType page leafFileName searchKey =
        let rec searchChild branchesMayBeUpdated page childBlockNo =
            if getLevel page > 0L then
                let (BlockId (pagefile, _)) = page.BlockId
                let childBlockId = BlockId.newBlockId pagefile childBlockNo
                txConcurrency.CrabDownBranchBlockForModification childBlockId

                let childPage =
                    initBTreePage txBuffer txConcurrency txRecovery schema childBlockId

                let branchesMayBeUpdated =
                    if childPage.WillFull() then
                        childBlockId :: branchesMayBeUpdated
                    else
                        branchesMayBeUpdated
                        |> List.iter txConcurrency.CrabBackBranchBlockForModification
                        [ childBlockId ]

                page.Close()

                findChildBlockNo keyType childPage searchKey
                |> searchChild branchesMayBeUpdated childPage
            else
                branchesMayBeUpdated, page, childBlockNo

        txConcurrency.CrabDownBranchBlockForModification page.BlockId

        let branchesMayBeUpdated, currentPage, childBlockNo =
            findChildBlockNo keyType page searchKey
            |> searchChild [ page.BlockId ] page

        let leafBlockId =
            BlockId.newBlockId leafFileName childBlockNo

        txConcurrency.ModifyLeafBlock leafBlockId
        leafBlockId, currentPage, branchesMayBeUpdated

    let searchForDelete txBuffer txConcurrency txRecovery schema keyType page leafFileName searchKey =
        let rec searchChild page childBlockNo =
            if getLevel page > 0L then
                let (BlockId (pagefile, _)) = page.BlockId
                let childBlockId = BlockId.newBlockId pagefile childBlockNo
                txConcurrency.CrabDownBranchBlockForRead childBlockId

                let childPage =
                    initBTreePage txBuffer txConcurrency txRecovery schema childBlockId

                txConcurrency.CrabBackBranchBlockForRead page.BlockId

                page.Close()

                findChildBlockNo keyType childPage searchKey
                |> searchChild childPage
            else
                page, childBlockNo

        txConcurrency.CrabDownBranchBlockForRead page.BlockId

        let currentPage, childBlockNo =
            findChildBlockNo keyType page searchKey
            |> searchChild page

        let leafBlockId =
            BlockId.newBlockId leafFileName childBlockNo

        txConcurrency.ModifyLeafBlock leafBlockId
        txConcurrency.CrabDownBranchBlockForRead currentPage.BlockId

        leafBlockId, currentPage, []

    let searchForRead txBuffer txConcurrency txRecovery schema keyType page leafFileName searchKey =
        let rec searchChild page childBlockNo =
            if getLevel page > 0L then
                let (BlockId (pagefile, _)) = page.BlockId
                let childBlockId = BlockId.newBlockId pagefile childBlockNo
                txConcurrency.CrabDownBranchBlockForRead childBlockId

                let childPage =
                    initBTreePage txBuffer txConcurrency txRecovery schema childBlockId

                txConcurrency.CrabBackBranchBlockForRead page.BlockId

                page.Close()

                findChildBlockNo keyType childPage searchKey
                |> searchChild childPage
            else
                page, childBlockNo

        txConcurrency.CrabDownBranchBlockForRead page.BlockId

        let currentPage, childBlockNo =
            findChildBlockNo keyType page searchKey
            |> searchChild page

        let leafBlockId =
            BlockId.newBlockId leafFileName childBlockNo

        txConcurrency.ReadLeafBlock leafBlockId
        txConcurrency.CrabDownBranchBlockForRead currentPage.BlockId

        leafBlockId, currentPage, []

    let search txBuffer txConcurrency txRecovery schema keyType page purpose leafFileName searchKey =
        match purpose with
        | Insert -> searchForInsert txBuffer txConcurrency txRecovery schema keyType page leafFileName searchKey
        | Delete -> searchForDelete txBuffer txConcurrency txRecovery schema keyType page leafFileName searchKey
        | Read -> searchForRead txBuffer txConcurrency txRecovery schema keyType page leafFileName searchKey

    let insert txRecovery keyType (page: BTreePage) (BTreeBranchEntry (key, blockNo)) =
        if page.GetCountOfRecords() > 0
        then (findMatchingSlot keyType page (Some key)) + 1
        else 0
        |> insertSlot txRecovery keyType page key blockNo

        if page.IsFull() then
            let splitPos = page.GetCountOfRecords() / 2
            let splitKey = getKey page splitPos keyType
            let splitBlockNo = page.Split splitPos [ getLevel page ]
            Some(newBTreeBranchEntry splitKey splitBlockNo)
        else
            None

    let makeNewRoot txBuffer txConcurrency txRecovery schema keyType page entry =
        let (BlockId (fileName, blockNo)) = page.BlockId

        let rootPage =
            if blockNo = 0L then
                page
            else
                page.Close()
                BlockId.newBlockId fileName 0L
                |> initBTreePage txBuffer txConcurrency txRecovery schema

        let firstKey = getKey rootPage 0 keyType
        let level = getLevel rootPage

        rootPage.Split 0 [ level ]
        |> newBTreeBranchEntry firstKey
        |> insert txRecovery keyType rootPage
        |> ignore

        insert txRecovery keyType rootPage entry |> ignore

        level + 1L |> setLevel rootPage

        rootPage

let newBTreeBranch txBuffer txConcurrency txRecovery blockId keyType =
    let schema = BTreeBranch.keyTypeToSchema keyType

    let mutable page =
        BTreeBranch.initBTreePage txBuffer txConcurrency txRecovery schema blockId
        |> Some

    let mutable branchesMayBeUpdated = []

    { GetCountOfRecords =
          fun () ->
              match page with
              | Some pg -> pg.GetCountOfRecords()
              | _ -> failwith "Closed branch"
      BranchesMayBeUpdated = fun () -> branchesMayBeUpdated
      Search =
          fun purpose leafFileName searchKey ->
              match page with
              | Some pg ->
                  let leafBlockId, nextPage, nextMayBeUpdated =
                      BTreeBranch.search
                          txBuffer
                          txConcurrency
                          txRecovery
                          schema
                          keyType
                          pg
                          purpose
                          leafFileName
                          searchKey

                  branchesMayBeUpdated <- nextMayBeUpdated
                  page <- Some nextPage
                  leafBlockId
              | _ -> failwith "Closed branch"
      Insert =
          match page with
          | Some pg -> BTreeBranch.insert txRecovery keyType pg
          | _ -> failwith "Closed branch"
      MakeNewRoot =
          fun entry ->
              match page with
              | Some pg ->
                  page <-
                      BTreeBranch.makeNewRoot txBuffer txConcurrency txRecovery schema keyType pg entry
                      |> Some
              | _ -> failwith "Closed branch"
      Close =
          fun () ->
              page |> Option.iter (fun pg -> pg.Close())
              page <- None
              branchesMayBeUpdated <- [] }
