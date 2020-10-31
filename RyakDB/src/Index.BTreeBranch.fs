module RyakDB.Index.BTreeBranch

open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Table
open RyakDB.Index
open RyakDB.Concurrency.TransactionConcurrency
open RyakDB.Recovery.TransactionRecovery
open RyakDB.Index.BTreePage

type BTreeBranchEntry = BTreeBranchEntry of key: SearchKey * blockNo: int64

let inline newBTreeBranchEntry key blockNo = BTreeBranchEntry(key, blockNo)

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
    interface System.IDisposable with
        member this.Dispose() = this.Close()

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

    let getKey page slotNo (SearchKeyType keys) =
        keys
        |> List.mapi (fun i _ -> page.GetVal slotNo (KeyPrefix + i.ToString()))
        |> SearchKey.newSearchKey

    let getChildBlockNo page slotNo =
        page.GetVal slotNo ChildBlockNo
        |> DbConstant.toLong

    let insertSlot txRecovery keyType page (SearchKey keys) blockNo slotNo =
        txRecovery.LogIndexPageInsertion false keyType page.BlockId slotNo
        |> ignore

        page.Insert slotNo

        page.SetVal slotNo ChildBlockNo (BigIntDbConstant blockNo)

        keys
        |> List.iteri (fun i k -> page.SetVal slotNo (KeyPrefix + i.ToString()) k)

    let getLevel page = page.GetFlag 0

    let setLevel page = page.SetFlag 0

    let findMatchingSlot keyType page searchKey =
        let rec binarySearch startSlot endSlot target =
            if startSlot <= endSlot then
                let middleSlot = startSlot + (endSlot - startSlot) / 2

                let comp =
                    SearchKey.compare (getKey page middleSlot keyType) target

                if comp < 0
                then binarySearch (middleSlot + 1) endSlot target
                elif comp > 0
                then binarySearch startSlot (middleSlot - 1) target
                else middleSlot
            else
                endSlot

        match page.GetCountOfRecords(), searchKey with
        | 0, _ -> 0
        | countOfRecords, Some key -> binarySearch 0 (countOfRecords - 1) key
        | _ -> 0

    let findChildBlockNo keyType page searchKey =
        findMatchingSlot keyType page searchKey
        |> getChildBlockNo page

    let searchForInsert txBuffer txConcurrency txRecovery schema keyType page leafFileName searchKey =
        let rec searchChild branchesMayBeUpdated page childBlockNo =
            match getLevel page with
            | 0L -> branchesMayBeUpdated, page, childBlockNo
            | _ ->
                let childBlockId =
                    BlockId.newBlockId (BlockId.fileName page.BlockId) childBlockNo

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

        txConcurrency.CrabDownBranchBlockForModification page.BlockId

        let branchesMayBeUpdated, currentPage, childBlockNo =
            findChildBlockNo keyType page searchKey
            |> searchChild [ page.BlockId ] page

        let leafBlockId =
            BlockId.newBlockId leafFileName childBlockNo

        txConcurrency.ModifyLeafBlock leafBlockId
        leafBlockId, currentPage, branchesMayBeUpdated

    let searchForRead txBuffer txConcurrency txRecovery schema keyType page leafFileName searchKey lockLeaf =
        let rec searchChild page childBlockNo =
            match getLevel page with
            | 0L -> page, childBlockNo
            | _ ->
                let childBlockId =
                    BlockId.newBlockId (BlockId.fileName page.BlockId) childBlockNo

                txConcurrency.CrabDownBranchBlockForRead childBlockId

                let childPage =
                    initBTreePage txBuffer txConcurrency txRecovery schema childBlockId

                txConcurrency.CrabBackBranchBlockForRead page.BlockId

                page.Close()

                findChildBlockNo keyType childPage searchKey
                |> searchChild childPage

        txConcurrency.CrabDownBranchBlockForRead page.BlockId

        let currentPage, childBlockNo =
            findChildBlockNo keyType page searchKey
            |> searchChild page

        let leafBlockId =
            BlockId.newBlockId leafFileName childBlockNo

        lockLeaf leafBlockId
        txConcurrency.CrabDownBranchBlockForRead currentPage.BlockId

        leafBlockId, currentPage, []

    let search txBuffer txConcurrency txRecovery schema keyType page purpose leafFileName searchKey =
        match purpose with
        | Insert -> searchForInsert txBuffer txConcurrency txRecovery schema keyType page leafFileName searchKey
        | Delete ->
            searchForRead
                txBuffer
                txConcurrency
                txRecovery
                schema
                keyType
                page
                leafFileName
                searchKey
                txConcurrency.ModifyLeafBlock
        | Read ->
            searchForRead
                txBuffer
                txConcurrency
                txRecovery
                schema
                keyType
                page
                leafFileName
                searchKey
                txConcurrency.ReadLeafBlock

    let insert txRecovery keyType (page: BTreePage) (BTreeBranchEntry (key, blockNo)) =
        match page.GetCountOfRecords() with
        | 0 -> 0
        | _ -> 1 + (Some key |> findMatchingSlot keyType page)
        |> insertSlot txRecovery keyType page key blockNo

        if page.IsFull() then
            let splitPos = page.GetCountOfRecords() / 2
            let splitKey = getKey page splitPos keyType
            let splitBlockNo = page.Split splitPos [ getLevel page ]
            newBTreeBranchEntry splitKey splitBlockNo |> Some
        else
            None

    let makeNewRoot txBuffer txConcurrency txRecovery schema keyType page entry =
        let rootPage =
            match BlockId.blockNo page.BlockId with
            | 0L -> page
            | _ ->
                page.Close()
                BlockId.newBlockId (BlockId.fileName page.BlockId) 0L
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
