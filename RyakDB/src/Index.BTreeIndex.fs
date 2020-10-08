module RyakDB.Index.BTreeIndex

open RyakDB.Storage
open RyakDB.Storage.File
open RyakDB.Table
open RyakDB.Index
open RyakDB.Buffer.TransactionBuffer
open RyakDB.Concurrency.TransactionConcurrency
open RyakDB.Recovery.TransactionRecovery
open RyakDB.Index.BTreePage
open RyakDB.Index.BTreeBranch
open RyakDB.Index.BTreeLeaf

module BTreeIndex =
    let fileSize (fileMgr: FileManager) txConcurrency fileName =
        txConcurrency.ReadFile fileName
        fileMgr.Size fileName

    let getRoot txBuffer txConcurrency txRecovery keyType branchFileName =
        newBTreeBranch txBuffer txConcurrency txRecovery (BlockId.newBlockId branchFileName 0L) keyType

    let search txBuffer txConcurrency txRecovery indexInfo keyType branchFileName purpose leafFileName searchRange =
        let root =
            getRoot txBuffer txConcurrency txRecovery keyType branchFileName

        let leafBlockId =
            searchRange.GetMin()
            |> root.Search purpose leafFileName

        let branchesMayBeUpdated = root.BranchesMayBeUpdated()
        root.Close()

        let leaf =
            newBTreeLeaf txBuffer txConcurrency txRecovery indexInfo.TableInfo.FileName leafBlockId keyType searchRange

        leaf, branchesMayBeUpdated

    let preLoadToMemory fileMgr txBuffer txConcurrency branchFileName leafFileName =
        let branchSize =
            fileSize fileMgr txConcurrency branchFileName

        [ 0L .. branchSize - 1L ]
        |> List.iter (fun i ->
            BlockId.newBlockId branchFileName i
            |> txBuffer.Pin
            |> ignore)

        let leafSize =
            fileSize fileMgr txConcurrency leafFileName

        [ 0L .. leafSize - 1L ]
        |> List.iter (fun i ->
            BlockId.newBlockId leafFileName i
            |> txBuffer.Pin
            |> ignore)

    let beforeFirst txBuffer txConcurrency txRecovery indexInfo keyType branchFileName leafFileName searchRange =
        if searchRange.IsValid() then
            let leaf, _ =
                search txBuffer txConcurrency txRecovery indexInfo keyType branchFileName Read leafFileName searchRange

            Some leaf
        else
            None

    let next leaf =
        match leaf with
        | Some l -> l.Next()
        | _ -> false

    let getDataRecordId leaf =
        match leaf with
        | Some l -> l.GetDataRecordId()
        | _ -> failwith "Must call beforeFirst()"

    let insert txBuffer
               txConcurrency
               txRecovery
               txReadOnly
               indexInfo
               keyType
               branchFileName
               leafFileName
               doLogicalLogging
               key
               dataRecordId
               =
        if txReadOnly then failwith "Transaction read only"

        if doLogicalLogging then txRecovery.LogLogicalStart() |> ignore

        let leaf, branchesMayBeUpdated =
            SearchRange.newSearchRangeBySearchKey key
            |> search txBuffer txConcurrency txRecovery indexInfo keyType branchFileName Insert leafFileName

        let splitedLeaf = leaf.Insert dataRecordId
        leaf.Close()

        if Option.isSome splitedLeaf then
            branchesMayBeUpdated
            |> List.fold (fun splitedBranch branchBlockId ->
                match splitedBranch with
                | Some entry ->
                    let branch =
                        newBTreeBranch txBuffer txConcurrency txRecovery branchBlockId keyType

                    let prevEntry = branch.Insert entry
                    branch.Close()
                    prevEntry
                | _ -> None) splitedLeaf
            |> Option.iter (fun entry ->
                let root =
                    getRoot txBuffer txConcurrency txRecovery keyType branchFileName

                root.MakeNewRoot entry
                root.Close())

        if doLogicalLogging then
            let (RecordId (slotNo, BlockId (_, blockNo))) = dataRecordId
            txRecovery.LogIndexInsertionEnd indexInfo.IndexName key blockNo slotNo
            |> ignore

    let delete txBuffer
               txConcurrency
               txRecovery
               txReadOnly
               indexInfo
               keyType
               branchFileName
               leafFileName
               doLogicalLogging
               key
               dataRecordId
               =
        if txReadOnly then failwith "Transaction read only"

        if doLogicalLogging then txRecovery.LogLogicalStart() |> ignore

        let leaf, _ =
            SearchRange.newSearchRangeBySearchKey key
            |> search txBuffer txConcurrency txRecovery indexInfo keyType branchFileName Delete leafFileName

        leaf.Delete dataRecordId
        leaf.Close()

        if doLogicalLogging then
            let (RecordId (slotNo, BlockId (_, blockNo))) = dataRecordId
            txRecovery.LogIndexDeletionEnd indexInfo.IndexName key blockNo slotNo
            |> ignore

    let close leaf = leaf |> Option.iter (fun l -> l.Close())

    let initLeaf fileMgr txBuffer txConcurrency keyType leafFileName =
        if fileSize fileMgr txConcurrency leafFileName = 0L then
            BTreePage.appendBlock txBuffer txConcurrency (BTreeLeaf.keyTypeToSchema keyType) leafFileName [ -1L; -1L ]
            |> ignore

    let initBranch fileMgr txBuffer txConcurrency keyType branchFileName =
        if fileSize fileMgr txConcurrency branchFileName = 0L then
            BTreePage.appendBlock txBuffer txConcurrency (BTreeBranch.keyTypeToSchema keyType) branchFileName [ 0L ]
            |> ignore

    let initRoot txBuffer txConcurrency txRecovery keyType branchFileName =
        let root =
            getRoot txBuffer txConcurrency txRecovery keyType branchFileName

        if root.GetCountOfRecords() = 0 then
            newBTreeBranchEntry (SearchKeyType.getMin keyType) 0L
            |> root.Insert
            |> ignore

        root.Close()

let newBTreeIndex fileMgr txBuffer txConcurrency txRecovery txReadOnly indexInfo keyType =
    let leafFileName =
        BTreeLeaf.getFileName indexInfo.IndexName

    let branchFileName =
        BTreeBranch.getFileName indexInfo.IndexName

    BTreeIndex.initLeaf fileMgr txBuffer txConcurrency keyType leafFileName
    BTreeIndex.initBranch fileMgr txBuffer txConcurrency keyType branchFileName
    BTreeIndex.initRoot txBuffer txConcurrency txRecovery keyType branchFileName

    let mutable leaf = None
    { BeforeFirst =
          fun searchRange ->
              BTreeIndex.close leaf
              leaf <-
                  BTreeIndex.beforeFirst
                      txBuffer
                      txConcurrency
                      txRecovery
                      indexInfo
                      keyType
                      branchFileName
                      leafFileName
                      searchRange
      Next = fun () -> BTreeIndex.next leaf
      GetDataRecordId = fun () -> BTreeIndex.getDataRecordId leaf
      Insert =
          fun doLogicalLogging key dataRecordId ->
              BTreeIndex.close leaf
              leaf <- None
              BTreeIndex.insert
                  txBuffer
                  txConcurrency
                  txRecovery
                  txReadOnly
                  indexInfo
                  keyType
                  branchFileName
                  leafFileName
                  doLogicalLogging
                  key
                  dataRecordId
      Delete =
          fun doLogicalLogging key dataRecordId ->
              BTreeIndex.close leaf
              leaf <- None
              BTreeIndex.delete
                  txBuffer
                  txConcurrency
                  txRecovery
                  txReadOnly
                  indexInfo
                  keyType
                  branchFileName
                  leafFileName
                  doLogicalLogging
                  key
                  dataRecordId
      Close =
          fun () ->
              BTreeIndex.close leaf
              leaf <- None
      PreLoadToMemory = fun () -> BTreeIndex.preLoadToMemory fileMgr txBuffer txConcurrency branchFileName leafFileName }
