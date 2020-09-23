module RyakDB.Index.BTreeIndex

open RyakDB.Storage
open RyakDB.Storage.File
open RyakDB.Table
open RyakDB.Index
open RyakDB.Buffer.Buffer
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

    let appendBlock txBuffer txConcurrency fileName schema flags =
        txConcurrency.ModifyFile fileName

        let buff =
            txBuffer.PinNew fileName (newBTreePageFormatter schema flags)

        txBuffer.Unpin buff
        buff.BlockId

    let getRoot txBuffer txConcurrency txRecovery keyType branchFileName =
        newBTreeBranch txBuffer txConcurrency txRecovery (BlockId.newBlockId branchFileName 0L) keyType

    let search txBuffer txConcurrency txRecovery indexInfo keyType branchFileName leafFileName searchRange purpose =
        let root =
            getRoot txBuffer txConcurrency txRecovery keyType branchFileName

        let leafBlockId =
            root.Search leafFileName (searchRange.GetMin()) purpose

        let branchesMayBeUpdated =
            if purpose = Insert then root.BranchesMayBeUpdated() else []

        root.Close()

        let leaf =
            newBTreeLeaf txBuffer txConcurrency txRecovery indexInfo.TableInfo.FileName leafBlockId keyType searchRange

        leaf, branchesMayBeUpdated

    let preLoadToMemory fileMgr txBuffer txConcurrency branchFileName leafFileName =
        let branchSize =
            fileSize fileMgr txConcurrency branchFileName

        for i in 0L .. branchSize - 1L do
            BlockId.newBlockId branchFileName i
            |> txBuffer.Pin
            |> ignore

        let leafSize =
            fileSize fileMgr txConcurrency leafFileName

        for i in 0L .. leafSize - 1L do
            BlockId.newBlockId leafFileName i
            |> txBuffer.Pin
            |> ignore

    let beforeFirst txBuffer txConcurrency txRecovery indexInfo keyType branchFileName leafFileName searchRange =
        if searchRange.IsValid() then
            let leaf, _ =
                search txBuffer txConcurrency txRecovery indexInfo keyType branchFileName leafFileName searchRange Read

            Some leaf
        else
            None

    let next leaf =
        match leaf with
        | Some (l) -> l.Next()
        | _ -> failwith "Must call beforeFirst()"

    let getDataRecordId leaf =
        match leaf with
        | Some (l) -> l.GetDataRecordId()
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

        let leaf, branchesMayBeUpdated =
            search
                txBuffer
                txConcurrency
                txRecovery
                indexInfo
                keyType
                branchFileName
                leafFileName
                (SearchRange.newSearchRangeBySearchKey key)
                Insert

        let entry = leaf.Insert dataRecordId
        leaf.Close()
        if Option.isSome entry then
            if doLogicalLogging then txRecovery.LogLogicalStart() |> ignore

            let mutable prevEntry = entry
            branchesMayBeUpdated
            |> List.iter (fun bid ->
                prevEntry
                |> Option.iter (fun e ->
                    let branch =
                        newBTreeBranch txBuffer txConcurrency txRecovery bid keyType

                    prevEntry <- branch.Insert e
                    branch.Close()))
            prevEntry
            |> Option.iter (fun e ->
                let root =
                    getRoot txBuffer txConcurrency txRecovery keyType branchFileName

                root.MakeNewRoot e
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

        let leaf, _ =
            search
                txBuffer
                txConcurrency
                txRecovery
                indexInfo
                keyType
                branchFileName
                leafFileName
                (SearchRange.newSearchRangeBySearchKey key)
                Delete

        if doLogicalLogging then txRecovery.LogLogicalStart() |> ignore

        leaf.Delete dataRecordId

        if doLogicalLogging then
            let (RecordId (slotNo, BlockId (_, blockNo))) = dataRecordId
            txRecovery.LogIndexDeletionEnd indexInfo.IndexName key blockNo slotNo
            |> ignore

    let close leaf = leaf |> Option.iter (fun l -> l.Close())

    let initLeaf fileMgr txBuffer txConcurrency keyType leafFileName =
        if fileSize fileMgr txConcurrency leafFileName = 0L then
            appendBlock txBuffer txConcurrency leafFileName (BTreeLeaf.keyTypeToSchema keyType) [ -1L; -1L ]
            |> ignore

    let initBranch fileMgr txBuffer txConcurrency keyType branchFileName =
        if fileSize fileMgr txConcurrency branchFileName = 0L then
            appendBlock txBuffer txConcurrency branchFileName (BTreeBranch.keyTypeToSchema keyType) [ 0L ]
            |> ignore

    let initRoot txBuffer txConcurrency txRecovery keyType branchFileName =
        let root =
            getRoot txBuffer txConcurrency txRecovery keyType branchFileName

        if root.GetCountOfRecords() = 0 then
            root.Insert(newBTreeBranchEntry (SearchKeyType.getMin keyType) 0L)
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
