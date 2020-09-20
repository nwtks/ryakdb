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
    type BTreeIndexState =
        { Leaf: BTreeLeaf
          BranchesMayBeUpdated: BlockId list }

    let fileSize (fileMgr: FileManager) txConcurrency fileName =
        txConcurrency.ReadFile fileName
        fileMgr.Size fileName

    let appendBlock txBuffer txConcurrency fileName schema flags =
        txConcurrency.ModifyFile fileName

        let buff =
            txBuffer.PinNew fileName (newBTreePageFormatter schema flags)

        txBuffer.Unpin buff
        buff.BlockId

    let createRoot txBuffer txConcurrency txRecovery keyType branchFileName =
        newBTreeBranch txBuffer txConcurrency txRecovery (BlockId.newBlockId branchFileName 0L) keyType

    let search txBuffer txConcurrency txRecovery indexInfo keyType branchFileName leafFileName searchRange purpose =
        let root =
            createRoot txBuffer txConcurrency txRecovery keyType branchFileName

        let leafBlockId =
            root.Search leafFileName (searchRange.GetMin()) purpose

        let branchesMayBeUpdated =
            if purpose = Insert then root.BranchesMayBeUpdated() else []

        root.Close()

        let leaf =
            newBTreeLeaf txBuffer txConcurrency txRecovery indexInfo.TableInfo.FileName leafBlockId keyType searchRange

        { Leaf = leaf
          BranchesMayBeUpdated = branchesMayBeUpdated }

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
        if searchRange.IsValid()
        then Some
                 (search
                     txBuffer
                      txConcurrency
                      txRecovery
                      indexInfo
                      keyType
                      branchFileName
                      leafFileName
                      searchRange
                      Read)
        else None

    let next state =
        match state with
        | Some ({ Leaf = leaf }) -> leaf.Next()
        | _ -> failwith "Must call beforeFirst()"

    let getDataRecordId state =
        match state with
        | Some ({ Leaf = leaf }) -> leaf.GetDataRecordId()
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
        let insertEntry entry branchBlockId =
            match entry with
            | Some (e) ->
                let branch =
                    newBTreeBranch txBuffer txConcurrency txRecovery branchBlockId keyType

                let newEntry = branch.Insert e
                branch.Close()
                newEntry
            | _ -> None

        if txReadOnly then failwith "Transaction read only"

        let newstate =
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

        let entry = newstate.Leaf.Insert dataRecordId
        newstate.Leaf.Close()
        if Option.isSome entry then
            if doLogicalLogging then txRecovery.LogLogicalStart() |> ignore

            match newstate.BranchesMayBeUpdated
                  |> List.fold insertEntry entry with
            | Some (e) ->
                let root =
                    createRoot txBuffer txConcurrency txRecovery keyType branchFileName

                root.MakeNewRoot e
                root.Close()
            | _ -> ()

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

        let newstate =
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

        newstate.Leaf.Delete dataRecordId

        if doLogicalLogging then
            let (RecordId (slotNo, BlockId (_, blockNo))) = dataRecordId
            txRecovery.LogIndexDeletionEnd indexInfo.IndexName key blockNo slotNo
            |> ignore

    let close state =
        state |> Option.iter (fun s -> s.Leaf.Close())

let newBTreeIndex fileMgr txBuffer txConcurrency txRecovery txReadOnly indexInfo keyType =
    let leafFileName =
        BTreeLeaf.getFileName indexInfo.IndexName

    if BTreeIndex.fileSize fileMgr txConcurrency leafFileName = 0L then
        BTreeIndex.appendBlock txBuffer txConcurrency leafFileName (BTreeLeaf.keyTypeToSchema keyType) [ -1L; -1L ]
        |> ignore

    let branchFileName =
        BTreeBranch.getFileName indexInfo.IndexName

    if BTreeIndex.fileSize fileMgr txConcurrency branchFileName = 0L then
        BTreeIndex.appendBlock txBuffer txConcurrency branchFileName (BTreeBranch.keyTypeToSchema keyType) [ 0L ]
        |> ignore

    let root =
        BTreeIndex.createRoot txBuffer txConcurrency txRecovery keyType branchFileName

    if root.GetCountOfRecords() = 0 then
        root.Insert(newBTreeBranchEntry (SearchKeyType.getMin keyType) 0L)
        |> ignore
    root.Close()

    let mutable state = None
    { BeforeFirst =
          fun searchRange ->
              BTreeIndex.close state
              state <-
                  BTreeIndex.beforeFirst
                      txBuffer
                      txConcurrency
                      txRecovery
                      indexInfo
                      keyType
                      branchFileName
                      leafFileName
                      searchRange
      Next = fun () -> BTreeIndex.next state
      GetDataRecordId = fun () -> BTreeIndex.getDataRecordId state
      Insert =
          fun doLogicalLogging key dataRecordId ->
              BTreeIndex.close state
              state <- None
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
              BTreeIndex.close state
              state <- None
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
              BTreeIndex.close state
              state <- None
      PreLoadToMemory = fun () -> BTreeIndex.preLoadToMemory fileMgr txBuffer txConcurrency branchFileName leafFileName }
