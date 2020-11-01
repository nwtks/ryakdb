module RyakDB.Index.BTreeIndex

open RyakDB.Storage
open RyakDB.Storage.File
open RyakDB.Table
open RyakDB.Index
open RyakDB.Transaction
open RyakDB.Index.BTreePage
open RyakDB.Index.BTreeBranch
open RyakDB.Index.BTreeLeaf

module BTreeIndex =
    let fileSize (fileService: FileService) tx fileName =
        tx.Concurrency.ReadFile fileName
        fileService.Size fileName

    let getRoot tx keyType branchFileName =
        newBTreeBranch tx (BlockId.newBlockId branchFileName 0L) keyType

    let search tx indexInfo keyType branchFileName purpose leafFileName searchKey =
        let leafBlockId, branchesMayBeUpdated =
            using (getRoot tx keyType branchFileName) (fun root ->
                root.Search purpose leafFileName searchKey, root.BranchesMayBeUpdated())

        newBTreeLeaf tx (IndexInfo.tableFileName indexInfo) leafBlockId keyType, branchesMayBeUpdated

    let loadToBuffer fileService tx branchFileName leafFileName =
        let branchSize = fileSize fileService tx branchFileName
        [ 0L .. branchSize - 1L ]
        |> List.iter (fun i ->
            BlockId.newBlockId branchFileName i
            |> tx.Buffer.Pin
            |> ignore)

        let leafSize = fileSize fileService tx leafFileName
        [ 0L .. leafSize - 1L ]
        |> List.iter (fun i ->
            BlockId.newBlockId leafFileName i
            |> tx.Buffer.Pin
            |> ignore)

    let beforeFirst tx indexInfo keyType branchFileName leafFileName searchRange =
        if searchRange.IsValid() then
            let leaf =
                searchRange.GetLow()
                |> search tx indexInfo keyType branchFileName Read leafFileName
                |> fst

            leaf.BeforeFirst searchRange
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

    let insert tx indexInfo keyType branchFileName leafFileName doLogicalLogging key dataRecordId =
        if tx.ReadOnly then failwith "Transaction read only"

        if doLogicalLogging then tx.Recovery.LogLogicalStart() |> ignore

        let leaf, branchesMayBeUpdated =
            Some key
            |> search tx indexInfo keyType branchFileName Insert leafFileName

        let splitedLeaf = leaf.Insert key dataRecordId
        leaf.Close()

        if splitedLeaf |> Option.isSome then
            branchesMayBeUpdated
            |> List.fold (fun splitedBranch branchBlockId ->
                match splitedBranch with
                | Some entry ->
                    use branch = newBTreeBranch tx branchBlockId keyType
                    branch.Insert entry
                | _ -> None) splitedLeaf
            |> Option.iter (fun entry ->
                use root = getRoot tx keyType branchFileName
                root.MakeNewRoot entry)

        if doLogicalLogging then
            tx.Recovery.LogIndexInsertionEnd
                (IndexInfo.indexName indexInfo)
                key
                (RecordId.blockNo dataRecordId)
                (RecordId.slotNo dataRecordId)
            |> ignore

    let delete tx indexInfo keyType branchFileName leafFileName doLogicalLogging key dataRecordId =
        if tx.ReadOnly then failwith "Transaction read only"

        if doLogicalLogging then tx.Recovery.LogLogicalStart() |> ignore

        use leaf =
            Some key
            |> search tx indexInfo keyType branchFileName Delete leafFileName
            |> fst

        leaf.Delete key dataRecordId

        if doLogicalLogging then
            tx.Recovery.LogIndexDeletionEnd
                (IndexInfo.indexName indexInfo)
                key
                (RecordId.blockNo dataRecordId)
                (RecordId.slotNo dataRecordId)
            |> ignore

    let close leaf = leaf |> Option.iter (fun l -> l.Close())

    let initLeaf fileService tx keyType leafFileName =
        if fileSize fileService tx leafFileName = 0L then
            BTreePage.appendBlock tx (BTreeLeaf.keyTypeToSchema keyType) leafFileName [ -1L; -1L ]
            |> ignore

    let initBranch fileService tx keyType branchFileName =
        if fileSize fileService tx branchFileName = 0L then
            BTreePage.appendBlock tx (BTreeBranch.keyTypeToSchema keyType) branchFileName [ 0L ]
            |> ignore

    let initRoot tx keyType branchFileName =
        use root = getRoot tx keyType branchFileName
        if root.GetCountOfRecords() = 0 then
            newBTreeBranchEntry (SearchKeyType.getMin keyType) 0L
            |> root.Insert
            |> ignore

let newBTreeIndex fileService tx indexInfo keyType =
    let leafFileName =
        IndexInfo.indexName indexInfo
        |> BTreeLeaf.getFileName

    let branchFileName =
        IndexInfo.indexName indexInfo
        |> BTreeBranch.getFileName

    BTreeIndex.initLeaf fileService tx keyType leafFileName
    BTreeIndex.initBranch fileService tx keyType branchFileName
    BTreeIndex.initRoot tx keyType branchFileName

    let mutable leaf = None
    { BeforeFirst =
          fun searchRange ->
              BTreeIndex.close leaf
              leaf <- BTreeIndex.beforeFirst tx indexInfo keyType branchFileName leafFileName searchRange
      Next = fun () -> BTreeIndex.next leaf
      GetDataRecordId = fun () -> BTreeIndex.getDataRecordId leaf
      Insert =
          fun doLogicalLogging key dataRecordId ->
              BTreeIndex.close leaf
              leaf <- None
              BTreeIndex.insert tx indexInfo keyType branchFileName leafFileName doLogicalLogging key dataRecordId
      Delete =
          fun doLogicalLogging key dataRecordId ->
              BTreeIndex.close leaf
              leaf <- None
              BTreeIndex.delete tx indexInfo keyType branchFileName leafFileName doLogicalLogging key dataRecordId
      Close =
          fun () ->
              BTreeIndex.close leaf
              leaf <- None
      LoadToBuffer = fun () -> BTreeIndex.loadToBuffer fileService tx branchFileName leafFileName }
