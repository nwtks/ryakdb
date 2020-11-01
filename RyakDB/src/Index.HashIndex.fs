module RyakDB.Index.HashIndex

open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Table
open RyakDB.Index
open RyakDB.Storage.File
open RyakDB.Transaction
open RyakDB.Table.TableFile

module HashIndex =
    type HashIndexState =
        { SearchKey: SearchKey
          TableFile: TableFile }

    let FieldBlockNo = "block_no"
    let FieldSlotNo = "slot_no"
    let KeyPrefix = "key"

    let fileSize fileService tx fileName =
        tx.Concurrency.ReadFile fileName
        fileService.Size fileName

    let keyTypeToSchema (SearchKeyType types) =
        let sch = Schema.newSchema ()
        sch.AddField FieldBlockNo BigIntDbType
        sch.AddField FieldSlotNo IntDbType
        types
        |> List.iteri (fun i t -> sch.AddField (KeyPrefix + i.ToString()) t)
        sch

    let getKey tableFile (SearchKeyType types) =
        types
        |> List.mapi (fun i _ -> tableFile.GetVal(KeyPrefix + i.ToString()))
        |> SearchKey.newSearchKey

    let loadToBuffer fileService tx indexName bucketsCount =
        [ 0 .. bucketsCount - 1 ]
        |> List.map (fun i -> indexName + i.ToString() + ".tbl")
        |> List.iter (fun tblName ->
            [ 0L .. fileSize fileService tx tblName - 1L ]
            |> List.iter (fun j ->
                BlockId.newBlockId tblName j
                |> tx.Buffer.Pin
                |> ignore))

    let beforeFirst fileService tx indexName keyType bucketsCount searchRange =
        if searchRange.IsSingleValue() |> not then failwith "Not supported"
        let searchKey = searchRange.ToSearchKey()
        let bucket = searchKey.GetHashCode() % bucketsCount
        let tblName = indexName + bucket.ToString()

        let ti =
            keyTypeToSchema keyType
            |> TableInfo.newTableInfo tblName

        let tf = newTableFile fileService tx false ti
        if tf.FileSize() = 0L then
            TableInfo.tableFileName ti
            |> TableFile.formatFileHeader fileService tx

        tf.BeforeFirst()
        { SearchKey = searchKey
          TableFile = tf }

    let next keyType state =
        let rec searchNext keyType tableFile searchKey =
            if tableFile.Next() then
                if getKey tableFile keyType
                   |> SearchKey.compare searchKey = 0 then
                    true
                else
                    searchNext keyType tableFile searchKey
            else
                false

        match state with
        | Some { SearchKey = searchKey; TableFile = tf } -> searchNext keyType tf searchKey
        | _ -> false

    let getDataRecordId tableFileName state =
        match state with
        | Some { TableFile = tf } ->
            let blockNo =
                tf.GetVal FieldBlockNo |> DbConstant.toLong

            let slotNo =
                tf.GetVal FieldSlotNo |> DbConstant.toInt

            RecordId.newBlockRecordId slotNo tableFileName blockNo
        | _ -> failwith "Must call beforeFirst()"

    let insert tx indexName tableFile doLogicalLogging key (RecordId (slotNo, BlockId (_, blockNo))) =
        if doLogicalLogging then tx.Recovery.LogLogicalStart() |> ignore

        tableFile.Insert()

        BigIntDbConstant blockNo
        |> tableFile.SetVal FieldBlockNo

        IntDbConstant slotNo
        |> tableFile.SetVal FieldSlotNo

        let (SearchKey keys) = key
        keys
        |> List.iteri (fun i t -> tableFile.SetVal (KeyPrefix + i.ToString()) t)

        if doLogicalLogging then
            tx.Recovery.LogIndexInsertionEnd indexName key blockNo slotNo
            |> ignore

    let delete tx indexName tableFile doLogicalLogging key (RecordId (slotNo, BlockId (_, blockNo))) =
        let rec searchDelete tableFile blockNo slotNo =
            if tableFile.Next() then
                let tfBlockNo =
                    tableFile.GetVal FieldBlockNo |> DbConstant.toLong

                let tfSlotNo =
                    tableFile.GetVal FieldSlotNo |> DbConstant.toInt

                if tfBlockNo = blockNo && tfSlotNo = slotNo then
                    tableFile.Delete()
                else
                    searchDelete tableFile blockNo slotNo

        if doLogicalLogging then tx.Recovery.LogLogicalStart() |> ignore

        searchDelete tableFile blockNo slotNo

        if doLogicalLogging then
            tx.Recovery.LogIndexDeletionEnd indexName key blockNo slotNo
            |> ignore

    let close state =
        state
        |> Option.iter (fun s -> s.TableFile.Close())

let newHashIndex fileService tx indexInfo keyType bucketsCount =
    let indexName = IndexInfo.indexName indexInfo
    let tableFileName = IndexInfo.tableFileName indexInfo
    let mutable state = None

    { BeforeFirst =
          fun searchRange ->
              HashIndex.close state
              state <-
                  HashIndex.beforeFirst fileService tx indexName keyType bucketsCount searchRange
                  |> Some
      Next = fun () -> HashIndex.next keyType state
      GetDataRecordId = fun () -> HashIndex.getDataRecordId tableFileName state
      Insert =
          fun doLogicalLogging key dataRecordId ->
              HashIndex.close state
              state <- None

              let st =
                  SearchRange.newSearchRangeBySearchKey key
                  |> HashIndex.beforeFirst fileService tx indexName keyType bucketsCount

              HashIndex.insert tx indexName st.TableFile doLogicalLogging key dataRecordId
      Delete =
          fun doLogicalLogging key dataRecordId ->
              HashIndex.close state
              state <- None

              let st =
                  SearchRange.newSearchRangeBySearchKey key
                  |> HashIndex.beforeFirst fileService tx indexName keyType bucketsCount

              HashIndex.delete tx indexName st.TableFile doLogicalLogging key dataRecordId
      Close =
          fun () ->
              HashIndex.close state
              state <- None
      LoadToBuffer = fun () -> HashIndex.loadToBuffer fileService tx indexName bucketsCount }
