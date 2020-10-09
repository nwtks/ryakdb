module RyakDB.Index.HashIndex

open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Table
open RyakDB.Index
open RyakDB.Storage.File
open RyakDB.Buffer.TransactionBuffer
open RyakDB.Concurrency.TransactionConcurrency
open RyakDB.Recovery.TransactionRecovery
open RyakDB.Table.TableFile

module HashIndex =
    type HashIndexState =
        { SearchKey: SearchKey
          TableFile: TableFile }

    let FieldBlockNo = "block_no"
    let FieldSlotNo = "slot_no"
    let KeyPrefix = "key"

    let fileSize (fileService: FileService) txConcurrency fileName =
        txConcurrency.ReadFile fileName
        fileService.Size fileName

    let keyTypeToSchema (SearchKeyType types) =
        let sch = Schema.newSchema ()
        sch.AddField FieldBlockNo BigIntDbType
        sch.AddField FieldSlotNo IntDbType
        types
        |> List.iteri (fun i t -> sch.AddField (KeyPrefix + i.ToString()) t)
        sch

    let getKey (tableFile: TableFile) (SearchKeyType types) =
        types
        |> List.mapi (fun i _ -> tableFile.GetVal(KeyPrefix + i.ToString()))
        |> SearchKey.newSearchKey

    let preLoadToMemory fileService txBuffer txConcurrency indexInfo bucketsCount =
        [ 0 .. bucketsCount - 1 ]
        |> List.map (fun i -> indexInfo.IndexName + i.ToString() + ".tbl")
        |> List.iter (fun tblName ->
            [ 0L .. (fileSize fileService txConcurrency tblName) - 1L ]
            |> List.iter (fun j ->
                BlockId.newBlockId tblName j
                |> txBuffer.Pin
                |> ignore))

    let beforeFirst fileService txBuffer txConcurrency txRecovery txReadOnly indexInfo keyType bucketsCount searchRange =
        if not (searchRange.IsSingleValue()) then failwith "Not supported"
        let searchKey = searchRange.ToSearchKey()
        let bucket = searchKey.GetHashCode() % bucketsCount
        let tblName = indexInfo.IndexName + bucket.ToString()

        let ti =
            keyTypeToSchema keyType
            |> TableInfo.newTableInfo tblName

        let tf =
            newTableFile fileService txBuffer txConcurrency txRecovery txReadOnly false ti

        if tf.FileSize() = 0L
        then TableFile.formatFileHeader fileService txBuffer txConcurrency ti.FileName

        tf.BeforeFirst()
        { SearchKey = searchKey
          TableFile = tf }

    let next keyType state =
        let rec searchNext keyType (tableFile: TableFile) searchKey =
            if tableFile.Next() then
                if SearchKey.compare searchKey (getKey tableFile keyType) = 0
                then true
                else searchNext keyType tableFile searchKey
            else
                false

        match state with
        | Some { SearchKey = searchKey; TableFile = tf } -> searchNext keyType tf searchKey
        | _ -> false

    let getDataRecordId indexInfo state =
        match state with
        | Some { TableFile = tf } ->
            let blockNo =
                tf.GetVal FieldBlockNo |> DbConstant.toLong

            let slotNo =
                tf.GetVal FieldSlotNo |> DbConstant.toInt

            RecordId.newBlockRecordId slotNo indexInfo.TableInfo.FileName blockNo
        | _ -> failwith "Must call beforeFirst()"

    let insert txRecovery
               indexInfo
               (tableFile: TableFile)
               doLogicalLogging
               key
               (RecordId (slotNo, BlockId (_, blockNo)))
               =
        if doLogicalLogging then txRecovery.LogLogicalStart() |> ignore

        tableFile.Insert()

        BigIntDbConstant blockNo
        |> tableFile.SetVal FieldBlockNo

        IntDbConstant slotNo
        |> tableFile.SetVal FieldSlotNo

        let (SearchKey keys) = key
        keys
        |> List.iteri (fun i t -> tableFile.SetVal (KeyPrefix + i.ToString()) t)

        if doLogicalLogging then
            txRecovery.LogIndexInsertionEnd indexInfo.IndexName key blockNo slotNo
            |> ignore

    let delete txRecovery indexInfo tableFile doLogicalLogging key (RecordId (slotNo, BlockId (_, blockNo))) =
        let rec searchDelete (tableFile: TableFile) blockNo slotNo =
            if tableFile.Next() then
                let tfBlockNo =
                    tableFile.GetVal FieldBlockNo |> DbConstant.toLong

                let tfSlotNo =
                    tableFile.GetVal FieldSlotNo |> DbConstant.toInt

                if tfBlockNo = blockNo && tfSlotNo = slotNo then
                    tableFile.Delete()
                else
                    searchDelete tableFile blockNo slotNo

        if doLogicalLogging then txRecovery.LogLogicalStart() |> ignore

        searchDelete tableFile blockNo slotNo

        if doLogicalLogging then
            txRecovery.LogIndexDeletionEnd indexInfo.IndexName key blockNo slotNo
            |> ignore

    let close state =
        state
        |> Option.iter (fun s -> s.TableFile.Close())

let newHashIndex fileService txBuffer txConcurrency txRecovery txReadOnly indexInfo keyType bucketsCount =
    let mutable state = None
    { BeforeFirst =
          fun searchRange ->
              HashIndex.close state
              state <-
                  HashIndex.beforeFirst
                      fileService
                      txBuffer
                      txConcurrency
                      txRecovery
                      txReadOnly
                      indexInfo
                      keyType
                      bucketsCount
                      searchRange
                  |> Some
      Next = fun () -> HashIndex.next keyType state
      GetDataRecordId = fun () -> HashIndex.getDataRecordId indexInfo state
      Insert =
          fun doLogicalLogging key dataRecordId ->
              HashIndex.close state
              state <- None

              let st =
                  SearchRange.newSearchRangeBySearchKey key
                  |> HashIndex.beforeFirst
                      fileService
                         txBuffer
                         txConcurrency
                         txRecovery
                         txReadOnly
                         indexInfo
                         keyType
                         bucketsCount

              HashIndex.insert txRecovery indexInfo st.TableFile doLogicalLogging key dataRecordId
      Delete =
          fun doLogicalLogging key dataRecordId ->
              HashIndex.close state
              state <- None

              let st =
                  SearchRange.newSearchRangeBySearchKey key
                  |> HashIndex.beforeFirst
                      fileService
                         txBuffer
                         txConcurrency
                         txRecovery
                         txReadOnly
                         indexInfo
                         keyType
                         bucketsCount

              HashIndex.delete txRecovery indexInfo st.TableFile doLogicalLogging key dataRecordId
      Close =
          fun () ->
              HashIndex.close state
              state <- None
      PreLoadToMemory = fun () -> HashIndex.preLoadToMemory fileService txBuffer txConcurrency indexInfo bucketsCount }
