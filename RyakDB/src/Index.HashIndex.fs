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

    let BucketsCount = 100

    let fileSize (fileMgr: FileManager) txConcurrency fileName =
        txConcurrency.ReadFile fileName
        fileMgr.Size fileName

    let keyTypeToSchema (SearchKeyType types) =
        let sch = Schema.newSchema ()
        sch.AddField FieldBlockNo BigIntDbType
        sch.AddField FieldSlotNo IntDbType
        types
        |> List.iteri (fun i t -> sch.AddField (KeyPrefix + i.ToString()) t)
        sch

    let getKey (tableFile: TableFile) (SearchKeyType types) =
        types
        |> List.mapi (fun i _ ->
            tableFile.GetVal(KeyPrefix + i.ToString())
            |> Option.get)
        |> SearchKey.newSearchKey

    let preLoadToMemory fileMgr txBuffer txConcurrency indexInfo =
        for i in 0 .. BucketsCount - 1 do
            let tblName =
                indexInfo.IndexName + i.ToString() + ".tbl"

            let size = fileSize fileMgr txConcurrency tblName
            for j in 0L .. size - 1L do
                BlockId.newBlockId tblName j
                |> txBuffer.Pin
                |> ignore

    let beforeFirst fileMgr txBuffer txConcurrency txRecovery txReadOnly indexInfo keyType searchRange =
        if not (searchRange.IsSingleValue()) then failwith "Not supported"
        let searchKey = searchRange.ToSearchKey()
        let bucket = searchKey.GetHashCode() % BucketsCount
        let tblName = indexInfo.IndexName + bucket.ToString()

        let ti =
            keyTypeToSchema keyType
            |> TableInfo.newTableInfo tblName

        let tf =
            newTableFile fileMgr txBuffer txConcurrency txRecovery txReadOnly false ti

        if tf.FileSize() = 0L
        then TableFile.formatFileHeader fileMgr txBuffer txConcurrency ti.FileName
        tf.BeforeFirst()
        { SearchKey = searchKey
          TableFile = tf }

    let next keyType state =
        let rec loopNext keyType (tableFile: TableFile) searchKey =
            if tableFile.Next() then
                if SearchKey.compare searchKey (getKey tableFile keyType) = 0
                then true
                else loopNext keyType tableFile searchKey
            else
                false

        match state with
        | Some ({ SearchKey = searchKey; TableFile = tf }) -> loopNext keyType tf searchKey
        | _ -> failwith "Must call beforeFirst()"

    let getDataRecordId indexInfo state =
        match state with
        | Some ({ TableFile = tf }) ->
            let blockNo =
                tf.GetVal FieldBlockNo
                |> Option.get
                |> DbConstant.toLong

            let slotNo =
                tf.GetVal FieldSlotNo
                |> Option.get
                |> DbConstant.toInt

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
        tableFile.SetVal FieldBlockNo (BigIntDbConstant blockNo)
        tableFile.SetVal FieldSlotNo (IntDbConstant slotNo)
        let (SearchKey keys) = key
        keys
        |> List.iteri (fun i t -> tableFile.SetVal (KeyPrefix + i.ToString()) t)
        if doLogicalLogging then
            txRecovery.LogIndexInsertionEnd indexInfo.IndexName key blockNo slotNo
            |> ignore

    let delete txRecovery indexInfo tableFile doLogicalLogging key (RecordId (slotNo, BlockId (_, blockNo))) =
        let rec loopDelete (tableFile: TableFile) blockNo slotNo =
            if tableFile.Next() then
                let tfBlockNo =
                    tableFile.GetVal FieldBlockNo
                    |> Option.get
                    |> DbConstant.toLong

                let tfSlotNo =
                    tableFile.GetVal FieldSlotNo
                    |> Option.get
                    |> DbConstant.toInt

                if tfBlockNo = blockNo && tfSlotNo = slotNo then
                    tableFile.Delete()
                else
                    loopDelete tableFile blockNo slotNo

        if doLogicalLogging then txRecovery.LogLogicalStart() |> ignore
        loopDelete tableFile blockNo slotNo
        if doLogicalLogging then
            txRecovery.LogIndexDeletionEnd indexInfo.IndexName key blockNo slotNo
            |> ignore

    let close state =
        state
        |> Option.iter (fun s -> s.TableFile.Close())

let newHashIndex fileMgr txBuffer txConcurrency txRecovery txReadOnly indexInfo keyType =
    let mutable state = None
    { BeforeFirst =
          fun searchRange ->
              HashIndex.close state
              state <-
                  Some
                      (HashIndex.beforeFirst
                          fileMgr
                           txBuffer
                           txConcurrency
                           txRecovery
                           txReadOnly
                           indexInfo
                           keyType
                           searchRange)
      Next = fun () -> HashIndex.next keyType state
      GetDataRecordId = fun () -> HashIndex.getDataRecordId indexInfo state
      Insert =
          fun doLogicalLogging key dataRecordId ->
              HashIndex.close state
              state <- None

              let st =
                  HashIndex.beforeFirst
                      fileMgr
                      txBuffer
                      txConcurrency
                      txRecovery
                      txReadOnly
                      indexInfo
                      keyType
                      (SearchRange.newSearchRangeBySearchKey key)

              HashIndex.insert txRecovery indexInfo st.TableFile doLogicalLogging key dataRecordId
      Delete =
          fun doLogicalLogging key dataRecordId ->
              HashIndex.close state
              state <- None

              let st =
                  HashIndex.beforeFirst
                      fileMgr
                      txBuffer
                      txConcurrency
                      txRecovery
                      txReadOnly
                      indexInfo
                      keyType
                      (SearchRange.newSearchRangeBySearchKey key)

              HashIndex.delete txRecovery indexInfo st.TableFile doLogicalLogging key dataRecordId
      Close =
          fun () ->
              HashIndex.close state
              state <- None
      PreLoadToMemory = fun () -> HashIndex.preLoadToMemory fileMgr txBuffer txConcurrency indexInfo }
