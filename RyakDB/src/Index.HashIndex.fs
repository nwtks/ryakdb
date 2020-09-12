module RyakDB.Index.HashIndex

open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Table
open RyakDB.Storage.File
open RyakDB.Table.TableFile
open RyakDB.Index
open RyakDB.Concurrency.TransactionConcurrency
open RyakDB.Transaction

module HashIndex =
    type HashIndexState =
        { SearchKey: SearchKey
          TableFile: TableFile }

    let BucketsCount = 100

    let fileSize (fileMgr: FileManager) txConcurrency fileName =
        txConcurrency.ReadFile fileName
        fileMgr.Size fileName

    let keyTypeToSchema (SearchKeyType types) =
        let sch = Schema.newSchema ()
        sch.AddField "blockNo" BigIntDbType
        sch.AddField "slotNo" IntDbType
        types
        |> List.iteri (fun i t -> sch.AddField ("key" + i.ToString()) t)
        sch

    let getKey (tableFile: TableFile) (SearchKeyType types) =
        types
        |> List.mapi (fun i _ ->
            tableFile.GetVal("key" + i.ToString())
            |> Option.get)
        |> SearchKey.newSearchKey

    let preLoadToMemory fileMgr tx indexInfo =
        for i in 0 .. BucketsCount - 1 do
            let tblName =
                indexInfo.IndexName + i.ToString() + ".tbl"

            let size = fileSize fileMgr tx.Concurrency tblName
            for j in 0L .. size - 1L do
                BlockId.newBlockId tblName j
                |> tx.Buffer.Pin
                |> ignore

    let beforeFirst fileMgr tx indexInfo keyType searchRange =
        if not (searchRange.IsSingleValue()) then failwith "Not supported"
        let searchKey = searchRange.ToSearchKey()
        let bucket = searchKey.GetHashCode() % BucketsCount
        let tblName = indexInfo.IndexName + bucket.ToString()

        let ti =
            keyTypeToSchema keyType
            |> TableInfo.newTableInfo tblName

        let tf = newTableFile fileMgr tx false ti

        if tf.FileSize() = 0L
        then TableFile.formatFileHeader fileMgr tx ti.FileName
        tf.BeforeFirst()
        { SearchKey = searchKey
          TableFile = tf }

    let next keyType state =
        let rec loopNext keyType (tableFile: TableFile) searchKey =
            if not (tableFile.Next()) then
                false
            else
                let key = getKey tableFile keyType
                if searchKey = key then true else loopNext keyType tableFile searchKey

        match state with
        | Some ({ SearchKey = searchKey; TableFile = tf }) -> loopNext keyType tf searchKey
        | _ -> failwith "You must call beforeFirst()"

    let getDataRecordId indexInfo state =
        match state with
        | Some ({ TableFile = tf }) ->
            let blockNo =
                tf.GetVal "blockNo"
                |> Option.get
                |> DbConstant.toLong

            let slotNo =
                tf.GetVal "slotNo"
                |> Option.get
                |> DbConstant.toInt

            RecordId.newBlockRecordId slotNo (indexInfo.TableInfo.FileName) blockNo
        | _ -> failwith "You must call beforeFirst()"

    let insert tx indexInfo (tableFile: TableFile) doLogicalLogging key (RecordId (slotNo, BlockId (_, blockNo))) =
        if doLogicalLogging then tx.Recovery.LogLogicalStart() |> ignore
        tableFile.Insert()
        tableFile.SetVal "blockNo" (BigIntDbConstant blockNo)
        tableFile.SetVal "slotNo" (IntDbConstant slotNo)
        let (SearchKey keys) = key
        keys
        |> List.iteri (fun i t -> tableFile.SetVal ("key" + i.ToString()) t)
        if doLogicalLogging then
            tx.Recovery.LogIndexInsertionEnd indexInfo.IndexName key blockNo slotNo
            |> ignore

    let delete tx indexInfo tableFile doLogicalLogging key (RecordId (slotNo, BlockId (_, blockNo))) =
        let rec loopDelete (tableFile: TableFile) blockNo slotNo =
            if tableFile.Next() then
                let tfBlockNo =
                    tableFile.GetVal "blockNo"
                    |> Option.get
                    |> DbConstant.toLong

                let tfSlotNo =
                    tableFile.GetVal "slotNo"
                    |> Option.get
                    |> DbConstant.toInt

                if tfBlockNo = blockNo && tfSlotNo = slotNo then
                    tableFile.Delete()
                else
                    loopDelete tableFile blockNo slotNo

        if doLogicalLogging then tx.Recovery.LogLogicalStart() |> ignore
        loopDelete tableFile blockNo slotNo
        if doLogicalLogging then
            tx.Recovery.LogIndexDeletionEnd indexInfo.IndexName key blockNo slotNo
            |> ignore

    let inline close state =
        state
        |> Option.iter (fun s -> s.TableFile.Close())

let newHashIndex fileMgr tx indexInfo keyType =
    let mutable state = None
    { BeforeFirst =
          fun searchRange ->
              HashIndex.close state
              state <- Some(HashIndex.beforeFirst fileMgr tx indexInfo keyType searchRange)
      Next = fun () -> HashIndex.next keyType state
      GetDataRecordId = fun () -> HashIndex.getDataRecordId indexInfo state
      Insert =
          fun doLogicalLogging key dataRecordId ->
              HashIndex.close state
              state <- None

              let st =
                  HashIndex.beforeFirst fileMgr tx indexInfo keyType (SearchRange.newSearchRangeBySearchKey key)

              HashIndex.insert tx indexInfo st.TableFile doLogicalLogging key dataRecordId
      Delete =
          fun doLogicalLogging key dataRecordId ->
              HashIndex.close state
              state <- None

              let st =
                  HashIndex.beforeFirst fileMgr tx indexInfo keyType (SearchRange.newSearchRangeBySearchKey key)

              HashIndex.delete tx indexInfo st.TableFile doLogicalLogging key dataRecordId
      Close =
          fun () ->
              HashIndex.close state
              state <- None
      PreLoadToMemory = fun () -> HashIndex.preLoadToMemory fileMgr tx indexInfo }
