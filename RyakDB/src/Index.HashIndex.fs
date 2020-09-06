namespace RyakDB.Index.HashIndex

open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Storage.File
open RyakDB.Table.Record
open RyakDB.Table
open RyakDB.Index
open RyakDB.Concurrency
open RyakDB.Transaction

module HashIndex =
    type HashIndexState =
        { SearchKey: SearchKey
          RecordFile: RecordFile }

    let BucketsCount = 100

    let fileSize (fileMgr: FileManager) concurMgr fileName =
        concurMgr.ReadFile fileName
        fileMgr.Size fileName

    let keyTypeToSchema (SearchKeyType types) =
        let sch = Schema.newSchema ()
        sch.AddField "block" BigIntSqlType
        sch.AddField "id" IntSqlType
        types
        |> List.iteri (fun i t -> sch.AddField ("key" + i.ToString()) t)
        sch

    let getKey (rf: RecordFile) (SearchKeyType types) =
        types
        |> List.mapi (fun i t -> rf.GetVal("key" + i.ToString()) |> Option.get)
        |> SearchKey.newSearchKey

    let preLoadToMemory fileMgr tx indexInfo =
        for i in 0 .. BucketsCount - 1 do
            let tblName =
                indexInfo.IndexName + i.ToString() + ".tbl"

            let size = fileSize fileMgr tx.ConcurMgr tblName
            for j in 0L .. size - 1L do
                BlockId.newBlockId tblName j
                |> tx.BufferMgr.Pin
                |> ignore

    let beforeFirst fileMgr tx indexInfo keyType searchRange =
        if not (searchRange.IsSingleValue()) then failwith "Not supported"
        let searchKey = searchRange.ToSearchKey()
        let bucket = searchKey.GetHashCode() % BucketsCount
        let tblName = indexInfo.IndexName + bucket.ToString()

        let ti =
            keyTypeToSchema keyType
            |> TableInfo.newTableInfo tblName

        let rf =
            RecordFile.newRecordFile fileMgr tx false ti

        if rf.FileSize() = 0L
        then RecordFile.formatFileHeader fileMgr tx ti.FileName
        rf.BeforeFirst()
        { SearchKey = searchKey
          RecordFile = rf }

    let next keyType state =
        let rec loopNext keyType (rf: RecordFile) searchKey =
            if not (rf.Next()) then
                false
            else
                let key = getKey rf keyType
                if searchKey = key then true else loopNext keyType rf searchKey

        match state with
        | Some ({ SearchKey = searchKey; RecordFile = rf }) -> loopNext keyType rf searchKey
        | _ -> failwith "You must call beforeFirst()"

    let getDataRecordId indexInfo state =
        match state with
        | Some ({ RecordFile = rf }) ->
            let blockNo =
                rf.GetVal "block"
                |> Option.get
                |> SqlConstant.toLong

            let rid =
                rf.GetVal "id" |> Option.get |> SqlConstant.toInt

            RecordId.newBlockRecordId rid (indexInfo.TableInfo.TableName + ".tbl") blockNo
        | _ -> failwith "You must call beforeFirst()"

    let insert tx indexInfo (rf: RecordFile) doLogicalLogging key (RecordId (rid, BlockId (_, blockNo))) =
        if doLogicalLogging
        then tx.RecoveryMgr.LogLogicalStart() |> ignore
        rf.Insert()
        rf.SetVal "block" (BigIntSqlConstant blockNo)
        rf.SetVal "id" (IntSqlConstant rid)
        let (SearchKey keys) = key
        keys
        |> List.iteri (fun i t -> rf.SetVal ("key" + i.ToString()) t)
        if doLogicalLogging then
            tx.RecoveryMgr.LogIndexInsertionEnd indexInfo.IndexName key blockNo rid
            |> ignore

    let delete tx indexInfo (rf: RecordFile) doLogicalLogging key (RecordId (rid, BlockId (_, blockNo))) =
        let rec loopDelete (rf: RecordFile) blockNo rid =
            if rf.Next() then
                let rfBlockNo =
                    rf.GetVal "block"
                    |> Option.get
                    |> SqlConstant.toLong

                let rfRid =
                    rf.GetVal "id" |> Option.get |> SqlConstant.toInt

                if rfBlockNo = blockNo && rfRid = rid then rf.Delete() else loopDelete rf blockNo rid

        if doLogicalLogging
        then tx.RecoveryMgr.LogLogicalStart() |> ignore
        loopDelete rf blockNo rid
        if doLogicalLogging then
            tx.RecoveryMgr.LogIndexDeletionEnd indexInfo.IndexName key blockNo rid
            |> ignore

    let close state =
        state
        |> Option.iter (fun s -> s.RecordFile.Close())

    let newHashIndex fileMgr tx indexInfo keyType =
        let mutable state = None
        { BeforeFirst =
              fun searchRange ->
                  close state
                  state <- Some(beforeFirst fileMgr tx indexInfo keyType searchRange)
          Next = fun () -> next keyType state
          GetDataRecordId = fun () -> getDataRecordId indexInfo state
          Insert =
              fun doLogicalLogging key dataRecordId ->
                  close state
                  state <- None

                  let { RecordFile = rf } =
                      beforeFirst fileMgr tx indexInfo keyType (SearchRange.newSearchRangeBySearchKey key)

                  insert tx indexInfo rf doLogicalLogging key dataRecordId
          Delete =
              fun doLogicalLogging key dataRecordId ->
                  close state
                  state <- None

                  let { RecordFile = rf } =
                      beforeFirst fileMgr tx indexInfo keyType (SearchRange.newSearchRangeBySearchKey key)

                  delete tx indexInfo rf doLogicalLogging key dataRecordId
          Close =
              fun () ->
                  close state
                  state <- None
          PreLoadToMemory = fun () -> preLoadToMemory fileMgr tx indexInfo }
