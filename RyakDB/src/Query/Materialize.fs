namespace RyakDB.Query.Materialize

open RyakDB.Sql.Type
open RyakDB.Storage.Type
open RyakDB.Storage.Record
open RyakDB.Storage.Catalog
open RyakDB.Query.Algebra

module RecordComparator =
    let newRecordComparator sortFields =
        fun (record1: Record) (record2: Record) ->
            sortFields
            |> List.tryPick (fun (SortField (sortFld, sortDir)) ->
                let comp =
                    SqlConstant.compare (record1 sortFld) (record2 sortFld)

                if comp < 0
                then if sortDir = SortDesc then Some 1 else Some -1
                elif comp > 0
                then if sortDir = SortDesc then Some -1 else Some 1
                else None)
            |> Option.defaultValue 0

module BufferNeeds =
    let bestRoot tx size =
        let rec loopRoot avail k i =
            if k > avail then
                loopRoot avail
                    ((double size, 1.0 / double i)
                     |> System.Math.Pow
                     |> System.Math.Ceiling
                     |> int32) (i + 1)
            else
                k

        let avail = tx.BufferMgr.Available()
        if avail <= 1 then 1 else loopRoot avail System.Int32.MaxValue 1

type TempTable = { OpenScan: unit -> Scan }

module TempTable =
    let mutable nextTableNo = 0L

    let nextTableName () =
        "_temp"
        + System.Threading.Interlocked.Increment(&nextTableNo).ToString()

    let openScan tx tableInfo = Scan.newTableScan tx tableInfo

    let newTempTable fileMgr tx schema =
        let ti =
            TableInfo.newTableInfo fileMgr (nextTableName ()) schema

        tx.BufferMgr.PinNew ti.FileName FileHeaderFormatter.format
        |> tx.BufferMgr.Unpin

        { OpenScan = fun () -> openScan tx ti }

module TempRecordPage =
    type TempRecordPage =
        { RecordPage: RecordPage
          Schema: Schema }

    let moveToPageHead trp = trp.RecordPage.MoveToId -1

    let insertFromScan scan trp =
        if trp.RecordPage.InsertIntoNextEmptySlot() then
            trp.Schema.Fields()
            |> List.iter (fun fn -> scan.GetVal fn |> trp.RecordPage.SetVal fn)
            if scan.Next() then 1 else -1
        else
            0

    let copyToScan scan trp =
        if trp.RecordPage.Next() then
            scan.Insert()
            trp.Schema.Fields()
            |> List.iter (fun fn ->
                trp.RecordPage.GetVal fn
                |> Option.iter (scan.SetVal fn))
            true
        else
            false

    let compareRecords sortFields id1 id2 trp =
        sortFields
        |> List.tryPick (fun (SortField (sortFld, sortDir)) ->
            trp.RecordPage.MoveToId id1
            let val1 = trp.RecordPage.GetVal sortFld
            trp.RecordPage.MoveToId id2
            let val2 = trp.RecordPage.GetVal sortFld
            match val1, val2 with
            | Some v1, Some v2 ->
                if v1 < v2
                then if sortDir = SortDesc then Some 1 else Some -1
                elif v1 > v2
                then if sortDir = SortDesc then Some -1 else Some 1
                else None
            | _ -> None)
        |> Option.defaultValue 0

    let findSmallestFrom sortFields startId trp =
        let rec findMin minId =
            if trp.RecordPage.Next() then
                trp.RecordPage.CurrentSlot()
                |> fun id ->
                    if minId < 0
                       || compareRecords sortFields minId id trp > 0 then
                        id
                    else
                        minId
                    |> findMin
            else
                minId

        findMin startId

    let swapRecords id1 id2 trp =
        trp.Schema.Fields()
        |> List.iter (fun fn ->
            trp.RecordPage.MoveToId id1
            let val1 = trp.RecordPage.GetVal fn
            trp.RecordPage.MoveToId id2
            let val2 = trp.RecordPage.GetVal fn
            match val1, val2 with
            | Some v1, Some v2 ->
                trp.RecordPage.SetVal fn v1
                trp.RecordPage.MoveToId id1
                trp.RecordPage.SetVal fn v2
            | _ -> ())

    let sortBySelection sortFields trp =
        let rec loopSelect i trp =
            if trp.RecordPage.Next() then
                let minId = findSmallestFrom sortFields i trp
                if minId <> i then swapRecords i minId trp
                trp.RecordPage.MoveToId i
                loopSelect (i + 1) trp

        moveToPageHead trp
        loopSelect 0 trp

    let close trp = trp.RecordPage.Close()

    let newTempRecordPage tx blk ti =
        { RecordPage = RecordPage.newRecordPage tx blk ti false
          Schema = ti.Schema }

module Materialize =
    let newTempRecordPage fileMgr tx schema tblcount =
        let ti =
            TableInfo.newTableInfo fileMgr
                ("_tempRecordFile"
                 + "-"
                 + tblcount.ToString()
                 + "-"
                 + tx.TransactionNumber.ToString()) schema

        let trp =
            RecordFormatter.newFormatter ti
            |> tx.BufferMgr.PinNew ti.TableName
            |> fun buff -> TempRecordPage.newTempRecordPage tx (buff.BlockId()) ti

        trp |> TempRecordPage.moveToPageHead
        trp

    let rec loopInsertFromScan scan trp =
        let f =
            trp |> TempRecordPage.insertFromScan scan

        if f > 0 then loopInsertFromScan scan trp else f

    let rec loopCopyToScan scan trp =
        if trp |> TempRecordPage.copyToScan scan then loopCopyToScan scan trp

    let rec loopTempRecordPage fileMgr tx schema src temps scan tblcount sortFields trp =
        let flag = trp |> loopInsertFromScan src
        trp |> TempRecordPage.sortBySelection sortFields
        trp |> TempRecordPage.moveToPageHead
        trp |> loopCopyToScan scan
        trp |> TempRecordPage.close
        scan.Close()
        if flag <> -1 then
            let temp = TempTable.newTempTable fileMgr tx schema
            let nextTblcount = tblcount + 1
            newTempRecordPage fileMgr tx schema nextTblcount
            |> loopTempRecordPage fileMgr tx schema src (temp :: temps) (temp.OpenScan()) nextTblcount sortFields
        else
            List.rev temps

    let splitIntoRuns fileMgr tx schema sortFields src =
        src.BeforeFirst()
        if src.Next() then
            let temp = TempTable.newTempTable fileMgr tx schema
            newTempRecordPage fileMgr tx schema 0
            |> loopTempRecordPage fileMgr tx schema src [ temp ] (temp.OpenScan()) 0 sortFields
        else
            []

    let copy schema src dest =
        dest.Insert()
        schema.Fields()
        |> List.iter (fun fn -> src.GetVal fn |> dest.SetVal fn)
        src.Next()

    let rec loopMerge schema comparator dest count (srcs: Scan list) (hasMores: bool list) =
        if count > 0 then
            let mutable target = -1
            List.zip hasMores srcs
            |> List.iteri (fun i (has, src) ->
                if has
                   && (target < 0
                       || comparator src.GetVal srcs.[target].GetVal < 0) then
                    target <- i)
            let hasMore = copy schema srcs.[target] dest
            hasMores
            |> List.mapi (fun i v -> if i = target then hasMore else v)
            |> loopMerge schema comparator dest (if hasMore then count else count - 1) srcs

    let mergeTemps fileMgr tx schema comparator temps =
        let srcs =
            temps
            |> List.map (fun tmp ->
                let s = tmp.OpenScan()
                s.BeforeFirst()
                s)

        let result = TempTable.newTempTable fileMgr tx schema
        let dest = result.OpenScan()
        let hasMores = srcs |> List.map (fun s -> s.Next())
        let count = (hasMores |> List.filter id).Length
        loopMerge schema comparator dest count srcs hasMores
        dest.Close()
        srcs |> List.iter (fun s -> s.Close())
        result

    let rec loopMergeRuns fileMgr tx schema comparator numofbuf (temps: TempTable list) results =
        if temps.Length > numofbuf then
            (mergeTemps fileMgr tx schema comparator temps.[..(numofbuf - 1)])
            :: results
            |> loopMergeRuns fileMgr tx schema comparator numofbuf temps.[numofbuf..]
        else
            temps, results

    let mergeRuns fileMgr tx schema comparator runs =
        let numofbuf =
            BufferNeeds.bestRoot tx (List.length runs)

        let temps, results =
            loopMergeRuns fileMgr tx schema comparator numofbuf runs []

        if temps.Length > 1 then
            (mergeTemps fileMgr tx schema comparator temps)
            :: results
        elif temps.Length = 1 then
            temps.Head :: results
        else
            results
        |> List.rev

    let newSortScan fileMgr tx =
        fun schema sortFields scan ->
            let mutable runs =
                splitIntoRuns fileMgr tx schema sortFields scan

            if runs.IsEmpty then
                scan
            else
                scan.Close()

                let comparator =
                    RecordComparator.newRecordComparator sortFields

                while runs.Length > 2 do
                    runs <- mergeRuns fileMgr tx schema comparator runs

                let scan1 = runs.Head.OpenScan()

                let scan2 =
                    runs
                    |> List.tail
                    |> List.tryHead
                    |> Option.map (fun s -> s.OpenScan())

                Scan.newSortScan scan1 scan2 comparator
