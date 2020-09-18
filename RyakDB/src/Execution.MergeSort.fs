module RyakDB.Execution.MergeSort

open RyakDB.DataType
open RyakDB.Storage.File
open RyakDB.Table
open RyakDB.Table.SlottedPage
open RyakDB.Table.TableFile
open RyakDB.Transaction
open RyakDB.Query
open RyakDB.Execution.Scan

module BufferNeeds =
    let bestRoot tx size =
        let rec loopRoot avail k i =
            if k > avail then
                loopRoot
                    avail
                    ((double size, 1.0 / double i)
                     |> System.Math.Pow
                     |> System.Math.Ceiling
                     |> int32)
                    (i + 1)
            else
                k

        let avail = tx.Buffer.Available()
        if avail <= 1 then 1 else loopRoot avail System.Int32.MaxValue 1

type TempTable = { OpenScan: unit -> Scan }

module TempTable =
    let mutable nextTableNo = 0L

    let inline nextTableName () =
        FileManager.TmpFilePrefix
        + System.Threading.Interlocked.Increment(&nextTableNo).ToString()

    let inline openScan fileMgr tx tableInfo = Scan.newTableScan fileMgr tx tableInfo

let newTempTable fileMgr tx schema =
    let ti =
        TableInfo.newTableInfo (TempTable.nextTableName ()) schema

    tx.Buffer.PinNew ti.FileName FileHeaderFormatter.format
    |> tx.Buffer.Unpin

    { OpenScan = fun () -> TempTable.openScan fileMgr tx ti }

module TempSlottedPage =
    type TempSlottedPage =
        { SlottedPage: SlottedPage
          Schema: Schema }

    let moveToPageHead tsp = tsp.SlottedPage.MoveToSlotNo -1

    let insertFromScan scan tsp =
        if tsp.SlottedPage.InsertIntoNextEmptySlot() then
            tsp.Schema.Fields()
            |> List.iter (fun fn -> scan.GetVal fn |> tsp.SlottedPage.SetVal fn)
            if scan.Next() then 1 else -1
        else
            0

    let copyToScan scan tsp =
        if tsp.SlottedPage.Next() then
            scan.Insert()
            tsp.Schema.Fields()
            |> List.iter (fun fn -> tsp.SlottedPage.GetVal fn |> (scan.SetVal fn))
            true
        else
            false

    let compareRecords sortFields id1 id2 tsp =
        sortFields
        |> List.tryPick (fun (SortField (sortFld, sortDir)) ->
            tsp.SlottedPage.MoveToSlotNo id1
            let val1 = tsp.SlottedPage.GetVal sortFld
            tsp.SlottedPage.MoveToSlotNo id2
            let val2 = tsp.SlottedPage.GetVal sortFld
            if val1 < val2
            then if sortDir = SortDesc then Some 1 else Some -1
            elif val1 > val2
            then if sortDir = SortDesc then Some -1 else Some 1
            else None)
        |> Option.defaultValue 0

    let findSmallestFrom sortFields startId tsp =
        let rec findMin minId =
            if tsp.SlottedPage.Next() then
                tsp.SlottedPage.CurrentSlotNo()
                |> fun id ->
                    if minId < 0
                       || compareRecords sortFields minId id tsp > 0 then
                        id
                    else
                        minId
                    |> findMin
            else
                minId

        findMin startId

    let swapRecords id1 id2 tsp =
        tsp.Schema.Fields()
        |> List.iter (fun fn ->
            tsp.SlottedPage.MoveToSlotNo id1
            let val1 = tsp.SlottedPage.GetVal fn
            tsp.SlottedPage.MoveToSlotNo id2
            let val2 = tsp.SlottedPage.GetVal fn
            tsp.SlottedPage.SetVal fn val1
            tsp.SlottedPage.MoveToSlotNo id1
            tsp.SlottedPage.SetVal fn val2)

    let sortBySelection sortFields tsp =
        let rec loopSelect i tsp =
            if tsp.SlottedPage.Next() then
                let minId = findSmallestFrom sortFields i tsp
                if minId <> i then swapRecords i minId tsp
                tsp.SlottedPage.MoveToSlotNo i
                loopSelect (i + 1) tsp

        moveToPageHead tsp
        loopSelect 0 tsp

    let close tsp = tsp.SlottedPage.Close()

    let newTempSlottedPage tx blk ti =
        { SlottedPage = newSlottedPage tx.Buffer tx.Concurrency tx.Recovery blk ti false
          Schema = ti.Schema }

module MergeSort =
    let newTempSlottedPage tx schema tblcount =
        let ti =
            TableInfo.newTableInfo
                (FileManager.TmpFilePrefix
                 + "TableFile"
                 + "-"
                 + tblcount.ToString()
                 + "-"
                 + tx.TransactionNo.ToString())
                schema

        let tsp =
            newSlottedPageFormatter ti
            |> tx.Buffer.PinNew ti.TableName
            |> fun buff -> TempSlottedPage.newTempSlottedPage tx (buff.BlockId()) ti

        tsp |> TempSlottedPage.moveToPageHead
        tsp

    let rec loopInsertFromScan scan tsp =
        let f =
            tsp |> TempSlottedPage.insertFromScan scan

        if f > 0 then loopInsertFromScan scan tsp else f

    let rec loopCopyToScan scan tsp =
        if tsp |> TempSlottedPage.copyToScan scan
        then loopCopyToScan scan tsp

    let rec loopTempSlottedPage fileMgr tx schema src temps scan tblcount sortFields tsp =
        let flag = tsp |> loopInsertFromScan src
        tsp |> TempSlottedPage.sortBySelection sortFields
        tsp |> TempSlottedPage.moveToPageHead
        tsp |> loopCopyToScan scan
        tsp |> TempSlottedPage.close
        scan.Close()
        if flag <> -1 then
            let temp = newTempTable fileMgr tx schema
            let nextTblcount = tblcount + 1
            newTempSlottedPage tx schema nextTblcount
            |> loopTempSlottedPage fileMgr tx schema src (temp :: temps) (temp.OpenScan()) nextTblcount sortFields
        else
            List.rev temps

    let splitIntoRuns fileMgr tx schema sortFields src =
        src.BeforeFirst()
        if src.Next() then
            let temp = newTempTable fileMgr tx schema
            newTempSlottedPage tx schema 0
            |> loopTempSlottedPage fileMgr tx schema src [ temp ] (temp.OpenScan()) 0 sortFields
        else
            []

    let copy schema src dest =
        dest.Insert()
        schema.Fields()
        |> List.iter (fun fn -> src.GetVal fn |> dest.SetVal fn)
        src.Next()

    let rec loopMerge schema comparator dest count srcs hasMores =
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

        let result = newTempTable fileMgr tx schema
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

    let newRecordComparator sortFields =
        fun (record1: Record) (record2: Record) ->
            sortFields
            |> List.tryPick (fun (SortField (sortFld, sortDir)) ->
                let comp =
                    DbConstant.compare (record1 sortFld) (record2 sortFld)

                if comp < 0
                then if sortDir = SortDesc then Some 1 else Some -1
                elif comp > 0
                then if sortDir = SortDesc then Some -1 else Some 1
                else None)
            |> Option.defaultValue 0

    let newSortScan fileMgr tx =
        fun schema sortFields scan ->
            let mutable runs =
                splitIntoRuns fileMgr tx schema sortFields scan

            if runs.IsEmpty then
                scan
            else
                scan.Close()

                let comparator = newRecordComparator sortFields

                while runs.Length > 2 do
                    runs <- mergeRuns fileMgr tx schema comparator runs

                let scan1 = runs.Head.OpenScan()

                let scan2 =
                    runs
                    |> List.tail
                    |> List.tryHead
                    |> Option.map (fun s -> s.OpenScan())

                Scan.newSortScan scan1 scan2 comparator