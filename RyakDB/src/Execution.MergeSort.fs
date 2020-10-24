module RyakDB.Execution.MergeSort

open RyakDB.DataType
open RyakDB.Table
open RyakDB.Query
open RyakDB.Storage.File
open RyakDB.Buffer.BufferPool
open RyakDB.Table.TablePage
open RyakDB.Table.TableFile
open RyakDB.Transaction
open RyakDB.Execution.Scan

module BufferNeeds =
    let bestRoot bufferPool size =
        let rec searchRoot avail k i =
            if k > avail then
                searchRoot
                    avail
                    ((double size, 1.0 / double i)
                     |> System.Math.Pow
                     |> System.Math.Ceiling
                     |> int32)
                    (i + 1)
            else
                k

        let avail = bufferPool.Available()
        if avail <= 1 then 1 else searchRoot avail System.Int32.MaxValue 1

type TempTable = { OpenScan: unit -> Scan }

module TempTable =
    let mutable nextTableNo = 0L

    let nextTableName () =
        FileService.TmpFilePrefix
        + System.Threading.Interlocked.Increment(&nextTableNo).ToString()

    let openScan fileService tx tableInfo =
        Scan.newTableScan fileService tx tableInfo

let newTempTable fileService tx schema =
    let ti =
        TableInfo.newTableInfo (TempTable.nextTableName ()) schema

    tx.Buffer.PinNew (TableInfo.tableFileName ti) FileHeaderFormatter.format
    |> tx.Buffer.Unpin

    { OpenScan = fun () -> TempTable.openScan fileService tx ti }

module TempTablePage =
    type TempTablePage =
        { TablePage: TablePage
          Schema: Schema }

    let moveToPageHead ttp = ttp.TablePage.MoveToSlotNo -1

    let insertFromScan scan ttp =
        if ttp.TablePage.InsertIntoNextEmptySlot() then
            ttp.Schema.Fields()
            |> List.iter (fun fn -> scan.GetVal fn |> ttp.TablePage.SetVal fn)
            if scan.Next() then 1 else -1
        else
            0

    let copyToScan scan ttp =
        if ttp.TablePage.Next() then
            scan.Insert()
            ttp.Schema.Fields()
            |> List.iter (fun fn -> ttp.TablePage.GetVal fn |> (scan.SetVal fn))
            true
        else
            false

    let compareRecords sortFields slotNo1 slotNo2 ttp =
        sortFields
        |> List.tryPick (fun (SortField (sortFld, sortDir)) ->
            ttp.TablePage.MoveToSlotNo slotNo1
            let val1 = ttp.TablePage.GetVal sortFld
            ttp.TablePage.MoveToSlotNo slotNo2
            let val2 = ttp.TablePage.GetVal sortFld
            if val1 < val2
            then if sortDir = SortDesc then Some 1 else Some -1
            elif val1 > val2
            then if sortDir = SortDesc then Some -1 else Some 1
            else None)
        |> Option.defaultValue 0

    let findSmallestFrom sortFields startSlotNo ttp =
        let rec findMin minSlotNo =
            if ttp.TablePage.Next() then
                ttp.TablePage.CurrentSlotNo()
                |> fun slotNo ->
                    if minSlotNo < 0
                       || compareRecords sortFields minSlotNo slotNo ttp > 0 then
                        slotNo
                    else
                        minSlotNo
                    |> findMin
            else
                minSlotNo

        findMin startSlotNo

    let swapRecords slotNo1 slotNo2 ttp =
        ttp.Schema.Fields()
        |> List.iter (fun fn ->
            ttp.TablePage.MoveToSlotNo slotNo1
            let val1 = ttp.TablePage.GetVal fn
            ttp.TablePage.MoveToSlotNo slotNo2
            let val2 = ttp.TablePage.GetVal fn
            ttp.TablePage.SetVal fn val1
            ttp.TablePage.MoveToSlotNo slotNo1
            ttp.TablePage.SetVal fn val2)

    let sortBySelection sortFields ttp =
        let rec loopSelect i ttp =
            if ttp.TablePage.Next() then
                let minId = findSmallestFrom sortFields i ttp
                if minId <> i then swapRecords i minId ttp
                ttp.TablePage.MoveToSlotNo i
                loopSelect (i + 1) ttp

        moveToPageHead ttp
        loopSelect 0 ttp

    let close ttp = ttp.TablePage.Close()

    let newTempTablePage tx blockId schema =
        { TablePage = newTablePage tx.Buffer tx.Concurrency tx.Recovery blockId schema false
          Schema = schema }

module MergeSort =
    let newTempTablePage tx schema tblcount =
        let tableName =
            FileService.TmpFilePrefix
            + "TableFile"
            + "-"
            + tblcount.ToString()
            + "-"
            + tx.TransactionNo.ToString()

        let ttp =
            newTablePageFormatter schema
            |> tx.Buffer.PinNew tableName
            |> fun buff -> TempTablePage.newTempTablePage tx (buff.BlockId()) schema

        ttp |> TempTablePage.moveToPageHead
        ttp

    let rec loopInsertFromScan scan ttp =
        let f = ttp |> TempTablePage.insertFromScan scan

        if f > 0 then loopInsertFromScan scan ttp else f

    let rec loopCopyToScan scan ttp =
        if ttp |> TempTablePage.copyToScan scan then loopCopyToScan scan ttp

    let rec loopTempTablePage fileService tx schema src temps scan tblcount sortFields ttp =
        let flag = ttp |> loopInsertFromScan src
        ttp |> TempTablePage.sortBySelection sortFields
        ttp |> TempTablePage.moveToPageHead
        ttp |> loopCopyToScan scan
        ttp |> TempTablePage.close
        scan.Close()
        if flag <> -1 then
            let temp = newTempTable fileService tx schema
            let nextTblcount = tblcount + 1
            newTempTablePage tx schema nextTblcount
            |> loopTempTablePage fileService tx schema src (temp :: temps) (temp.OpenScan()) nextTblcount sortFields
        else
            List.rev temps

    let splitIntoRuns fileService tx schema sortFields src =
        src.BeforeFirst()
        if src.Next() then
            let temp = newTempTable fileService tx schema
            newTempTablePage tx schema 0
            |> loopTempTablePage fileService tx schema src [ temp ] (temp.OpenScan()) 0 sortFields
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

    let mergeTemps fileService tx schema comparator temps =
        let srcs =
            temps
            |> List.map (fun tmp ->
                let s = tmp.OpenScan()
                s.BeforeFirst()
                s)

        let result = newTempTable fileService tx schema
        use dest = result.OpenScan()
        let hasMores = srcs |> List.map (fun s -> s.Next())

        let count =
            hasMores |> List.filter id |> List.length

        loopMerge schema comparator dest count srcs hasMores
        srcs |> List.iter (fun s -> s.Close())
        result

    let rec loopMergeRuns fileService tx schema comparator numofbuf temps results =
        if List.length temps > numofbuf then
            (mergeTemps fileService tx schema comparator temps.[..(numofbuf - 1)])
            :: results
            |> loopMergeRuns fileService tx schema comparator numofbuf temps.[numofbuf..]
        else
            temps, results

    let mergeRuns fileService bufferPool tx schema comparator runs =
        let numofbuf =
            List.length runs
            |> BufferNeeds.bestRoot bufferPool

        let temps, results =
            loopMergeRuns fileService tx schema comparator numofbuf runs []

        if List.length temps > 1 then
            (mergeTemps fileService tx schema comparator temps)
            :: results
        elif List.length temps = 1 then
            List.head temps :: results
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

    let newSortScan fileService bufferPool tx =
        fun schema sortFields scan ->
            let mutable runs =
                splitIntoRuns fileService tx schema sortFields scan

            if List.isEmpty runs then
                scan
            else
                scan.Close()

                let comparator = newRecordComparator sortFields

                while List.length runs > 2 do
                    runs <- mergeRuns fileService bufferPool tx schema comparator runs

                let scan1 = runs.Head.OpenScan()

                let scan2 =
                    runs
                    |> List.tail
                    |> List.tryHead
                    |> Option.map (fun s -> s.OpenScan())

                Scan.newSortScan scan1 scan2 comparator
