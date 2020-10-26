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

type TempTablePage =
    { TablePage: TablePage
      Schema: Schema }

module TempTablePage =
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
            |> List.iter (fun fn -> ttp.TablePage.GetVal fn |> scan.SetVal fn)
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
                let slotNo = ttp.TablePage.CurrentSlotNo()
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

        if f > 0 then ttp |> loopInsertFromScan scan else f

    let rec loopCopyToScan scan ttp =
        if ttp |> TempTablePage.copyToScan scan then ttp |> loopCopyToScan scan

    let rec loopTempTablePage fileService tx schema srcScan tempTables destScan tblcount sortFields ttp =
        let flag = ttp |> loopInsertFromScan srcScan
        ttp |> TempTablePage.sortBySelection sortFields
        ttp |> TempTablePage.moveToPageHead
        ttp |> loopCopyToScan destScan
        ttp |> TempTablePage.close
        destScan.Close()
        if flag <> -1 then
            let tt = newTempTable fileService tx schema
            let nextTblcount = tblcount + 1
            newTempTablePage tx schema nextTblcount
            |> loopTempTablePage
                fileService
                   tx
                   schema
                   srcScan
                   (tt :: tempTables)
                   (tt.OpenScan())
                   nextTblcount
                   sortFields
        else
            tempTables |> List.rev

    let splitIntoRuns fileService tx schema sortFields srcScan =
        srcScan.BeforeFirst()
        if srcScan.Next() then
            let tt = newTempTable fileService tx schema
            newTempTablePage tx schema 0
            |> loopTempTablePage fileService tx schema srcScan [ tt ] (tt.OpenScan()) 0 sortFields
        else
            []

    let copy schema srcScan destScan =
        destScan.Insert()
        schema.Fields()
        |> List.iter (fun fn -> srcScan.GetVal fn |> destScan.SetVal fn)
        srcScan.Next()

    let rec loopMerge schema comparator count destScan srcScans hasMores =
        if count > 0 then
            let mutable target = -1
            List.zip hasMores srcScans
            |> List.iteri (fun i (hasMore, srcScan) ->
                if hasMore
                   && (target < 0
                       || comparator srcScan.GetVal srcScans.[target].GetVal < 0) then
                    target <- i)
            let hasMore = copy schema srcScans.[target] destScan
            hasMores
            |> List.mapi (fun i v -> if i = target then hasMore else v)
            |> loopMerge schema comparator (if hasMore then count else count - 1) destScan srcScans

    let mergeTemps fileService tx schema comparator tempTables =
        let srcScans =
            tempTables
            |> List.map (fun tmp ->
                let s = tmp.OpenScan()
                s.BeforeFirst()
                s)

        let result = newTempTable fileService tx schema
        use destScan = result.OpenScan()
        let hasMores = srcScans |> List.map (fun s -> s.Next())

        let count =
            hasMores |> List.filter id |> List.length

        loopMerge schema comparator count destScan srcScans hasMores
        srcScans |> List.iter (fun s -> s.Close())
        result

    let rec loopMergeRuns fileService tx schema comparator numofbuf tempTables results =
        if tempTables |> List.length > numofbuf then
            (mergeTemps fileService tx schema comparator tempTables.[..(numofbuf - 1)])
            :: results
            |> loopMergeRuns fileService tx schema comparator numofbuf tempTables.[numofbuf..]
        else
            tempTables, results

    let mergeRuns fileService bufferPool tx schema comparator runs =
        let numofbuf =
            List.length runs
            |> BufferNeeds.bestRoot bufferPool

        let tempTables, results =
            loopMergeRuns fileService tx schema comparator numofbuf runs []

        if tempTables |> List.length > 1 then
            (tempTables
             |> mergeTemps fileService tx schema comparator)
            :: results
        elif tempTables |> List.length = 1 then
            List.head tempTables :: results
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
                scan
                |> splitIntoRuns fileService tx schema sortFields

            if runs |> List.isEmpty then
                scan
            else
                scan.Close()

                let comparator = newRecordComparator sortFields

                while runs |> List.length > 2 do
                    runs <-
                        runs
                        |> mergeRuns fileService bufferPool tx schema comparator

                let scan1 = runs.Head.OpenScan()

                let scan2 =
                    runs
                    |> List.tail
                    |> List.tryHead
                    |> Option.map (fun s -> s.OpenScan())

                Scan.newSortScan scan1 scan2 comparator
