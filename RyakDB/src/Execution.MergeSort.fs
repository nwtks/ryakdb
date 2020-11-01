module RyakDB.Execution.MergeSort

open RyakDB.DataType
open RyakDB.Table
open RyakDB.Query
open RyakDB.Storage.File
open RyakDB.Buffer.BufferPool
open RyakDB.Transaction
open RyakDB.Table.TablePage
open RyakDB.Table.TableFile
open RyakDB.Execution.Scan

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
    let moveToPageHead ttp =
        ttp.TablePage.MoveToSlotNo -1
        ttp

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

    let sortBySelection sortFields ttp =
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

        let rec findSmallestFrom sortFields minSlotNo ttp =
            if ttp.TablePage.Next() then
                let slotNo = ttp.TablePage.CurrentSlotNo()

                let nextSlotNo =
                    if minSlotNo < 0
                       || ttp
                          |> compareRecords sortFields minSlotNo slotNo > 0 then
                        slotNo
                    else
                        minSlotNo

                ttp |> findSmallestFrom sortFields nextSlotNo
            else
                minSlotNo

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

        let rec moveSmallest i ttp =
            if ttp.TablePage.Next() then
                let minId = ttp |> findSmallestFrom sortFields i
                if minId <> i then ttp |> swapRecords i minId
                ttp.TablePage.MoveToSlotNo i
                ttp |> moveSmallest (i + 1)
            else
                ttp

        ttp |> moveToPageHead |> moveSmallest 0

    let close ttp = ttp.TablePage.Close()

    let newTempTablePage tx blockId schema =
        { TablePage = newTablePage tx blockId schema false
          Schema = schema }

module MergeSort =
    let splitIntoRuns fileService tx schema sortFields srcScan =
        let newTempTablePage tx schema count =
            let tableName =
                FileService.TmpFilePrefix
                + "TableFile"
                + "-"
                + count.ToString()
                + "-"
                + tx.TransactionNo.ToString()

            let buff =
                newTablePageFormatter schema
                |> tx.Buffer.PinNew tableName

            TempTablePage.newTempTablePage tx (buff.BlockId()) schema
            |> TempTablePage.moveToPageHead

        let copySortedToScan scan ttp =
            let rec copyToScan scan ttp =
                if ttp |> TempTablePage.copyToScan scan then ttp |> copyToScan scan else ttp

            ttp
            |> TempTablePage.sortBySelection sortFields
            |> TempTablePage.moveToPageHead
            |> copyToScan scan
            |> TempTablePage.close

        let rec sortedTempTables fileService tx schema tempTables count tt =
            let rec insertFromScan scan ttp =
                let f = ttp |> TempTablePage.insertFromScan scan
                if f > 0 then ttp |> insertFromScan scan else f

            let ttp = newTempTablePage tx schema count
            let flag = ttp |> insertFromScan srcScan
            using (tt.OpenScan()) (fun destScan -> ttp |> copySortedToScan destScan)
            if flag < 0 then
                tt :: tempTables |> List.rev
            else
                newTempTable fileService tx schema
                |> sortedTempTables fileService tx schema (tt :: tempTables) (count + 1)

        srcScan.BeforeFirst()
        if srcScan.Next() then
            newTempTable fileService tx schema
            |> sortedTempTables fileService tx schema [] 0
        else
            []

    let mergeTempTables fileService tx schema comparator tempTables =
        let copyScan schema srcScan destScan =
            destScan.Insert()
            schema.Fields()
            |> List.iter (fun fn -> srcScan.GetVal fn |> destScan.SetVal fn)
            srcScan.Next()

        let rec copyScans schema comparator count destScan srcScans hasMores =
            if count > 0 then
                let mutable target = -1
                List.zip hasMores srcScans
                |> List.iteri (fun i (hasMore, srcScan) ->
                    if hasMore
                       && (target < 0
                           || comparator srcScan.GetVal srcScans.[target].GetVal < 0) then
                        target <- i)

                let hasMore =
                    copyScan schema srcScans.[target] destScan

                hasMores
                |> List.mapi (fun i v -> if i = target then hasMore else v)
                |> copyScans schema comparator (if hasMore then count else count - 1) destScan srcScans

        let srcScans =
            tempTables
            |> List.map (fun tmp ->
                let s = tmp.OpenScan()
                s.BeforeFirst()
                s)

        let merged = newTempTable fileService tx schema
        use destScan = merged.OpenScan()
        let hasMores = srcScans |> List.map (fun s -> s.Next())

        let count =
            hasMores |> List.filter id |> List.length

        copyScans schema comparator count destScan srcScans hasMores
        srcScans |> List.iter (fun s -> s.Close())
        merged

    let rootAvailable available size =
        let rec searchAvailable available i =
            let k =
                System.Math.Pow(double size, 1.0 / double i)
                |> System.Math.Ceiling
                |> int32

            if k <= available then k else searchAvailable available (i + 1)

        if available <= 1 then 1 else searchAvailable available 2

    let mergeRuns fileService tx schema comparator bufferAvailable runs =
        let rec mergeTempTablesByBufferSize fileService tx schema comparator bufferSize tempTables mergedTempTables =
            if tempTables |> List.length > bufferSize then
                mergeTempTables fileService tx schema comparator tempTables.[..(bufferSize - 1)]
                :: mergedTempTables
                |> mergeTempTablesByBufferSize fileService tx schema comparator bufferSize tempTables.[bufferSize..]
            else
                tempTables, mergedTempTables

        let bufferSize =
            runs
            |> List.length
            |> rootAvailable bufferAvailable

        let restTempTables, mergedTempTables =
            mergeTempTablesByBufferSize fileService tx schema comparator bufferSize runs []

        match restTempTables with
        | [] -> mergedTempTables
        | [ tt ] -> tt :: mergedTempTables
        | _ ->
            mergeTempTables fileService tx schema comparator restTempTables
            :: mergedTempTables
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

    let newSortScanFactory fileService bufferPool tx =
        let rec loopMergeRuns schema comparator runs =
            if runs |> List.length > 2 then
                runs
                |> mergeRuns fileService tx schema comparator (bufferPool.Available())
                |> loopMergeRuns schema comparator
            else
                runs

        fun schema sortFields scan ->
            match scan
                  |> splitIntoRuns fileService tx schema sortFields with
            | [] -> scan
            | runs ->
                scan.Close()

                let comparator = newRecordComparator sortFields
                match loopMergeRuns schema comparator runs with
                | [ tt1 ] -> Scan.newSortScan (tt1.OpenScan()) None comparator
                | [ tt1; tt2 ] -> Scan.newSortScan (tt1.OpenScan()) (tt2.OpenScan() |> Some) comparator
                | _ -> failwith "no runs"
