module RyakDB.Execution.Scan

open RyakDB.DataType
open RyakDB.Table
open RyakDB.Query
open RyakDB.Query.Predicate
open RyakDB.Table.TableFile
open RyakDB.Transaction
open RyakDB.Execution.AggregationFnScan

[<ReferenceEquality>]
type Scan =
    { GetVal: string -> DbConstant
      BeforeFirst: unit -> unit
      Close: unit -> unit
      Next: unit -> bool
      HasField: string -> bool
      SetVal: string -> DbConstant -> unit
      Insert: unit -> unit
      Delete: unit -> unit
      GetRecordId: unit -> RecordId
      MoveToRecordId: RecordId -> unit }

module Scan =
    let newTableScan fileService tx tableInfo =
        let tableFile =
            newTableFile fileService tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true tableInfo

        { GetVal = fun field -> tableFile.GetVal field
          BeforeFirst = fun () -> tableFile.BeforeFirst()
          Close = fun () -> tableFile.Close()
          Next = fun () -> tableFile.Next()
          HasField = (TableInfo.schema tableInfo).HasField
          SetVal = tableFile.SetVal
          Insert = fun () -> tableFile.Insert()
          Delete = fun () -> tableFile.Delete()
          GetRecordId = fun () -> tableFile.CurrentRecordId()
          MoveToRecordId = tableFile.MoveToRecordId }

    let newSelectScan scan predicate =
        let next () =
            let rec searchNext () =
                if scan.Next() then
                    if Predicate.isSatisfied scan.GetVal predicate
                    then true
                    else searchNext ()
                else
                    false

            searchNext ()

        { GetVal = scan.GetVal
          BeforeFirst = fun () -> scan.BeforeFirst()
          Close = fun () -> scan.Close()
          Next = fun () -> next ()
          HasField = scan.HasField
          SetVal = scan.SetVal
          Insert = fun () -> scan.Insert()
          Delete = fun () -> scan.Delete()
          GetRecordId = fun () -> scan.GetRecordId()
          MoveToRecordId = scan.MoveToRecordId }

    let newProductScan scan1 scan2 =
        let mutable isScan1Empty = false

        let getVal field =
            if scan1.HasField field then scan1.GetVal field else scan2.GetVal field

        let hasField field =
            scan1.HasField field || scan2.HasField field

        let beforeFirst () =
            scan1.BeforeFirst()
            isScan1Empty <- not (scan1.Next())
            scan2.BeforeFirst()

        let next () =
            if isScan1Empty then
                false
            elif scan2.Next() then
                true
            else
                isScan1Empty <- not (scan1.Next())
                if not isScan1Empty then
                    scan2.BeforeFirst()
                    scan2.Next()
                else
                    false

        let close () =
            scan1.Close()
            scan2.Close()

        { GetVal = getVal
          BeforeFirst = fun () -> beforeFirst ()
          Close = fun () -> close ()
          Next = fun () -> next ()
          HasField = hasField
          SetVal = fun _ _ -> ()
          Insert = fun () -> ()
          Delete = fun () -> ()
          GetRecordId = fun () -> failwith "Can't update"
          MoveToRecordId = fun _ -> () }

    let newProjectScan scan fields =
        let getVal field =
            if fields |> List.contains field
            then scan.GetVal field
            else failwith ("field " + field + " not found.")

        let hasField field = fields |> List.contains field

        { GetVal = getVal
          BeforeFirst = fun () -> scan.BeforeFirst()
          Close = fun () -> scan.Close()
          Next = fun () -> scan.Next()
          HasField = hasField
          SetVal = fun _ _ -> ()
          Insert = fun () -> ()
          Delete = fun () -> ()
          GetRecordId = fun () -> failwith "Can't update"
          MoveToRecordId = fun _ -> () }

    let newSortScan scan1 scan2 comparator =
        let mutable currentScan = None
        let mutable hasMore1 = false
        let mutable hasMore2 = false

        let beforeFirst () =
            currentScan <- None
            scan1.BeforeFirst()
            hasMore1 <- scan1.Next()
            hasMore2 <-
                scan2
                |> Option.map (fun s ->
                    s.BeforeFirst()
                    s.Next())
                |> Option.defaultValue false

        let next () =
            currentScan
            |> Option.iter (fun s ->
                if s = scan1 then
                    hasMore1 <- scan1.Next()
                else
                    scan2
                    |> Option.iter (fun s2 -> if s = s2 then hasMore2 <- s2.Next()))
            if hasMore1 && hasMore2 then
                currentScan <-
                    if scan2
                       |> Option.map (fun s2 -> comparator scan1.GetVal s2.GetVal)
                       |> Option.defaultValue -1 < 0 then
                        Some scan1
                    else
                        scan2
                true
            elif hasMore1 then
                currentScan <- Some scan1
                true
            elif hasMore2 then
                currentScan <- scan2
                true
            else
                false

        let close () =
            scan1.Close()
            scan2 |> Option.iter (fun s -> s.Close())

        { GetVal =
              fun field ->
                  match currentScan with
                  | Some scan -> scan.GetVal field
                  | _ -> failwith "Must call next()"
          BeforeFirst = fun () -> beforeFirst ()
          Close = fun () -> close ()
          Next = fun () -> next ()
          HasField =
              fun field ->
                  match currentScan with
                  | Some scan -> scan.HasField field
                  | _ -> failwith "Must call next()"
          SetVal = fun _ _ -> ()
          Insert = fun () -> ()
          Delete = fun () -> ()
          GetRecordId = fun () -> failwith "Can't update"
          MoveToRecordId = fun _ -> () }

    let newGroupByScan scan groupFields aggFns =
        let mutable groupVal = Map.empty
        let mutable moreGroups = false

        let getVal field =
            if groupFields |> List.contains field then
                groupVal.[field]
            else
                (aggFns
                 |> List.filter (fun fn -> fn.FieldName = field)
                 |> List.head).Value()

        let hasField field =
            groupFields
            |> List.contains field
            || aggFns
               |> List.exists (fun fn -> fn.FieldName = field)

        let beforeFirst () =
            scan.BeforeFirst()
            moreGroups <- scan.Next()

        let next () =
            let getGroupVal () =
                groupFields
                |> List.fold (fun gv f -> Map.add f (scan.GetVal f) gv) Map.empty

            let rec searchMoreGroups () =
                if scan.Next() then
                    if groupVal = getGroupVal () then
                        aggFns
                        |> List.iter (fun fn -> fn.ProcessNext scan.GetVal)
                        searchMoreGroups ()
                    else
                        true
                else
                    false

            if moreGroups then
                aggFns
                |> List.iter (fun fn -> fn.ProcessFirst scan.GetVal)
                groupVal <- getGroupVal ()
                moreGroups <- searchMoreGroups ()
                true
            else
                false

        { GetVal = getVal
          BeforeFirst = fun () -> beforeFirst ()
          Close = fun () -> scan.Close()
          Next = fun () -> next ()
          HasField = hasField
          SetVal = fun _ _ -> ()
          Insert = fun () -> ()
          Delete = fun () -> ()
          GetRecordId = fun () -> failwith "Can't update"
          MoveToRecordId = fun _ -> () }
