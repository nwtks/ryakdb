namespace RyakDB.Query.Algebra

open RyakDB.Sql.Type
open RyakDB.Sql.Predicate
open RyakDB.Storage.Type

[<ReferenceEquality>]
type Scan =
    { GetVal: string -> SqlConstant
      BeforeFirst: unit -> unit
      Close: unit -> unit
      Next: unit -> bool
      HasField: string -> bool
      SetVal: string -> SqlConstant -> unit
      Insert: unit -> unit
      Delete: unit -> unit
      GetRecordId: unit -> RecordId
      MoveToRecordId: RecordId -> unit }

type AggregationFnScan =
    { FieldName: string
      ArgumentFieldName: string
      ProcessFirst: Record -> unit
      ProcessNext: Record -> unit
      Value: unit -> SqlConstant
      IsArgumentTypeDependent: bool
      FieldType: unit -> SqlType }

type NewSortScan = Schema -> SortField list -> Scan -> Scan

type Plan =
    | TablePlan of tx: Transaction * tableInfo: TableInfo
    | SelectPlan of plan: Plan * predicate: Predicate
    | ProductPlan of plan1: Plan * plan2: Plan * schema: Schema
    | ProjectPlan of plan: Plan * schema: Schema
    | SortPlan of plan: Plan * schema: Schema * sortFields: SortField list * newSortScan: NewSortScan
    | GroupByPlan of plan: Plan * schema: Schema * groupFields: string list * aggFns: AggregationFnScan list

module AggregationFnScan =
    let newAvgFn fieldName aggFnFieldName =
        let mutable count = 1
        let mutable sum = DoubleSqlConstant(nan)

        let processFirst (record: Record) = sum <- record fieldName

        let processNext (record: Record) =
            sum <- record fieldName |> SqlConstant.add sum
            count <- count + 1

        { FieldName = aggFnFieldName
          ArgumentFieldName = fieldName
          ProcessFirst = processFirst
          ProcessNext = processNext
          Value = fun () -> SqlConstant.div (sum |> SqlConstant.castTo DoubleSqlType) (IntSqlConstant count)
          IsArgumentTypeDependent = false
          FieldType = fun () -> DoubleSqlType }

    let newCountFn fieldName aggFnFieldName =
        let mutable count = 0

        { FieldName = aggFnFieldName
          ArgumentFieldName = fieldName
          ProcessFirst = fun _ -> count <- 1
          ProcessNext = fun _ -> count <- count + 1
          Value = fun () -> (IntSqlConstant count)
          IsArgumentTypeDependent = false
          FieldType = fun () -> IntSqlType }

    let newMaxFn fieldName aggFnFieldName =
        let mutable value = DoubleSqlConstant(nan)

        let processFirst (record: Record) = value <- record fieldName

        let processNext (record: Record) =
            let v = record fieldName
            value <- if SqlConstant.compare v value > 0 then v else value

        { FieldName = aggFnFieldName
          ArgumentFieldName = fieldName
          ProcessFirst = processFirst
          ProcessNext = processNext
          Value = fun () -> value
          IsArgumentTypeDependent = true
          FieldType = fun () -> failwith "type is dependent" }

    let newMinFn fieldName aggFnFieldName =
        let mutable value = DoubleSqlConstant(nan)

        let processFirst (record: Record) = value <- record fieldName

        let processNext (record: Record) =
            let v = record fieldName
            value <- if SqlConstant.compare v value < 0 then v else value

        { FieldName = aggFnFieldName
          ArgumentFieldName = fieldName
          ProcessFirst = processFirst
          ProcessNext = processNext
          Value = fun () -> value
          IsArgumentTypeDependent = true
          FieldType = fun () -> failwith "type is dependent" }

    let newSumFn fieldName aggFnFieldName =
        let mutable sum = DoubleSqlConstant(nan)

        let processFirst (record: Record) = sum <- record fieldName

        let processNext (record: Record) =
            sum <- record fieldName |> SqlConstant.add sum

        { FieldName = aggFnFieldName
          ArgumentFieldName = fieldName
          ProcessFirst = processFirst
          ProcessNext = processNext
          Value = fun () -> sum
          IsArgumentTypeDependent = false
          FieldType = fun () -> DoubleSqlType }

    let newAggregationFnScan aggregationFn =
        let aggFnFieldName = aggregationFn |> AggregationFn.fieldName
        match aggregationFn with
        | AvgFn (fn) -> newAvgFn fn aggFnFieldName
        | CountFn (fn) -> newCountFn fn aggFnFieldName
        | MaxFn (fn) -> newMaxFn fn aggFnFieldName
        | MinFn (fn) -> newMinFn fn aggFnFieldName
        | SumFn (fn) -> newSumFn fn aggFnFieldName

module Scan =
    let newTableScan tx tableInfo =
        let recodFile = tableInfo.OpenFile tx true
        let schema = tableInfo.Schema

        { GetVal = fun field -> recodFile.GetVal field |> Option.get
          BeforeFirst = fun () -> recodFile.BeforeFirst()
          Close = fun () -> recodFile.Close()
          Next = fun () -> recodFile.Next()
          HasField = schema.HasField
          SetVal = recodFile.SetVal
          Insert = fun () -> recodFile.Insert()
          Delete = fun () -> recodFile.Delete()
          GetRecordId = fun () -> recodFile.CurrentRecordId() |> Option.get
          MoveToRecordId = recodFile.MoveToRecordId }

    let newSelectScan (scan: Scan) (predicate: Predicate) =
        let next () =
            let rec loopNext () =
                if scan.Next() then
                    if Predicate.isSatisfied scan.GetVal predicate
                    then true
                    else loopNext ()
                else
                    false

            loopNext ()

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

    let newProductScan (scan1: Scan) (scan2: Scan) =
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

    let newProjectScan (scan: Scan) fields =
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

    let newSortScan (scan1: Scan) (scan2: Scan option) comparator =
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

        { GetVal = fun field -> (Option.get currentScan).GetVal field
          BeforeFirst = fun () -> beforeFirst ()
          Close = fun () -> close ()
          Next = fun () -> next ()
          HasField = fun field -> (Option.get currentScan).HasField field
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
                aggFns
                |> List.filter (fun fn -> fn.FieldName = field)
                |> List.tryHead
                |> Option.map (fun fn -> fn.Value())
                |> Option.get

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

            let rec loopMoreGroups () =
                if scan.Next() then
                    if groupVal = getGroupVal () then
                        aggFns
                        |> List.iter (fun fn -> fn.ProcessNext scan.GetVal)
                        loopMoreGroups ()
                    else
                        true
                else
                    false

            if moreGroups then
                aggFns
                |> List.iter (fun fn -> fn.ProcessFirst scan.GetVal)
                groupVal <- getGroupVal ()
                moreGroups <- loopMoreGroups ()
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

module Plan =
    let rec schema plan =
        match plan with
        | TablePlan(tableInfo = ti) -> ti.Schema
        | SelectPlan(plan = p) -> schema p
        | ProductPlan(schema = sch) -> sch
        | ProjectPlan(schema = sch) -> sch
        | GroupByPlan(schema = sch) -> sch
        | SortPlan(schema = sch) -> sch

    let rec openScan plan: Scan =
        match plan with
        | TablePlan (tx, ti) -> Scan.newTableScan tx ti
        | SelectPlan (p, pred) -> Scan.newSelectScan (openScan p) pred
        | ProductPlan (p1, p2, _) -> Scan.newProductScan (openScan p1) (openScan p2)
        | ProjectPlan (p, schema) ->
            schema.Fields()
            |> Scan.newProjectScan (openScan p)
        | GroupByPlan (sp, _, groupFlds, aggFns) -> Scan.newGroupByScan (openScan sp) groupFlds aggFns
        | SortPlan (plan, schema, sortFields, newSortScan) -> openScan plan |> newSortScan schema sortFields

    let newTablePlan (tx: Transaction) tableInfo = TablePlan(tx, tableInfo)

    let newSelectPlan predicate plan = SelectPlan(plan, predicate)

    let newProductPlan plan1 plan2 =
        let sch = Schema.newSchema ()
        schema plan1 |> sch.AddAll
        schema plan2 |> sch.AddAll
        ProductPlan(plan1, plan2, sch)

    let newProjectPlan fieldNames plan =
        let sch = Schema.newSchema ()
        fieldNames
        |> List.iter (fun f -> schema plan |> sch.Add f)
        ProjectPlan(plan, sch)

    let newSortPlan newSortScan sortFields plan =
        let sch = schema plan
        SortPlan(plan, sch, sortFields, newSortScan)

    let newGroupByPlan newSortScan (groupFlds: string list) aggFns plan =
        let sch = Schema.newSchema ()

        let sp =
            if groupFlds.IsEmpty then
                plan
            else
                groupFlds
                |> List.map (fun f ->
                    schema plan |> sch.Add f
                    SortField(f, SortAsc))
                |> fun sortFields -> newSortPlan newSortScan sortFields plan

        let aggFnScans =
            aggFns
            |> List.map AggregationFnScan.newAggregationFnScan

        aggFnScans
        |> List.iter (fun fn ->
            if fn.IsArgumentTypeDependent
            then (schema plan).SqlType fn.ArgumentFieldName
            else fn.FieldType()
            |> sch.AddField fn.FieldName)

        GroupByPlan(sp, sch, groupFlds, aggFnScans)
