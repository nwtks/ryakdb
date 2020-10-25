module RyakDB.Execution.QueryPlanner

open RyakDB.DataType
open RyakDB.Table
open RyakDB.Index
open RyakDB.Query.Predicate
open RyakDB.Sql.Parse
open RyakDB.Transaction
open RyakDB.Catalog.CatalogService
open RyakDB.Execution.Plan
open RyakDB.Execution.MergeSort

type QueryPlanner =
    { CreatePlan: Transaction -> QueryData -> Plan }

module QueryPlanner =
    type QueryPlan =
        | TableQueryPlan of plan: Plan * tableName: string
        | ViewQueryPlan of plan: Plan

    let addSelectPredicate predicate plan =
        Plan.pipeSelectPlan (Predicate.selectPredicate (Plan.schema plan) predicate) plan

    let addJoinPredicate predicate joinedPlan plan =
        Plan.pipeSelectPlan (Predicate.joinPredicate (Plan.schema plan) (Plan.schema joinedPlan) predicate) plan

    let makeIndexSelectPlan fileService catalogService tx tableName predicate excludedFields =
        let getConstantRanges predicate indexInfo =
            let indexType = IndexInfo.indexType indexInfo
            IndexInfo.fieldNames indexInfo
            |> List.map (fun f -> f, Predicate.toConstantRange f predicate)
            |> List.takeWhile (fun (_, range) -> Option.isSome range)
            |> List.map (fun (f, range) -> f, Option.get range)
            |> List.takeWhile (fun (_, range) ->
                indexType = BTree
                || indexType = Hash && range.IsConstant())
            |> List.fold (fun ranges (f, range) -> ranges |> Map.add f range) Map.empty

        let getIndexSelectPlan fileService tx ranges =
            let index, constantRanges =
                ranges
                |> List.maxBy (fun (_, ranges) -> Map.count ranges)

            SearchRange.newSearchRangeByFieldsRanges (IndexInfo.fieldNames index) constantRanges
            |> Plan.pipeIndexSelectPlan fileService tx index

        match catalogService.GetIndexInfosByTable tx tableName
              |> List.filter (fun ii ->
                  not
                      (IndexInfo.fieldNames ii
                       |> List.exists (fun f -> List.contains f excludedFields)))
              |> List.map (fun ii -> ii, getConstantRanges predicate ii) with
        | [] -> None
        | ranges -> getIndexSelectPlan fileService tx ranges |> Some

    let makeIndexJoinPlan fileService catalogService tx joinedTableName predicate joinedPlan plan =
        let getJoinFields (schema: Schema) indexInfo =
            IndexInfo.fieldNames indexInfo
            |> List.collect (fun iif ->
                Predicate.joinFields iif predicate
                |> List.takeWhile schema.HasField
                |> List.map (fun f -> iif, f))

        let getIndexJoinPlan candidates =
            let indexInfo, joinFields =
                candidates
                |> List.maxBy (fun (_, pairs) -> List.length pairs)

            Plan.pipeIndexJoinPlan fileService tx joinedPlan indexInfo joinFields plan

        let schema = Plan.schema plan
        match catalogService.GetIndexInfosByTable tx joinedTableName
              |> List.map (fun ii -> ii, getJoinFields schema ii) with
        | [] ->
            joinedPlan
            |> (makeIndexSelectPlan fileService catalogService tx joinedTableName predicate []
                |> Option.defaultValue id)
            |> addSelectPredicate predicate
            |> Plan.pipeProductPlan plan
        | candidates -> getIndexJoinPlan candidates

    let rec createPlan fileService
                       bufferPool
                       catalogService
                       tx
                       (QueryData (projectionFields, tables, predicate, groupFields, aggregationFns, sortFields))
                       =
        let getQueryPlans tables =
            tables
            |> List.map (fun tableName ->
                match catalogService.GetViewDef tx tableName with
                | Some viewDef ->
                    Parser.queryCommand viewDef
                    |> createPlan fileService bufferPool catalogService tx
                    |> addSelectPredicate predicate
                    |> ViewQueryPlan
                | _ ->
                    match catalogService.GetTableInfo tx tableName with
                    | Some ti ->
                        (Plan.newTablePlan fileService tx ti, tableName)
                        |> TableQueryPlan
                    | _ -> failwith ("Not found table:" + tableName))

        let getJoinPlans queryPlans =
            queryPlans
            |> List.tail
            |> List.fold (fun plan queryPlan ->
                match queryPlan with
                | TableQueryPlan (p, tn) ->
                    makeIndexJoinPlan fileService catalogService tx tn predicate p plan
                    |> addJoinPredicate predicate p
                | ViewQueryPlan p -> Plan.pipeProductPlan p plan
                |> addSelectPredicate predicate)
                   (match List.head queryPlans with
                    | TableQueryPlan (p, tn) ->
                        p
                        |> (makeIndexSelectPlan fileService catalogService tx tn predicate []
                            |> Option.defaultValue id)
                        |> addSelectPredicate predicate
                    | ViewQueryPlan p -> p)

        let newSortScan =
            MergeSort.newSortScan fileService bufferPool tx

        tables
        |> getQueryPlans
        |> getJoinPlans
        |> Plan.pipeSelectPlan predicate
        |> Plan.pipeGroupByPlan newSortScan groupFields aggregationFns
        |> Plan.pipeProjectPlan projectionFields
        |> Plan.pipeSortPlan newSortScan sortFields

let newQueryPlanner fileService bufferPool catalogService =
    { CreatePlan = QueryPlanner.createPlan fileService bufferPool catalogService }
