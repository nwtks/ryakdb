module RyakDB.Execution.Plan

open RyakDB.Table
open RyakDB.Index
open RyakDB.Query
open RyakDB.Query.Predicate
open RyakDB.Storage.File
open RyakDB.Transaction
open RyakDB.Execution.Scan
open RyakDB.Execution.AggregationFnScan

type NewSortScan = Schema -> SortField list -> Scan -> Scan

type Plan =
    | TablePlan of fileService: FileService * tx: Transaction * tableInfo: TableInfo
    | SelectPlan of plan: Plan * predicate: Predicate
    | IndexSelectPlan of fileService: FileService * tx: Transaction * plan: Plan * indexInfo: IndexInfo * searchRange: SearchRange
    | ProductPlan of plan1: Plan * plan2: Plan * schema: Schema
    | ProjectPlan of plan: Plan * schema: Schema
    | IndexJoinPlan of fileService: FileService * tx: Transaction * plan: Plan * joinedPlan: Plan * schema: Schema * indexInfo: IndexInfo * joinFields: (string * string) list
    | SortPlan of plan: Plan * schema: Schema * sortFields: SortField list * newSortScan: NewSortScan
    | GroupByPlan of plan: Plan * schema: Schema * groupFields: string list * aggFns: AggregationFnScan list

module Plan =
    let rec schema plan =
        match plan with
        | TablePlan(tableInfo = ti) -> TableInfo.schema ti
        | SelectPlan(plan = p) -> p |> schema
        | IndexSelectPlan(plan = p) -> p |> schema
        | ProductPlan(schema = sch) -> sch
        | ProjectPlan(schema = sch) -> sch
        | IndexJoinPlan(schema = sch) -> sch
        | GroupByPlan(schema = sch) -> sch
        | SortPlan(schema = sch) -> sch

    let rec openScan plan =
        match plan with
        | TablePlan (fs, tx, ti) -> Scan.newTableScan fs tx ti
        | SelectPlan (p, pred) -> Scan.newSelectScan (p |> openScan) pred
        | IndexSelectPlan (fs, tx, p, ii, range) ->
            Scan.newIndexSelectScan (p |> openScan) (IndexFactory.newIndex fs tx ii) range
        | ProductPlan (p1, p2, _) -> Scan.newProductScan (p1 |> openScan) (p2 |> openScan)
        | ProjectPlan (p, schema) ->
            schema.Fields()
            |> Scan.newProjectScan (p |> openScan)
        | IndexJoinPlan (fs, tx, p1, p2, _, ii, fields) ->
            Scan.newIndexJoinScan (p1 |> openScan) (p2 |> openScan) ii (IndexFactory.newIndex fs tx ii) fields
        | GroupByPlan (sp, _, groupFlds, aggFns) -> Scan.newGroupByScan (sp |> openScan) groupFlds aggFns
        | SortPlan (plan, schema, sortFields, newSortScan) -> plan |> openScan |> newSortScan schema sortFields

    let newTablePlan fileService tx tableInfo = TablePlan(fileService, tx, tableInfo)

    let pipeSelectPlan predicate plan =
        if predicate |> Predicate.isEmpty then plan else SelectPlan(plan, predicate)

    let pipeIndexSelectPlan fileService tx indexInfo searchRange plan =
        IndexSelectPlan(fileService, tx, plan, indexInfo, searchRange)

    let pipeProductPlan plan1 plan2 =
        let sch = Schema.newSchema ()
        plan1 |> schema |> sch.AddAll
        plan2 |> schema |> sch.AddAll
        ProductPlan(plan1, plan2, sch)

    let pipeProjectPlan fieldNames plan =
        match fieldNames with
        | [] -> plan
        | _ ->
            let planSchema = schema plan
            let sch = Schema.newSchema ()
            fieldNames
            |> List.iter (fun f -> sch.Add f planSchema)
            ProjectPlan(plan, sch)

    let pipeIndexJoinPlan fileService tx joinedPlan indexInfo joinFields plan =
        let sch = Schema.newSchema ()
        plan |> schema |> sch.AddAll
        joinedPlan |> schema |> sch.AddAll
        IndexJoinPlan(fileService, tx, plan, joinedPlan, sch, indexInfo, joinFields)

    let pipeSortPlan newSortScan sortFields plan =
        match sortFields with
        | [] -> plan
        | _ -> SortPlan(plan, plan |> schema, sortFields, newSortScan)

    let pipeGroupByPlan newSortScan groupFlds aggFns plan =
        match aggFns with
        | [] -> plan
        | _ ->
            let planSchema = plan |> schema
            let sch = Schema.newSchema ()

            let sp =
                match groupFlds with
                | [] -> plan
                | _ ->
                    groupFlds
                    |> List.map (fun f ->
                        planSchema |> sch.Add f
                        SortField(f, SortAsc))
                    |> fun sortFields -> pipeSortPlan newSortScan sortFields plan

            let aggFnScans = aggFns |> List.map newAggregationFnScan
            aggFnScans
            |> List.iter (fun fn ->
                if fn.IsArgumentTypeDependent then planSchema.DbType fn.ArgumentFieldName else fn.FieldType()
                |> sch.AddField fn.FieldName)
            GroupByPlan(sp, sch, groupFlds, aggFnScans)
