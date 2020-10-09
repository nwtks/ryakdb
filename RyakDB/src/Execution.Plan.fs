module RyakDB.Execution.Plan

open RyakDB.Table
open RyakDB.Query
open RyakDB.Query.Predicate
open RyakDB.Transaction
open RyakDB.Execution.Scan
open RyakDB.Execution.AggregationFnScan

type NewSortScan = Schema -> SortField list -> Scan -> Scan

type Plan =
    | TablePlan of tx: Transaction * tableInfo: TableInfo
    | SelectPlan of plan: Plan * predicate: Predicate
    | ProductPlan of plan1: Plan * plan2: Plan * schema: Schema
    | ProjectPlan of plan: Plan * schema: Schema
    | SortPlan of plan: Plan * schema: Schema * sortFields: SortField list * newSortScan: NewSortScan
    | GroupByPlan of plan: Plan * schema: Schema * groupFields: string list * aggFns: AggregationFnScan list

module Plan =
    let rec schema plan =
        match plan with
        | TablePlan(tableInfo = ti) -> ti.Schema
        | SelectPlan(plan = p) -> schema p
        | ProductPlan(schema = sch) -> sch
        | ProjectPlan(schema = sch) -> sch
        | GroupByPlan(schema = sch) -> sch
        | SortPlan(schema = sch) -> sch

    let rec openScan fileService plan =
        match plan with
        | TablePlan (tx, ti) -> Scan.newTableScan fileService tx ti
        | SelectPlan (p, pred) -> Scan.newSelectScan (openScan fileService p) pred
        | ProductPlan (p1, p2, _) -> Scan.newProductScan (openScan fileService p1) (openScan fileService p2)
        | ProjectPlan (p, schema) ->
            schema.Fields()
            |> Scan.newProjectScan (openScan fileService p)
        | GroupByPlan (sp, _, groupFlds, aggFns) -> Scan.newGroupByScan (openScan fileService sp) groupFlds aggFns
        | SortPlan (plan, schema, sortFields, newSortScan) ->
            openScan fileService plan
            |> newSortScan schema sortFields

    let newTablePlan tx tableInfo = TablePlan(tx, tableInfo)

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
            then (schema plan).DbType fn.ArgumentFieldName
            else fn.FieldType()
            |> sch.AddField fn.FieldName)
        GroupByPlan(sp, sch, groupFlds, aggFnScans)
