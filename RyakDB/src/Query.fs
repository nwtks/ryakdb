namespace RyakDB.Query

open RyakDB.DataType

type Record = string -> DbConstant

type SortDirection =
    | SortAsc
    | SortDesc

type SortField = SortField of field: string * direction: SortDirection

type AggregationFn =
    | AvgFn of fieldName: string
    | CountFn of fieldName: string
    | MaxFn of fieldName: string
    | MinFn of fieldName: string
    | SumFn of fieldName: string

module AggregationFn =
    let inline fieldName aggregationFn =
        match aggregationFn with
        | AvgFn fn -> "avg_of_" + fn
        | CountFn fn -> "count_of_" + fn
        | MaxFn fn -> "max_of_" + fn
        | MinFn fn -> "min_of_" + fn
        | SumFn fn -> "sum_of_" + fn
