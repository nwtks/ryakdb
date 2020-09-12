module RyakDB.Execution.AggregationFnScan

open RyakDB.DataType
open RyakDB.Query

type AggregationFnScan =
    { FieldName: string
      ArgumentFieldName: string
      ProcessFirst: Record -> unit
      ProcessNext: Record -> unit
      Value: unit -> DbConstant
      IsArgumentTypeDependent: bool
      FieldType: unit -> DbType }

module AggregationFnScan =
    let newAvgFn fieldName aggFnFieldName =
        let mutable count = 1
        let mutable sum = DoubleDbConstant(nan)

        let inline processFirst record = sum <- record fieldName

        let inline processNext record =
            sum <- record fieldName |> DbConstant.add sum
            count <- count + 1

        { FieldName = aggFnFieldName
          ArgumentFieldName = fieldName
          ProcessFirst = processFirst
          ProcessNext = processNext
          Value = fun () -> DbConstant.div (sum |> DbConstant.castTo DoubleDbType) (IntDbConstant count)
          IsArgumentTypeDependent = false
          FieldType = fun () -> DoubleDbType }

    let newCountFn fieldName aggFnFieldName =
        let mutable count = 0

        { FieldName = aggFnFieldName
          ArgumentFieldName = fieldName
          ProcessFirst = fun _ -> count <- 1
          ProcessNext = fun _ -> count <- count + 1
          Value = fun () -> (IntDbConstant count)
          IsArgumentTypeDependent = false
          FieldType = fun () -> IntDbType }

    let newMaxFn fieldName aggFnFieldName =
        let mutable value = DoubleDbConstant(nan)

        let inline processFirst record = value <- record fieldName

        let inline processNext record =
            let v = record fieldName
            value <- if DbConstant.compare v value > 0 then v else value

        { FieldName = aggFnFieldName
          ArgumentFieldName = fieldName
          ProcessFirst = processFirst
          ProcessNext = processNext
          Value = fun () -> value
          IsArgumentTypeDependent = true
          FieldType = fun () -> failwith "type is dependent" }

    let newMinFn fieldName aggFnFieldName =
        let mutable value = DoubleDbConstant(nan)

        let inline processFirst record = value <- record fieldName

        let inline processNext record =
            let v = record fieldName
            value <- if DbConstant.compare v value < 0 then v else value

        { FieldName = aggFnFieldName
          ArgumentFieldName = fieldName
          ProcessFirst = processFirst
          ProcessNext = processNext
          Value = fun () -> value
          IsArgumentTypeDependent = true
          FieldType = fun () -> failwith "type is dependent" }

    let newSumFn fieldName aggFnFieldName =
        let mutable sum = DoubleDbConstant(nan)

        let inline processFirst record = sum <- record fieldName

        let inline processNext record =
            sum <- record fieldName |> DbConstant.add sum

        { FieldName = aggFnFieldName
          ArgumentFieldName = fieldName
          ProcessFirst = processFirst
          ProcessNext = processNext
          Value = fun () -> sum
          IsArgumentTypeDependent = false
          FieldType = fun () -> DoubleDbType }

let newAggregationFnScan aggregationFn =
    let aggFnFieldName = aggregationFn |> AggregationFn.fieldName
    match aggregationFn with
    | AvgFn (fn) -> AggregationFnScan.newAvgFn fn aggFnFieldName
    | CountFn (fn) -> AggregationFnScan.newCountFn fn aggFnFieldName
    | MaxFn (fn) -> AggregationFnScan.newMaxFn fn aggFnFieldName
    | MinFn (fn) -> AggregationFnScan.newMinFn fn aggFnFieldName
    | SumFn (fn) -> AggregationFnScan.newSumFn fn aggFnFieldName
