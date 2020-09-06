namespace RyakDB.Execution.AggregationFnScan

open RyakDB.DataType
open RyakDB.Query

type AggregationFnScan =
    { FieldName: string
      ArgumentFieldName: string
      ProcessFirst: Record -> unit
      ProcessNext: Record -> unit
      Value: unit -> SqlConstant
      IsArgumentTypeDependent: bool
      FieldType: unit -> SqlType }

module AggregationFnScan =
    let newAvgFn fieldName aggFnFieldName =
        let mutable count = 1
        let mutable sum = DoubleSqlConstant(nan)

        let processFirst record = sum <- record fieldName

        let processNext record =
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

        let processFirst record = value <- record fieldName

        let processNext record =
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

        let processFirst record = value <- record fieldName

        let processNext record =
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

        let processFirst record = sum <- record fieldName

        let processNext record =
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
