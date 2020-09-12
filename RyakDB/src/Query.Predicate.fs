module RyakDB.Query.Predicate

open RyakDB.DataType
open RyakDB.Table

type BinaryArithmeticOperator =
    | AddOperator
    | SubOperator
    | MulOperator
    | DivOperator

type Expression =
    | ConstantExpression of value: DbConstant
    | FieldNameExpression of fieldName: string
    | BinaryArithmeticExpression of op: BinaryArithmeticOperator * lhs: Expression * rhs: Expression

type TermOperator =
    | EqualOperator
    | GraterThanOperator
    | LessThanOperator
    | GraterThanEqualOperator
    | LessThanEqualOperator

type Term = Term of op: TermOperator * lhs: Expression * rhs: Expression

type Predicate = Predicate of terms: Term list

module BinaryArithmeticOperator =
    let inline operate lhs rhs op =
        match op with
        | AddOperator -> DbConstant.add lhs rhs
        | SubOperator -> DbConstant.sub lhs rhs
        | MulOperator -> DbConstant.mul lhs rhs
        | DivOperator -> DbConstant.div lhs rhs

module Expression =
    let rec evaluate record exp =
        match exp with
        | ConstantExpression v -> v
        | FieldNameExpression fn -> record fn
        | BinaryArithmeticExpression (op, lhs, rhs) ->
            BinaryArithmeticOperator.operate (lhs |> evaluate record) (rhs |> evaluate record) op

    let rec isApplicableTo schema exp =
        match exp with
        | ConstantExpression _ -> true
        | FieldNameExpression fn -> schema.HasField fn
        | BinaryArithmeticExpression (_, lhs, rhs) ->
            lhs
            |> isApplicableTo schema
            && rhs |> isApplicableTo schema

    let inline fieldName exp =
        match exp with
        | FieldNameExpression fn -> Some fn
        | _ -> None

    let rec constant exp =
        match exp with
        | ConstantExpression v -> Some v
        | BinaryArithmeticExpression (op, lhs, rhs) ->
            match lhs |> constant, rhs |> constant with
            | Some lv, Some rv -> BinaryArithmeticOperator.operate lv rv op |> Some
            | _ -> None
        | _ -> None

module TermOperator =
    let inline isSatisfied record lhs rhs op =
        match op with
        | EqualOperator ->
            DbConstant.compare (lhs |> Expression.evaluate record) (rhs |> Expression.evaluate record) = 0
        | GraterThanOperator ->
            DbConstant.compare (lhs |> Expression.evaluate record) (rhs |> Expression.evaluate record) > 0
        | LessThanOperator ->
            DbConstant.compare (lhs |> Expression.evaluate record) (rhs |> Expression.evaluate record) < 0
        | GraterThanEqualOperator ->
            DbConstant.compare (lhs |> Expression.evaluate record) (rhs |> Expression.evaluate record)
            >= 0
        | LessThanEqualOperator ->
            DbConstant.compare (lhs |> Expression.evaluate record) (rhs |> Expression.evaluate record)
            <= 0

    let inline complement op =
        match op with
        | EqualOperator -> EqualOperator
        | GraterThanOperator -> LessThanOperator
        | LessThanOperator -> GraterThanOperator
        | GraterThanEqualOperator -> LessThanEqualOperator
        | LessThanEqualOperator -> GraterThanEqualOperator

module Term =
    let inline isSatisfied record (Term (op, lhs, rhs)) =
        TermOperator.isSatisfied record lhs rhs op

    let inline isApplicableTo schema (Term (_, lhs, rhs)) =
        lhs
        |> Expression.isApplicableTo schema
        && rhs |> Expression.isApplicableTo schema

    let inline operator fieldName (Term (op, lhs, rhs)) =
        match lhs |> Expression.fieldName, rhs |> Expression.fieldName with
        | Some lfn, _ when fieldName = lfn -> Some op
        | _, Some rfn when fieldName = rfn -> TermOperator.complement op |> Some
        | _ -> None

    let inline oppositeField fieldName (Term (_, lhs, rhs)) =
        match lhs |> Expression.fieldName, rhs |> Expression.fieldName with
        | Some lfn, Some rfh when fieldName = lfn -> Some rfh
        | Some lfn, Some rfn when fieldName = rfn -> Some lfn
        | _ -> None

    let inline oppositeConstant fieldName (Term (_, lhs, rhs)) =
        match lhs |> Expression.fieldName,
              rhs |> Expression.fieldName,
              lhs |> Expression.constant,
              rhs |> Expression.constant with
        | Some lfn, _, _, Some rv when fieldName = lfn -> Some rv
        | _, Some rfn, Some lv, _ when fieldName = rfn -> Some lv
        | _ -> None

module Predicate =
    let inline isSatisfied record (Predicate terms) =
        terms |> List.forall (Term.isSatisfied record)

    let inline selectPredicate schema (Predicate terms) =
        terms
        |> List.filter (Term.isApplicableTo schema)
        |> Predicate

    let inline conjunctWith terms (Predicate pterms) = pterms @ terms |> Predicate
