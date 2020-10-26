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
    let operate lhs rhs op =
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

    let fieldName exp =
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
    let isSatisfied record lhs rhs op =
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

    let complement op =
        match op with
        | EqualOperator -> EqualOperator
        | GraterThanOperator -> LessThanOperator
        | LessThanOperator -> GraterThanOperator
        | GraterThanEqualOperator -> LessThanEqualOperator
        | LessThanEqualOperator -> GraterThanEqualOperator

module Term =
    let isSatisfied record (Term (op, lhs, rhs)) =
        TermOperator.isSatisfied record lhs rhs op

    let isApplicableTo schema (Term (_, lhs, rhs)) =
        lhs
        |> Expression.isApplicableTo schema
        && rhs |> Expression.isApplicableTo schema

    let operator fieldName (Term (op, lhs, rhs)) =
        match lhs |> Expression.fieldName, rhs |> Expression.fieldName with
        | Some lfn, _ when fieldName = lfn -> Some op
        | _, Some rfn when fieldName = rfn -> TermOperator.complement op |> Some
        | _ -> None

    let oppositeField fieldName (Term (_, lhs, rhs)) =
        match lhs |> Expression.fieldName, rhs |> Expression.fieldName with
        | Some lfn, Some rfh when fieldName = lfn -> Some rfh
        | Some lfn, Some rfn when fieldName = rfn -> Some lfn
        | _ -> None

    let oppositeConstant fieldName (Term (_, lhs, rhs)) =
        match lhs |> Expression.fieldName,
              rhs |> Expression.fieldName,
              lhs |> Expression.constant,
              rhs |> Expression.constant with
        | Some lfn, _, _, Some rv when fieldName = lfn -> Some rv
        | _, Some rfn, Some lv, _ when fieldName = rfn -> Some lv
        | _ -> None

module Predicate =
    let isEmpty (Predicate terms) = List.isEmpty terms

    let isSatisfied record (Predicate terms) =
        terms |> List.forall (Term.isSatisfied record)

    let selectPredicate schema (Predicate terms) =
        terms
        |> List.filter (Term.isApplicableTo schema)
        |> Predicate

    let joinPredicate schema1 schema2 (Predicate terms) =
        let allSchema = Schema.newSchema ()
        allSchema.AddAll schema1
        allSchema.AddAll schema2
        terms
        |> List.filter (fun t ->
            Term.isApplicableTo schema1 t
            |> not
            && Term.isApplicableTo schema2 t |> not
            && Term.isApplicableTo allSchema t)
        |> Predicate

    let joinFields fieldName (Predicate terms) =
        let rec transiteFields (fields, transite) =
            if List.isEmpty transite then
                fields
            else
                terms
                |> List.map (fun t ->
                    let f = List.head transite
                    Term.operator f t, Term.oppositeField f t)
                |> List.filter (fun (op, f) ->
                    op
                    |> Option.exists ((=) EqualOperator)
                    && f
                       |> Option.exists (fun f -> fields |> List.contains f |> not))
                |> List.map (snd >> Option.get)
                |> List.fold (fun (fields, transite) f -> (f :: fields, f :: transite)) (fields, List.tail transite)
                |> transiteFields

        transiteFields ([ fieldName ], [ fieldName ])
        |> List.filter ((<>) fieldName)

    let toConstantRange fieldName (Predicate terms) =
        terms
        |> List.fold (fun cr t ->
            match Term.oppositeConstant fieldName t with
            | Some c ->
                match Term.operator fieldName t with
                | Some EqualOperator ->
                    cr
                    |> Option.map (fun cr -> cr.ApplyConstant c)
                    |> Option.defaultWith (fun () -> DbConstantRange.newConstantRange (Some c) true (Some c) true)
                    |> Some
                | Some GraterThanOperator ->
                    cr
                    |> Option.map (fun cr -> cr.ApplyLow c false)
                    |> Option.defaultWith (fun () -> DbConstantRange.newConstantRange (Some c) false None false)
                    |> Some
                | Some GraterThanEqualOperator ->
                    cr
                    |> Option.map (fun cr -> cr.ApplyLow c true)
                    |> Option.defaultWith (fun () -> DbConstantRange.newConstantRange (Some c) true None false)
                    |> Some
                | Some LessThanOperator ->
                    cr
                    |> Option.map (fun cr -> cr.ApplyHigh c false)
                    |> Option.defaultWith (fun () -> DbConstantRange.newConstantRange None false (Some c) false)
                    |> Some
                | Some LessThanEqualOperator ->
                    cr
                    |> Option.map (fun cr -> cr.ApplyHigh c true)
                    |> Option.defaultWith (fun () -> DbConstantRange.newConstantRange None false (Some c) true)
                    |> Some
                | _ -> cr
            | _ -> cr) None
        |> Option.filter (fun cr ->
            cr.IsValid()
            && (cr.Low()
                |> Option.isSome
                || cr.High() |> Option.isSome))
