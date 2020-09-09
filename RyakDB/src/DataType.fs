module RyakDB.DataType

type SqlType =
    | IntSqlType
    | BigIntSqlType
    | DoubleSqlType
    | VarcharSqlType of argument: int32

type SqlConstant =
    | IntSqlConstant of value: int32
    | BigIntSqlConstant of value: int64
    | DoubleSqlConstant of value: double
    | VarcharSqlConstant of value: string * sqlType: SqlType

type SqlConstantRange =
    { Low: unit -> SqlConstant
      High: unit -> SqlConstant
      IsValid: unit -> bool
      HasLowerBound: unit -> bool
      HasUpperBound: unit -> bool
      IsLowInclusive: unit -> bool
      IsHighInclusive: unit -> bool
      Size: unit -> int32
      ApplyLow: bool -> SqlConstant -> SqlConstantRange
      ApplyHigh: bool -> SqlConstant -> SqlConstantRange
      ApplyConstant: SqlConstant -> SqlConstantRange
      IsConstant: unit -> bool
      ToConstant: unit -> SqlConstant
      Contains: SqlConstant -> bool
      LessThan: SqlConstant -> bool
      GraterThan: SqlConstant -> bool
      IsOverlappingRange: SqlConstantRange -> bool
      ContainsRange: SqlConstantRange -> bool
      Intersect: SqlConstantRange -> SqlConstantRange
      Union: SqlConstantRange -> SqlConstantRange }

module SqlType =
    let argument t =
        match t with
        | VarcharSqlType arg -> arg
        | _ -> -1

    let isFixedSize t =
        match t with
        | IntSqlType
        | BigIntSqlType
        | DoubleSqlType -> true
        | _ -> false

    let isNumeric t =
        match t with
        | IntSqlType
        | BigIntSqlType
        | DoubleSqlType -> true
        | _ -> false

    let maxSize t =
        match t with
        | IntSqlType -> 4
        | BigIntSqlType -> 8
        | DoubleSqlType -> 8
        | VarcharSqlType arg -> System.Text.UTF8Encoding().GetMaxByteCount(arg)

    let minValue t =
        match t with
        | IntSqlType -> IntSqlConstant System.Int32.MinValue
        | BigIntSqlType -> BigIntSqlConstant System.Int64.MinValue
        | DoubleSqlType -> DoubleSqlConstant System.Double.MinValue
        | VarcharSqlType _ -> VarcharSqlConstant("", VarcharSqlType 0)

    let maxValue t =
        match t with
        | IntSqlType -> IntSqlConstant System.Int32.MaxValue
        | BigIntSqlType -> BigIntSqlConstant System.Int64.MaxValue
        | DoubleSqlType -> DoubleSqlConstant System.Double.MaxValue
        | _ -> failwith "Invalid type"

    let toInt t =
        match t with
        | IntSqlType -> 1
        | BigIntSqlType -> 2
        | DoubleSqlType -> 3
        | VarcharSqlType _ -> 4

    let fromInt t arg =
        match t with
        | 1 -> IntSqlType
        | 2 -> BigIntSqlType
        | 3 -> DoubleSqlType
        | 4 -> VarcharSqlType arg
        | _ -> failwith "Invalid type"

module SqlConstant =
    let newVarchar s =
        VarcharSqlConstant(s, VarcharSqlType s.Length)

    let DefaultInt = IntSqlConstant 0
    let DefaultBigInt = BigIntSqlConstant 0L
    let DefaultDouble = DoubleSqlConstant 0.0
    let DefaultVarchar = newVarchar ""

    let sqlType c =
        match c with
        | IntSqlConstant _ -> IntSqlType
        | BigIntSqlConstant _ -> BigIntSqlType
        | DoubleSqlConstant _ -> DoubleSqlType
        | VarcharSqlConstant (_, t) -> t

    let toBytes c: byte [] =
        match c with
        | IntSqlConstant v -> System.BitConverter.GetBytes(v)
        | BigIntSqlConstant v -> System.BitConverter.GetBytes(v)
        | DoubleSqlConstant v -> System.BitConverter.GetBytes(v)
        | VarcharSqlConstant (v, _) -> System.Text.UTF8Encoding().GetBytes(v)

    let fromBytes sqlType (bytes: byte []) =
        match sqlType with
        | IntSqlType ->
            System.BitConverter.ToInt32(System.ReadOnlySpan(bytes))
            |> IntSqlConstant
        | BigIntSqlType ->
            System.BitConverter.ToInt64(System.ReadOnlySpan(bytes))
            |> BigIntSqlConstant
        | DoubleSqlType ->
            System.BitConverter.ToDouble(System.ReadOnlySpan(bytes))
            |> DoubleSqlConstant
        | VarcharSqlType _ ->
            System.Text.UTF8Encoding().GetString(bytes)
            |> newVarchar

    let size c = (toBytes c).Length

    let defaultConstant sqlType =
        match sqlType with
        | IntSqlType -> DefaultInt
        | BigIntSqlType -> DefaultBigInt
        | DoubleSqlType -> DefaultDouble
        | VarcharSqlType _ -> DefaultVarchar

    let toInt value =
        match value with
        | IntSqlConstant v -> v
        | BigIntSqlConstant v -> int32 v
        | DoubleSqlConstant v -> int32 v
        | _ -> failwith "Can't cast"

    let toLong value =
        match value with
        | IntSqlConstant v -> int64 v
        | BigIntSqlConstant v -> v
        | DoubleSqlConstant v -> int64 v
        | _ -> failwith "Can't cast"

    let toDouble value =
        match value with
        | IntSqlConstant v -> double v
        | BigIntSqlConstant v -> double v
        | DoubleSqlConstant v -> v
        | _ -> failwith "Can't cast"

    let toString value =
        match value with
        | IntSqlConstant v -> v.ToString()
        | BigIntSqlConstant v -> v.ToString()
        | DoubleSqlConstant v -> v.ToString()
        | VarcharSqlConstant (v, _) -> v

    let castTo sqlType value =
        match value, sqlType with
        | IntSqlConstant _, IntSqlType -> value
        | IntSqlConstant v, BigIntSqlType -> int64 v |> BigIntSqlConstant
        | IntSqlConstant v, DoubleSqlType -> double v |> DoubleSqlConstant
        | IntSqlConstant v, VarcharSqlType _ -> v.ToString() |> newVarchar
        | BigIntSqlConstant _, BigIntSqlType -> value
        | BigIntSqlConstant v, IntSqlType -> int32 v |> IntSqlConstant
        | BigIntSqlConstant v, DoubleSqlType -> double v |> DoubleSqlConstant
        | BigIntSqlConstant v, VarcharSqlType _ -> v.ToString() |> newVarchar
        | DoubleSqlConstant _, DoubleSqlType -> value
        | DoubleSqlConstant v, IntSqlType -> int32 v |> IntSqlConstant
        | DoubleSqlConstant v, BigIntSqlType -> int64 v |> BigIntSqlConstant
        | DoubleSqlConstant v, VarcharSqlType _ -> v.ToString() |> newVarchar
        | VarcharSqlConstant _, VarcharSqlType _ -> value
        | _ -> failwith "Can't cast"

    let compare lhs rhs =
        let compareValue v1 v2 =
            if v1 < v2 then -1
            elif v1 > v2 then 1
            else 0

        match lhs, rhs with
        | IntSqlConstant lv, IntSqlConstant rv -> compareValue lv rv
        | BigIntSqlConstant lv, BigIntSqlConstant rv -> compareValue lv rv
        | IntSqlConstant lv, BigIntSqlConstant rv -> compareValue (int64 lv) rv
        | BigIntSqlConstant lv, IntSqlConstant rv -> compareValue lv (int64 rv)
        | DoubleSqlConstant lv, DoubleSqlConstant rv -> compareValue lv rv
        | IntSqlConstant lv, DoubleSqlConstant rv -> compareValue (double lv) rv
        | DoubleSqlConstant lv, IntSqlConstant rv -> compareValue lv (double rv)
        | BigIntSqlConstant lv, DoubleSqlConstant rv -> compareValue (double lv) rv
        | DoubleSqlConstant lv, BigIntSqlConstant rv -> compareValue lv (double rv)
        | VarcharSqlConstant (lv, _), VarcharSqlConstant (rv, _) -> compareValue lv rv
        | _ -> failwith "Invalid operation"

    let add lhs rhs =
        match lhs, rhs with
        | IntSqlConstant lv, IntSqlConstant rv -> lv + rv |> IntSqlConstant
        | BigIntSqlConstant lv, BigIntSqlConstant rv -> lv + rv |> BigIntSqlConstant
        | IntSqlConstant lv, BigIntSqlConstant rv -> int64 lv + rv |> BigIntSqlConstant
        | BigIntSqlConstant lv, IntSqlConstant rv -> lv + int64 rv |> BigIntSqlConstant
        | DoubleSqlConstant lv, DoubleSqlConstant rv -> lv + rv |> DoubleSqlConstant
        | IntSqlConstant lv, DoubleSqlConstant rv -> double lv + rv |> DoubleSqlConstant
        | DoubleSqlConstant lv, IntSqlConstant rv -> lv + double rv |> DoubleSqlConstant
        | BigIntSqlConstant lv, DoubleSqlConstant rv -> double lv + rv |> DoubleSqlConstant
        | DoubleSqlConstant lv, BigIntSqlConstant rv -> lv + double rv |> DoubleSqlConstant
        | VarcharSqlConstant (lv, _), VarcharSqlConstant (rv, _) ->
            VarcharSqlConstant(lv + rv, lv.Length + rv.Length |> VarcharSqlType)
        | _ -> failwith "Invalid operation"

    let sub lhs rhs =
        match lhs, rhs with
        | IntSqlConstant lv, IntSqlConstant rv -> lv - rv |> IntSqlConstant
        | BigIntSqlConstant lv, BigIntSqlConstant rv -> lv - rv |> BigIntSqlConstant
        | IntSqlConstant lv, BigIntSqlConstant rv -> int64 lv - rv |> BigIntSqlConstant
        | BigIntSqlConstant lv, IntSqlConstant rv -> lv - int64 rv |> BigIntSqlConstant
        | DoubleSqlConstant lv, DoubleSqlConstant rv -> lv - rv |> DoubleSqlConstant
        | IntSqlConstant lv, DoubleSqlConstant rv -> double lv - rv |> DoubleSqlConstant
        | DoubleSqlConstant lv, IntSqlConstant rv -> lv - double rv |> DoubleSqlConstant
        | BigIntSqlConstant lv, DoubleSqlConstant rv -> double lv - rv |> DoubleSqlConstant
        | DoubleSqlConstant lv, BigIntSqlConstant rv -> lv - double rv |> DoubleSqlConstant
        | _ -> failwith "Invalid operation"

    let mul lhs rhs =
        match lhs, rhs with
        | IntSqlConstant lv, IntSqlConstant rv -> lv * rv |> IntSqlConstant
        | BigIntSqlConstant lv, BigIntSqlConstant rv -> lv * rv |> BigIntSqlConstant
        | IntSqlConstant lv, BigIntSqlConstant rv -> int64 lv * rv |> BigIntSqlConstant
        | BigIntSqlConstant lv, IntSqlConstant rv -> lv * int64 rv |> BigIntSqlConstant
        | DoubleSqlConstant lv, DoubleSqlConstant rv -> lv * rv |> DoubleSqlConstant
        | IntSqlConstant lv, DoubleSqlConstant rv -> double lv * rv |> DoubleSqlConstant
        | DoubleSqlConstant lv, IntSqlConstant rv -> lv * double rv |> DoubleSqlConstant
        | BigIntSqlConstant lv, DoubleSqlConstant rv -> double lv * rv |> DoubleSqlConstant
        | DoubleSqlConstant lv, BigIntSqlConstant rv -> lv * double rv |> DoubleSqlConstant
        | _ -> failwith "Invalid operation"

    let div lhs rhs =
        match lhs, rhs with
        | IntSqlConstant lv, IntSqlConstant rv -> lv / rv |> IntSqlConstant
        | BigIntSqlConstant lv, BigIntSqlConstant rv -> lv / rv |> BigIntSqlConstant
        | IntSqlConstant lv, BigIntSqlConstant rv -> int64 lv / rv |> BigIntSqlConstant
        | BigIntSqlConstant lv, IntSqlConstant rv -> lv / int64 rv |> BigIntSqlConstant
        | DoubleSqlConstant lv, DoubleSqlConstant rv -> lv / rv |> DoubleSqlConstant
        | IntSqlConstant lv, DoubleSqlConstant rv -> double lv / rv |> DoubleSqlConstant
        | DoubleSqlConstant lv, IntSqlConstant rv -> lv / double rv |> DoubleSqlConstant
        | BigIntSqlConstant lv, DoubleSqlConstant rv -> double lv / rv |> DoubleSqlConstant
        | DoubleSqlConstant lv, BigIntSqlConstant rv -> lv / double rv |> DoubleSqlConstant
        | _ -> failwith "Invalid operation"
