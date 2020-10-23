module RyakDB.DataType

type DbType =
    | IntDbType
    | BigIntDbType
    | DoubleDbType
    | VarcharDbType of argument: int32

type DbConstant =
    | IntDbConstant of value: int32
    | BigIntDbConstant of value: int64
    | DoubleDbConstant of value: double
    | VarcharDbConstant of value: string * dbType: DbType

type DbConstantRange =
    { Low: unit -> DbConstant option
      High: unit -> DbConstant option
      IsValid: unit -> bool
      IsConstant: unit -> bool
      ToConstant: unit -> DbConstant
      Contains: DbConstant -> bool
      ApplyLow: DbConstant -> bool -> DbConstantRange
      ApplyHigh: DbConstant -> bool -> DbConstantRange
      ApplyConstant: DbConstant -> DbConstantRange }

module DbType =
    let inline argument t =
        match t with
        | VarcharDbType arg -> arg
        | _ -> -1

    let inline isFixedSize t =
        match t with
        | IntDbType
        | BigIntDbType
        | DoubleDbType -> true
        | _ -> false

    let inline isNumeric t =
        match t with
        | IntDbType
        | BigIntDbType
        | DoubleDbType -> true
        | _ -> false

    let inline maxSize t =
        match t with
        | IntDbType -> 4
        | BigIntDbType -> 8
        | DoubleDbType -> 8
        | VarcharDbType arg -> System.Text.UTF8Encoding().GetMaxByteCount(arg)

    let inline minValue t =
        match t with
        | IntDbType -> IntDbConstant System.Int32.MinValue
        | BigIntDbType -> BigIntDbConstant System.Int64.MinValue
        | DoubleDbType -> DoubleDbConstant System.Double.MinValue
        | VarcharDbType _ -> VarcharDbConstant("", VarcharDbType 0)

    let inline maxValue t =
        match t with
        | IntDbType -> IntDbConstant System.Int32.MaxValue
        | BigIntDbType -> BigIntDbConstant System.Int64.MaxValue
        | DoubleDbType -> DoubleDbConstant System.Double.MaxValue
        | _ -> failwith ("Invalid type:" + t.ToString())

    let inline toInt t =
        match t with
        | IntDbType -> 1
        | BigIntDbType -> 2
        | DoubleDbType -> 3
        | VarcharDbType _ -> 4

    let inline fromInt t arg =
        match t with
        | 1 -> IntDbType
        | 2 -> BigIntDbType
        | 3 -> DoubleDbType
        | 4 -> VarcharDbType arg
        | _ -> failwith ("Invalid type:" + t.ToString())

module DbConstant =
    let inline newVarchar s =
        VarcharDbConstant(s, VarcharDbType s.Length)

    let DefaultInt = IntDbConstant 0
    let DefaultBigInt = BigIntDbConstant 0L
    let DefaultDouble = DoubleDbConstant 0.0
    let DefaultVarchar = newVarchar ""

    let inline dbType c =
        match c with
        | IntDbConstant _ -> IntDbType
        | BigIntDbConstant _ -> BigIntDbType
        | DoubleDbConstant _ -> DoubleDbType
        | VarcharDbConstant (_, t) -> t

    let toBytes c: byte [] =
        match c with
        | IntDbConstant v -> System.BitConverter.GetBytes(v)
        | BigIntDbConstant v -> System.BitConverter.GetBytes(v)
        | DoubleDbConstant v -> System.BitConverter.GetBytes(v)
        | VarcharDbConstant (v, _) -> System.Text.UTF8Encoding().GetBytes(v)

    let fromBytes dbType (bytes: byte []) =
        match dbType with
        | IntDbType ->
            System.BitConverter.ToInt32(System.ReadOnlySpan(bytes))
            |> IntDbConstant
        | BigIntDbType ->
            System.BitConverter.ToInt64(System.ReadOnlySpan(bytes))
            |> BigIntDbConstant
        | DoubleDbType ->
            System.BitConverter.ToDouble(System.ReadOnlySpan(bytes))
            |> DoubleDbConstant
        | VarcharDbType _ ->
            System.Text.UTF8Encoding().GetString(bytes)
            |> newVarchar

    let inline size c = (toBytes c).Length

    let defaultConstant dbType =
        match dbType with
        | IntDbType -> DefaultInt
        | BigIntDbType -> DefaultBigInt
        | DoubleDbType -> DefaultDouble
        | VarcharDbType _ -> DefaultVarchar

    let toInt value =
        match value with
        | IntDbConstant v -> v
        | BigIntDbConstant v -> int32 v
        | DoubleDbConstant v -> int32 v
        | _ -> failwith ("Can't cast " + value.ToString() + " to int")

    let toLong value =
        match value with
        | IntDbConstant v -> int64 v
        | BigIntDbConstant v -> v
        | DoubleDbConstant v -> int64 v
        | _ -> failwith ("Can't cast " + value.ToString() + " to long")

    let toDouble value =
        match value with
        | IntDbConstant v -> double v
        | BigIntDbConstant v -> double v
        | DoubleDbConstant v -> v
        | _ -> failwith ("Can't cast " + value.ToString() + " to double")

    let toString value =
        match value with
        | IntDbConstant v -> v.ToString()
        | BigIntDbConstant v -> v.ToString()
        | DoubleDbConstant v -> v.ToString()
        | VarcharDbConstant (v, _) -> v

    let castTo dbType value =
        match value, dbType with
        | IntDbConstant _, IntDbType -> value
        | IntDbConstant v, BigIntDbType -> int64 v |> BigIntDbConstant
        | IntDbConstant v, DoubleDbType -> double v |> DoubleDbConstant
        | IntDbConstant v, VarcharDbType _ -> v.ToString() |> newVarchar
        | BigIntDbConstant _, BigIntDbType -> value
        | BigIntDbConstant v, IntDbType -> int32 v |> IntDbConstant
        | BigIntDbConstant v, DoubleDbType -> double v |> DoubleDbConstant
        | BigIntDbConstant v, VarcharDbType _ -> v.ToString() |> newVarchar
        | DoubleDbConstant _, DoubleDbType -> value
        | DoubleDbConstant v, IntDbType -> int32 v |> IntDbConstant
        | DoubleDbConstant v, BigIntDbType -> int64 v |> BigIntDbConstant
        | DoubleDbConstant v, VarcharDbType _ -> v.ToString() |> newVarchar
        | VarcharDbConstant _, VarcharDbType _ -> value
        | _ ->
            failwith
                ("Can't cast "
                 + value.ToString()
                 + " to "
                 + dbType.ToString())

    let compare lhs rhs =
        let compareValue v1 v2 =
            if v1 < v2 then -1
            elif v1 > v2 then 1
            else 0

        match lhs, rhs with
        | IntDbConstant lv, IntDbConstant rv -> compareValue lv rv
        | BigIntDbConstant lv, BigIntDbConstant rv -> compareValue lv rv
        | IntDbConstant lv, BigIntDbConstant rv -> compareValue (int64 lv) rv
        | BigIntDbConstant lv, IntDbConstant rv -> compareValue lv (int64 rv)
        | DoubleDbConstant lv, DoubleDbConstant rv -> compareValue lv rv
        | IntDbConstant lv, DoubleDbConstant rv -> compareValue (double lv) rv
        | DoubleDbConstant lv, IntDbConstant rv -> compareValue lv (double rv)
        | BigIntDbConstant lv, DoubleDbConstant rv -> compareValue (double lv) rv
        | DoubleDbConstant lv, BigIntDbConstant rv -> compareValue lv (double rv)
        | VarcharDbConstant (lv, _), VarcharDbConstant (rv, _) -> compareValue lv rv
        | _ ->
            failwith
                ("Invalid operation:compare "
                 + lhs.ToString()
                 + " "
                 + rhs.ToString())

    let add lhs rhs =
        match lhs, rhs with
        | IntDbConstant lv, IntDbConstant rv -> lv + rv |> IntDbConstant
        | BigIntDbConstant lv, BigIntDbConstant rv -> lv + rv |> BigIntDbConstant
        | IntDbConstant lv, BigIntDbConstant rv -> int64 lv + rv |> BigIntDbConstant
        | BigIntDbConstant lv, IntDbConstant rv -> lv + int64 rv |> BigIntDbConstant
        | DoubleDbConstant lv, DoubleDbConstant rv -> lv + rv |> DoubleDbConstant
        | IntDbConstant lv, DoubleDbConstant rv -> double lv + rv |> DoubleDbConstant
        | DoubleDbConstant lv, IntDbConstant rv -> lv + double rv |> DoubleDbConstant
        | BigIntDbConstant lv, DoubleDbConstant rv -> double lv + rv |> DoubleDbConstant
        | DoubleDbConstant lv, BigIntDbConstant rv -> lv + double rv |> DoubleDbConstant
        | VarcharDbConstant (lv, _), VarcharDbConstant (rv, _) ->
            VarcharDbConstant(lv + rv, lv.Length + rv.Length |> VarcharDbType)
        | _ ->
            failwith
                ("Invalid operation:add "
                 + lhs.ToString()
                 + " "
                 + rhs.ToString())

    let sub lhs rhs =
        match lhs, rhs with
        | IntDbConstant lv, IntDbConstant rv -> lv - rv |> IntDbConstant
        | BigIntDbConstant lv, BigIntDbConstant rv -> lv - rv |> BigIntDbConstant
        | IntDbConstant lv, BigIntDbConstant rv -> int64 lv - rv |> BigIntDbConstant
        | BigIntDbConstant lv, IntDbConstant rv -> lv - int64 rv |> BigIntDbConstant
        | DoubleDbConstant lv, DoubleDbConstant rv -> lv - rv |> DoubleDbConstant
        | IntDbConstant lv, DoubleDbConstant rv -> double lv - rv |> DoubleDbConstant
        | DoubleDbConstant lv, IntDbConstant rv -> lv - double rv |> DoubleDbConstant
        | BigIntDbConstant lv, DoubleDbConstant rv -> double lv - rv |> DoubleDbConstant
        | DoubleDbConstant lv, BigIntDbConstant rv -> lv - double rv |> DoubleDbConstant
        | _ ->
            failwith
                ("Invalid operation:sub "
                 + lhs.ToString()
                 + " "
                 + rhs.ToString())

    let mul lhs rhs =
        match lhs, rhs with
        | IntDbConstant lv, IntDbConstant rv -> lv * rv |> IntDbConstant
        | BigIntDbConstant lv, BigIntDbConstant rv -> lv * rv |> BigIntDbConstant
        | IntDbConstant lv, BigIntDbConstant rv -> int64 lv * rv |> BigIntDbConstant
        | BigIntDbConstant lv, IntDbConstant rv -> lv * int64 rv |> BigIntDbConstant
        | DoubleDbConstant lv, DoubleDbConstant rv -> lv * rv |> DoubleDbConstant
        | IntDbConstant lv, DoubleDbConstant rv -> double lv * rv |> DoubleDbConstant
        | DoubleDbConstant lv, IntDbConstant rv -> lv * double rv |> DoubleDbConstant
        | BigIntDbConstant lv, DoubleDbConstant rv -> double lv * rv |> DoubleDbConstant
        | DoubleDbConstant lv, BigIntDbConstant rv -> lv * double rv |> DoubleDbConstant
        | _ ->
            failwith
                ("Invalid operation:mul "
                 + lhs.ToString()
                 + " "
                 + rhs.ToString())

    let div lhs rhs =
        match lhs, rhs with
        | IntDbConstant lv, IntDbConstant rv -> lv / rv |> IntDbConstant
        | BigIntDbConstant lv, BigIntDbConstant rv -> lv / rv |> BigIntDbConstant
        | IntDbConstant lv, BigIntDbConstant rv -> int64 lv / rv |> BigIntDbConstant
        | BigIntDbConstant lv, IntDbConstant rv -> lv / int64 rv |> BigIntDbConstant
        | DoubleDbConstant lv, DoubleDbConstant rv -> lv / rv |> DoubleDbConstant
        | IntDbConstant lv, DoubleDbConstant rv -> double lv / rv |> DoubleDbConstant
        | DoubleDbConstant lv, IntDbConstant rv -> lv / double rv |> DoubleDbConstant
        | BigIntDbConstant lv, DoubleDbConstant rv -> double lv / rv |> DoubleDbConstant
        | DoubleDbConstant lv, BigIntDbConstant rv -> lv / double rv |> DoubleDbConstant
        | _ ->
            failwith
                ("Invalid operation:div "
                 + lhs.ToString()
                 + " "
                 + rhs.ToString())

module DbConstantRange =
    let isValid low includesLow high includesHigh =
        match low, high with
        | Some l, Some h ->
            DbConstant.compare l h < 0
            || includesLow
               && includesHigh
               && DbConstant.compare l h = 0
        | _ -> true

    let isConstant low includesLow high includesHigh =
        match low, high with
        | Some l, Some h ->
            includesLow
            && includesHigh
            && DbConstant.compare l h = 0
        | _ -> false

    let toConstant low includesLow high includesHigh =
        if isConstant low includesLow high includesHigh
        then Option.get low
        else failwith "Not constant"

    let contains low includesLow high includesHigh value =
        isValid low includesLow high includesHigh
        && low
        |> Option.map (fun l ->
            DbConstant.compare l value < 0
            || includesLow && DbConstant.compare l value = 0)
        |> Option.defaultValue true
        && high
           |> Option.map (fun h ->
               DbConstant.compare h value > 0
               || includesHigh && DbConstant.compare h value = 0)
           |> Option.defaultValue true

    let applyLow low includesLow value includes =
        low
        |> Option.map (fun l ->
            if DbConstant.compare l value > 0 then
                (Some value), includes
            elif includesLow
                 && DbConstant.compare l value = 0
                 && not includes then
                low, false
            else
                low, includesLow)
        |> Option.defaultValue (low, includesLow)

    let applyHigh high includesHigh value includes =
        high
        |> Option.map (fun h ->
            if DbConstant.compare h value < 0 then
                (Some value), includes
            elif includesHigh
                 && DbConstant.compare h value = 0
                 && not includes then
                high, false
            else
                high, includesHigh)
        |> Option.defaultValue (high, includesHigh)

    let applyConstant low includesLow high includesHigh value =
        let newLow, newincludesLow = applyLow low includesLow value true
        let newHigh, newIncludesHigh = applyHigh high includesHigh value true
        newLow, newincludesLow, newHigh, newIncludesHigh

    let rec newConstantRange low includesLow high includesHigh =
        { Low = fun () -> low
          High = fun () -> high
          IsValid = fun () -> isValid low includesLow high includesHigh
          IsConstant = fun () -> isConstant low includesLow high includesHigh
          ToConstant = fun () -> toConstant low includesLow high includesHigh
          Contains = fun value -> contains low includesLow high includesHigh value
          ApplyLow =
              fun value includes ->
                  let newLow, newincludesLow = applyLow low includesLow value includes
                  newConstantRange newLow newincludesLow high includesHigh
          ApplyHigh =
              fun value includes ->
                  let newHigh, newIncludesHigh =
                      applyHigh high includesHigh value includes

                  newConstantRange low includesLow newHigh newIncludesHigh
          ApplyConstant =
              fun value ->
                  let newLow, newincludesLow, newHigh, newIncludesHigh =
                      applyConstant low includesLow high includesHigh value

                  newConstantRange newLow newincludesLow newHigh newIncludesHigh }

    let newConstantRangeByConstant constant =
        newConstantRange (Some constant) true (Some constant) true

    let NoRange = newConstantRange None false None false
