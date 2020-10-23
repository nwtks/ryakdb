namespace RyakDB.Index

open RyakDB.DataType
open RyakDB.Table

type SearchKey = SearchKey of constants: DbConstant list

type SearchKeyType = SearchKeyType of types: DbType list

type SearchRange =
    { Size: unit -> int32
      IsValid: unit -> bool
      GetLow: unit -> SearchKey option
      GetHigh: unit -> SearchKey option
      MatchsKey: SearchKey -> bool
      BetweenLowHigh: SearchKey -> bool
      IsSingleValue: unit -> bool
      ToSearchKey: unit -> SearchKey }

type IndexType =
    | Hash
    | BTree

type IndexInfo = IndexInfo of indexName: string * indexType: IndexType * tableInfo: TableInfo * fieldNames: string list

type Index =
    { BeforeFirst: SearchRange -> unit
      Next: unit -> bool
      GetDataRecordId: unit -> RecordId
      Insert: bool -> SearchKey -> RecordId -> unit
      Delete: bool -> SearchKey -> RecordId -> unit
      Close: unit -> unit
      PreLoadToMemory: unit -> unit }
    interface System.IDisposable with
        member this.Dispose() = this.Close()

module SearchKey =
    let compare (SearchKey key1) (SearchKey key2) =
        let rec searchCompares key1 key2 =
            match key1, key2 with
            | [], [] -> 0
            | _, [] -> 1
            | [], _ -> -1
            | h1 :: t1, h2 :: t2 ->
                let comp = DbConstant.compare h1 h2
                if comp = 0 then searchCompares t1 t2 else comp

        searchCompares key1 key2

    let inline newSearchKey constants = SearchKey constants

module SearchKeyType =
    let getMin (SearchKeyType types) =
        types
        |> List.map DbType.minValue
        |> SearchKey.newSearchKey

    let getMax (SearchKeyType types) =
        types
        |> List.map DbType.maxValue
        |> SearchKey.newSearchKey

    let inline newSearchKeyTypeByTypes types = SearchKeyType types

    let inline newSearchKeyType schema indexedFields =
        indexedFields
        |> List.map schema.DbType
        |> SearchKeyType

module SearchRange =
    let isValid (ranges: DbConstantRange list) =
        ranges |> List.forall (fun r -> r.IsValid())

    let getLow ranges =
        let values = ranges |> List.map (fun r -> r.Low())
        if List.contains None values then
            None
        else
            values
            |> List.map Option.get
            |> SearchKey.newSearchKey
            |> Some

    let getHigh ranges =
        let values = ranges |> List.map (fun r -> r.High())
        if List.contains None values then
            None
        else
            values
            |> List.map Option.get
            |> SearchKey.newSearchKey
            |> Some

    let matchsKey ranges (SearchKey keyConstants) =
        List.length ranges = List.length keyConstants
        && List.zip ranges keyConstants
           |> List.forall (fun (r, c) -> r.Contains c)

    let betweenLowHigh ranges key =
        getLow ranges
        |> Option.map (fun min -> SearchKey.compare key min >= 0)
        |> Option.defaultValue true
        && getHigh ranges
           |> Option.map (fun max -> SearchKey.compare key max <= 0)
           |> Option.defaultValue true

    let isSingleValue ranges =
        ranges |> List.forall (fun r -> r.IsConstant())

    let toSearchKey ranges =
        ranges
        |> List.map (fun r -> r.ToConstant())
        |> SearchKey.newSearchKey

    let newSearchRangeByRanges ranges =
        { Size = fun () -> List.length ranges
          IsValid = fun () -> isValid ranges
          GetLow = fun () -> getLow ranges
          GetHigh = fun () -> getHigh ranges
          MatchsKey = matchsKey ranges
          BetweenLowHigh = betweenLowHigh ranges
          IsSingleValue = fun () -> isSingleValue ranges
          ToSearchKey = fun () -> toSearchKey ranges }

    let newSearchRangeBySearchKey (SearchKey searchKeyConstants) =
        searchKeyConstants
        |> List.map DbConstantRange.newConstantRangeByConstant
        |> newSearchRangeByRanges

    let newSearchRangeByFieldsRanges fields ranges =
        fields
        |> List.map (fun f -> if Map.containsKey f ranges then Map.find f ranges else DbConstantRange.NoRange)
        |> newSearchRangeByRanges

module IndexInfo =
    let inline indexName (IndexInfo (indexName, _, _, _)) = indexName

    let inline indexType (IndexInfo (_, indexType, _, _)) = indexType

    let inline fieldNames (IndexInfo (_, _, _, fieldNames)) = fieldNames

    let inline tableName (IndexInfo (_, _, TableInfo (tableName, _, _), _)) = tableName

    let inline schema (IndexInfo (_, _, TableInfo (_, schema, _), _)) = schema

    let inline tableFileName (IndexInfo (_, _, TableInfo (_, _, tableFileName), _)) = tableFileName

    let inline newIndexInfo indexName indexType tableInfo fieldNames =
        IndexInfo(indexName, indexType, tableInfo, fieldNames)
