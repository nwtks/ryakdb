namespace RyakDB.Index

open RyakDB.DataType
open RyakDB.Table

type SearchKey = SearchKey of constants: DbConstant list

type SearchKeyType = SearchKeyType of types: DbType list

type SearchRange =
    { Size: unit -> int32
      IsValid: unit -> bool
      GetMin: unit -> SearchKey
      GetMax: unit -> SearchKey
      MatchsKey: SearchKey -> bool
      IsSingleValue: unit -> bool
      ToSearchKey: unit -> SearchKey }

type IndexType =
    | Hash = 1
    | BTree = 2

type IndexInfo =
    { IndexName: string
      IndexType: IndexType
      TableInfo: TableInfo
      FieldNames: string list }

type Index =
    { BeforeFirst: SearchRange -> unit
      Next: unit -> bool
      GetDataRecordId: unit -> RecordId
      Insert: bool -> SearchKey -> RecordId -> unit
      Delete: bool -> SearchKey -> RecordId -> unit
      Close: unit -> unit
      PreLoadToMemory: unit -> unit }

module SearchKey =
    let inline newSearchKey constants = SearchKey constants

module SearchKeyType =
    let inline getMin (SearchKeyType types) =
        types
        |> List.map DbType.minValue
        |> SearchKey.newSearchKey

    let inline getMax (SearchKeyType types) =
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

    let getMin ranges =
        ranges
        |> List.map (fun r -> r.Low())
        |> SearchKey.newSearchKey

    let getMax ranges =
        ranges
        |> List.map (fun r -> r.High())
        |> SearchKey.newSearchKey

    let matchsKey ranges key =
        let (SearchKey keyConstants) = key
        List.length ranges = List.length keyConstants
        && List.zip ranges keyConstants
           |> List.forall (fun (r, c) -> r.Contains c)

    let isSingleValue ranges =
        ranges |> List.forall (fun r -> r.IsConstant())

    let toSearchKey ranges =
        ranges
        |> List.map (fun r -> r.ToConstant())
        |> SearchKey.newSearchKey

    let newSearchRangeByRanges (ranges: DbConstantRange list) =
        { Size = fun () -> List.length ranges
          IsValid = fun () -> isValid ranges
          GetMin = fun () -> getMin ranges
          GetMax = fun () -> getMax ranges
          MatchsKey = matchsKey ranges
          IsSingleValue = fun () -> isSingleValue ranges
          ToSearchKey = fun () -> toSearchKey ranges }

    let newSearchRangeBySearchKey searchKey =
        let (SearchKey searchKeyConstants) = searchKey
        searchKeyConstants
        |> List.map (fun c -> DbConstantRange.newConstantRange (DbConstant.dbType c) (Some c) true (Some c) true)
        |> newSearchRangeByRanges

module IndexInfo =
    let inline newIndexInfo indexName indexType tableInfo fieldNames =
        { IndexName = indexName
          IndexType = indexType
          TableInfo = tableInfo
          FieldNames = fieldNames }
