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
      BetweenMinAndMax: SearchKey -> bool
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
    let inline newSearchRangeBySearchKey searchKey =
        let (SearchKey searchKeyConstants) = searchKey

        { Size = fun () -> List.length searchKeyConstants
          IsValid = fun () -> true
          GetMin = fun () -> searchKey
          GetMax = fun () -> searchKey
          MatchsKey = fun key -> key = searchKey
          BetweenMinAndMax = fun key -> key = searchKey
          IsSingleValue = fun () -> true
          ToSearchKey = fun () -> searchKey }

module IndexInfo =
    let inline newIndexInfo indexName indexType tableInfo fieldNames =
        { IndexName = indexName
          IndexType = indexType
          TableInfo = tableInfo
          FieldNames = fieldNames }
