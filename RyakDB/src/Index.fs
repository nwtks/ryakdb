namespace RyakDB.Index

open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Table
open RyakDB.Query

type SearchKey = SearchKey of constants: SqlConstant list

type SearchKeyType = SearchKeyType of types: SqlType list

type SearchRange =
    { Size: unit -> int32
      IsValid: unit -> bool
      GetMin: unit -> SearchKey
      GetMax: unit -> SearchKey
      MatchsKey: SearchKey -> bool
      BetweenMinAndMax: SearchKey -> bool
      IsSingleValue: unit -> bool
      ToSearchKey: unit -> SearchKey }

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
    let newSearchKey constants = SearchKey constants

module SearchKeyType =
    let newSearchKeyTypeByTypes types = SearchKeyType types

    let newSearchKeyType schema indexedFields =
        indexedFields
        |> List.map schema.SqlType
        |> SearchKeyType

module SearchRange =
    let newSearchRangeBySearchKey searchKey =
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
    let newIndexInfo indexName indexType tableInfo fieldNames =
        { IndexName = indexName
          IndexType = indexType
          TableInfo = tableInfo
          FieldNames = fieldNames }
