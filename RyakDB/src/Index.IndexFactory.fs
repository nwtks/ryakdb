module RyakDB.Index.IndexFactory

open RyakDB.Index
open RyakDB.Index.BTreeIndex
open RyakDB.Index.HashIndex

let inline newIndex fileService tx indexInfo =
    let keyType =
        IndexInfo.fieldNames indexInfo
        |> SearchKeyType.newSearchKeyType (IndexInfo.schema indexInfo)

    match IndexInfo.indexType indexInfo with
    | Hash bucketsCount -> newHashIndex fileService tx indexInfo keyType bucketsCount
    | BTree -> newBTreeIndex fileService tx indexInfo keyType
