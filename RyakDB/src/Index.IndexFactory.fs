module RyakDB.Index.IndexFactory

open RyakDB.Index
open RyakDB.Index.BTreeIndex
open RyakDB.Index.HashIndex
open RyakDB.Transaction

let inline newIndex fileService tx indexInfo =
    let keyType =
        IndexInfo.fieldNames indexInfo
        |> SearchKeyType.newSearchKeyType (IndexInfo.schema indexInfo)

    match IndexInfo.indexType indexInfo with
    | Hash bucketsCount ->
        newHashIndex fileService tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly indexInfo keyType bucketsCount
    | BTree -> newBTreeIndex fileService tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly indexInfo keyType
