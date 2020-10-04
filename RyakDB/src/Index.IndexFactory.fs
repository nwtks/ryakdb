module RyakDB.Index.IndexFactory

open RyakDB.Index
open RyakDB.Index.BTreeIndex
open RyakDB.Index.HashIndex
open RyakDB.Transaction

let inline newIndex fileMgr tx indexInfo =
    let keyType =
        SearchKeyType.newSearchKeyType indexInfo.TableInfo.Schema indexInfo.FieldNames

    match indexInfo.IndexType with
    | Hash -> newHashIndex fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly indexInfo keyType 1000
    | BTree -> newBTreeIndex fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly indexInfo keyType
