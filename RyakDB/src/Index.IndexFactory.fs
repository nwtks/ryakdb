module RyakDB.Index.IndexFactory

open RyakDB.Index
open RyakDB.Index.BTreeIndex
open RyakDB.Index.HashIndex
open RyakDB.Transaction

let inline newIndex fileMgr tx indexInfo =
    let keyType =
        SearchKeyType.newSearchKeyType indexInfo.TableInfo.Schema indexInfo.FieldNames

    match indexInfo.IndexType with
    | IndexType.Hash -> newHashIndex fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly indexInfo keyType
    | IndexType.BTree -> newBTreeIndex fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly indexInfo keyType
    | _ ->
        failwith
            ("Not supported index type:"
             + indexInfo.IndexType.ToString())
