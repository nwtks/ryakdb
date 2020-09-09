module RyakDB.Index.IndexFactory

open RyakDB.Index
open RyakDB.Index.BTreeIndex
open RyakDB.Index.HashIndex
open RyakDB.Query

module IndexFactory =
    let newIndex fileMgr tx indexInfo =
        let keyType =
            SearchKeyType.newSearchKeyType indexInfo.TableInfo.Schema indexInfo.FieldNames

        match indexInfo.IndexType with
        | IndexType.Hash -> HashIndex.newHashIndex fileMgr tx indexInfo keyType
        | IndexType.BTree -> BTreeIndex.newBTreeIndex tx indexInfo keyType
        | _ -> failwith "Not supported"
