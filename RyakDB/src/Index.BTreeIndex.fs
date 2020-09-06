namespace RyakDB.Index.BTreeIndex

open RyakDB.Storage
open RyakDB.Index

module BTreeIndex =
    let newBTreeIndex tx indexInfo keyType =
        { BeforeFirst = fun _ -> ()
          Next = fun () -> false
          GetDataRecordId = fun () -> RecordId.newBlockRecordId -1 "" -1L
          Insert = fun _ _ _ -> ()
          Delete = fun _ _ _ -> ()
          Close = fun () -> ()
          PreLoadToMemory = fun () -> () }
