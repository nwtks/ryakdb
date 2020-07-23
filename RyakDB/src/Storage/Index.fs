namespace RyakDB.Storage.Index

open RyakDB.Storage.Type
open RyakDB.Storage.Record

module SearchKey =
    let newSearchKey constants = SearchKey constants

module SearchKeyType =
    let newSearchKeyType schema indexedFields = SearchKeyType []
    let newSearchKeyTypeByTypes types = SearchKeyType types

module Index =
    let newIndex tx indexInfo keyType =
        { BeforeFirst = fun _ -> ()
          Next = fun () -> false
          GetDataRecordId = fun () -> RecordId.newBlockRecordId -1 "" -1L
          Insert = fun _ _ _ -> ()
          Delete = fun _ _ _ -> ()
          Close = fun () -> ()
          PreLoadToMemory = fun () -> () }

module BTreeDir =
    let insertASlot tx blockId keyType slotId = ()
    let deleteASlot tx blockId keyType slotId = ()

module BTreeLeaf =
    let insertASlot tx blockId keyType slotId = ()
    let deleteASlot tx blockId keyType slotId = ()
