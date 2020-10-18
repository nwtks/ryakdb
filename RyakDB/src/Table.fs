namespace RyakDB.Table

open RyakDB.DataType
open RyakDB.Storage

type Schema =
    { AddField: string -> DbType -> unit
      Add: string -> Schema -> unit
      AddAll: Schema -> unit
      Fields: unit -> string list
      HasField: string -> bool
      DbType: string -> DbType }

type TableInfo = TableInfo of tableName: string * schema: Schema * fileName: string

type RecordId = RecordId of slotNo: int32 * blockId: BlockId

module Schema =
    let addField fieldTypes fieldName dbType = Map.add fieldName dbType fieldTypes

    let add fieldTypes fieldName schema =
        Map.add fieldName (schema.DbType fieldName) fieldTypes

    let addAll fieldTypes schema =
        schema.Fields()
        |> List.fold (fun fts f -> add fts f schema) fieldTypes

    let fields fieldTypes = fieldTypes |> Map.toList |> List.map fst

    let hasField fieldTypes fieldName = Map.containsKey fieldName fieldTypes

    let dbType fieldTypes fieldName = Map.find fieldName fieldTypes

    let newSchema () =
        let mutable fieldTypes = Map.empty

        { AddField = fun fieldName dbType -> fieldTypes <- addField fieldTypes fieldName dbType
          Add = fun fieldName schema -> fieldTypes <- add fieldTypes fieldName schema
          AddAll = fun schema -> fieldTypes <- addAll fieldTypes schema
          Fields = fun () -> fields fieldTypes
          HasField = fun fieldName -> hasField fieldTypes fieldName
          DbType = fun fieldName -> dbType fieldTypes fieldName }

module TableInfo =
    let inline tableName (TableInfo (tableName, _, _)) = tableName

    let inline schema (TableInfo (_, schema, _)) = schema

    let inline tableFileName (TableInfo (_, _, tableFileName)) = tableFileName

    let inline newTableInfo tableName schema =
        TableInfo(tableName, schema, tableName + ".tbl")

module RecordId =
    let SlotNoSize = 4

    let inline blockId (RecordId (_, blockId)) = blockId

    let inline slotNo (RecordId (slotNo, _)) = slotNo

    let inline fileName (RecordId (_, BlockId (fileName, _))) = fileName

    let inline blockNo (RecordId (_, BlockId (_, blockNo))) = blockNo

    let inline newRecordId slotNo blockId = RecordId(slotNo, blockId)

    let inline newBlockRecordId slotNo fileName blockNo =
        RecordId(slotNo, BlockId.newBlockId fileName blockNo)
