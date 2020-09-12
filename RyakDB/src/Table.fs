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

type TableInfo =
    { TableName: string
      Schema: Schema
      FileName: string }

type RecordId = RecordId of slotNo: int32 * blockId: BlockId

module Schema =
    let inline addField (fieldTypes: Map<string, DbType>) fieldName dbType = fieldTypes.Add(fieldName, dbType)

    let inline add (fieldTypes: Map<string, DbType>) fieldName schema =
        fieldTypes.Add(fieldName, schema.DbType fieldName)

    let inline addAll fieldTypes schema =
        schema.Fields()
        |> List.fold (fun st f -> add st f schema) fieldTypes

    let inline fields fieldTypes =
        fieldTypes
        |> Map.toList
        |> List.map (fun (k, _) -> k)

    let inline hasField (fieldTypes: Map<string, DbType>) fieldName = fieldTypes.ContainsKey fieldName

    let inline dbType (fieldTypes: Map<string, DbType>) fieldName = fieldTypes.[fieldName]

    let newSchema () =
        let mutable fieldTypes = Map.empty

        { AddField = fun fieldName dbType -> fieldTypes <- addField fieldTypes fieldName dbType
          Add = fun fieldName schema -> fieldTypes <- add fieldTypes fieldName schema
          AddAll = fun schema -> fieldTypes <- addAll fieldTypes schema
          Fields = fun () -> fields fieldTypes
          HasField = fun fieldName -> hasField fieldTypes fieldName
          DbType = fun fieldName -> dbType fieldTypes fieldName }

module TableInfo =
    let inline newTableInfo tableName schema =
        { TableName = tableName
          Schema = schema
          FileName = tableName + ".tbl" }

module RecordId =
    let SlotNoSize = 4

    let inline newRecordId slotNo blockId = RecordId(slotNo, blockId)

    let inline newBlockRecordId slotNo fileName blockNo =
        RecordId(slotNo, BlockId.newBlockId fileName blockNo)
