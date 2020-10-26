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
    let add fieldName schema =
        schema.DbType fieldName |> Map.add fieldName

    let addAll schema fieldTypes =
        schema.Fields()
        |> List.fold (fun fts f -> add f schema fts) fieldTypes

    let newSchema () =
        let mutable fieldTypes = Map.empty

        { AddField = fun fieldName dbType -> fieldTypes <- fieldTypes |> Map.add fieldName dbType
          Add = fun fieldName schema -> fieldTypes <- fieldTypes |> add fieldName schema
          AddAll = fun schema -> fieldTypes <- fieldTypes |> addAll schema
          Fields = fun () -> fieldTypes |> Map.toList |> List.map fst
          HasField = fun fieldName -> fieldTypes |> Map.containsKey fieldName
          DbType = fun fieldName -> fieldTypes |> Map.find fieldName }

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
