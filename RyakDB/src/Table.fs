namespace RyakDB.Table

open RyakDB.DataType

type Schema =
    { AddField: string -> SqlType -> unit
      Add: string -> Schema -> unit
      AddAll: Schema -> unit
      Fields: unit -> string list
      HasField: string -> bool
      SqlType: string -> SqlType }

type TableInfo =
    { TableName: string
      Schema: Schema
      FileName: string }

module Schema =
    let addField (fieldTypes: Map<string, SqlType>) fieldName dbType = fieldTypes.Add(fieldName, dbType)

    let add (fieldTypes: Map<string, SqlType>) fieldName schema =
        fieldTypes.Add(fieldName, schema.SqlType fieldName)

    let addAll fieldTypes schema =
        schema.Fields()
        |> List.fold (fun st f -> add st f schema) fieldTypes

    let fields fieldTypes =
        fieldTypes
        |> Map.toList
        |> List.map (fun (k, _) -> k)

    let hasField (fieldTypes: Map<string, SqlType>) fieldName = fieldTypes.ContainsKey fieldName
    let sqlType (fieldTypes: Map<string, SqlType>) fieldName = fieldTypes.[fieldName]

    let newSchema () =
        let mutable fieldTypes = Map.empty

        { AddField = fun fieldName dbType -> fieldTypes <- addField fieldTypes fieldName dbType
          Add = fun fieldName schema -> fieldTypes <- add fieldTypes fieldName schema
          AddAll = fun schema -> fieldTypes <- addAll fieldTypes schema
          Fields = fun () -> fields fieldTypes
          HasField = fun fieldName -> hasField fieldTypes fieldName
          SqlType = fun fieldName -> sqlType fieldTypes fieldName }

module TableInfo =
    let newTableInfo tableName schema =
        { TableName = tableName
          Schema = schema
          FileName = tableName + ".tbl" }
