namespace RyakDB.Storage.Table

open RyakDB.Sql.Type
open RyakDB.Storage.Type
open RyakDB.Storage.File
open RyakDB.Storage.Record

module TableInfo =
    let openFile (fileMgr: FileManager) (tableInfo: TableInfo) (tx: Transaction) doLog =
        RecordFile.newRecordFile fileMgr tx doLog tableInfo

    let newTableInfo (fileMgr: FileManager) tableName schema =
        let mutable tableInfo = None

        let self =
            { TableName = tableName
              Schema = schema
              FileName = tableName + ".tbl"
              OpenFile = fun tx doLog -> openFile fileMgr (tableInfo |> Option.get) tx doLog }

        tableInfo <- Some self
        self
