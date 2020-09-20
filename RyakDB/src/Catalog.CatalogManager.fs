module RyakDB.Catalog.CatalogManager

open RyakDB.Table
open RyakDB.Index
open RyakDB.Transaction
open RyakDB.Catalog.TableManager
open RyakDB.Catalog.IndexManager
open RyakDB.Catalog.ViewManager

type CatalogManager =
    { CreateTable: Transaction -> string -> Schema -> unit
      DropTable: Transaction -> string -> unit
      GetTableInfo: Transaction -> string -> TableInfo option
      CreateIndex: Transaction -> string -> IndexType -> string -> string list -> unit
      DropIndex: Transaction -> string -> unit
      GetIndexInfoByName: Transaction -> string -> IndexInfo option
      GetIndexInfoByField: Transaction -> string -> string -> IndexInfo list
      GetIndexedFields: Transaction -> string -> string list
      CreateView: Transaction -> string -> string -> unit
      DropView: Transaction -> string -> unit
      GetViewDef: Transaction -> string -> string option
      GetViewNamesByTable: Transaction -> string -> string list
      InitCatalogManager: Transaction -> unit }

let newCatalogManager fileMgr =
    let tableManager: TableManager = newTableManager fileMgr
    let indexManager: IndexManager = newIndexManager fileMgr tableManager
    let viewManager: ViewManager = newViewManager fileMgr tableManager

    { CreateTable = tableManager.CreateTable
      DropTable =
          fun tx tableName ->
              tableManager.DropTable tx tableName
              indexManager.GetIndexedFields tx tableName
              |> List.collect (indexManager.GetIndexInfoByField tx tableName)
              |> List.iter (fun ii -> indexManager.DropIndex tx ii.IndexName)
              viewManager.GetViewNamesByTable tx tableName
              |> List.iter (viewManager.DropView tx)
      GetTableInfo = tableManager.GetTableInfo
      CreateIndex = indexManager.CreateIndex
      DropIndex = indexManager.DropIndex
      GetIndexInfoByName = indexManager.GetIndexInfoByName
      GetIndexInfoByField = indexManager.GetIndexInfoByField
      GetIndexedFields = indexManager.GetIndexedFields
      CreateView = viewManager.CreateView
      DropView = viewManager.DropView
      GetViewDef = viewManager.GetViewDef
      GetViewNamesByTable = viewManager.GetViewNamesByTable
      InitCatalogManager =
          fun tx ->
              tableManager.InitTableManager tx
              indexManager.InitIndexManager tx
              viewManager.InitViewManager tx }
