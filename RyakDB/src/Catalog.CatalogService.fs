module RyakDB.Catalog.CatalogService

open RyakDB.Table
open RyakDB.Index
open RyakDB.Transaction
open RyakDB.Catalog.TableCatalogService
open RyakDB.Catalog.IndexCatalogService
open RyakDB.Catalog.ViewCatalogService

type CatalogService =
    { CreateTable: Transaction -> string -> Schema -> unit
      DropTable: Transaction -> string -> unit
      GetTableInfo: Transaction -> string -> TableInfo option
      CreateIndex: Transaction -> string -> IndexType -> string -> string list -> unit
      DropIndex: Transaction -> string -> unit
      GetIndexInfoByName: Transaction -> string -> IndexInfo option
      GetIndexInfosByTable: Transaction -> string -> IndexInfo list
      CreateView: Transaction -> string -> string -> unit
      DropView: Transaction -> string -> unit
      GetViewDef: Transaction -> string -> string option
      GetViewNamesByTable: Transaction -> string -> string list
      InitCatalogService: Transaction -> unit }

let newCatalogService (tableService: TableCatalogService)
                      (indexService: IndexCatalogService)
                      (viewService: ViewCatalogService)
                      =
    { CreateTable = tableService.CreateTable
      DropTable =
          fun tx tableName ->
              indexService.GetIndexInfosByTable tx tableName
              |> List.map (IndexInfo.indexName)
              |> List.iter (indexService.DropIndex tx)
              viewService.GetViewNamesByTable tx tableName
              |> List.iter (viewService.DropView tx)
              tableService.DropTable tx tableName
      GetTableInfo = tableService.GetTableInfo
      CreateIndex = indexService.CreateIndex
      DropIndex = indexService.DropIndex
      GetIndexInfoByName = indexService.GetIndexInfoByName
      GetIndexInfosByTable = indexService.GetIndexInfosByTable
      CreateView = viewService.CreateView
      DropView = viewService.DropView
      GetViewDef = viewService.GetViewDef
      GetViewNamesByTable = viewService.GetViewNamesByTable
      InitCatalogService =
          fun tx ->
              tableService.InitTableCatalogService tx
              indexService.InitIndexCatalogService tx
              viewService.InitViewCatalogService tx }
