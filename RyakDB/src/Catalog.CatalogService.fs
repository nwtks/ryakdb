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
      GetIndexInfosByField: Transaction -> string -> string -> IndexInfo list
      GetIndexedFields: Transaction -> string -> string list
      CreateView: Transaction -> string -> string -> unit
      DropView: Transaction -> string -> unit
      GetViewDef: Transaction -> string -> string option
      GetViewNamesByTable: Transaction -> string -> string list
      InitCatalogService: Transaction -> unit }

let newCatalogService fileService =
    let tableService: TableCatalogService = newTableCatalogService fileService

    let indexService: IndexCatalogService =
        newIndexCatalogService fileService tableService

    let viewService: ViewCatalogService =
        newViewCatalogService fileService tableService

    { CreateTable = tableService.CreateTable
      DropTable =
          fun tx tableName ->
              indexService.GetIndexedFields tx tableName
              |> List.collect (indexService.GetIndexInfosByField tx tableName)
              |> List.map (IndexInfo.indexName)
              |> List.iter (indexService.DropIndex tx)
              viewService.GetViewNamesByTable tx tableName
              |> List.iter (viewService.DropView tx)
              tableService.DropTable tx tableName
      GetTableInfo = tableService.GetTableInfo
      CreateIndex = indexService.CreateIndex
      DropIndex = indexService.DropIndex
      GetIndexInfoByName = indexService.GetIndexInfoByName
      GetIndexInfosByField = indexService.GetIndexInfosByField
      GetIndexedFields = indexService.GetIndexedFields
      CreateView = viewService.CreateView
      DropView = viewService.DropView
      GetViewDef = viewService.GetViewDef
      GetViewNamesByTable = viewService.GetViewNamesByTable
      InitCatalogService =
          fun tx ->
              tableService.InitTableCatalogService tx
              indexService.InitIndexCatalogService tx
              viewService.InitViewCatalogService tx }
