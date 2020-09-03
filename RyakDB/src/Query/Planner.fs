namespace RyakDB.Query.Planner

open RyakDB.Sql.Type
open RyakDB.Sql.Predicate
open RyakDB.Sql.Parse
open RyakDB.Storage.Type
open RyakDB.Storage.File
open RyakDB.Storage.Catalog
open RyakDB.Query.Algebra
open RyakDB.Query.Materialize

type Planner =
    { CreateQueryPlan: Transaction -> string -> Plan
      ExecuteUpdate: Transaction -> string -> int }

type QueryPlanner =
    { CreatePlan: Transaction -> QueryData -> Plan }

type UpdatePlanner =
    { ExecuteInsert: Transaction -> string -> string list -> SqlConstant list -> int
      ExecuteDelete: Transaction -> string -> Predicate -> int
      ExecuteModify: Transaction -> string -> Predicate -> Map<string, Expression> -> int
      ExecuteCreateTable: Transaction -> string -> Schema -> int
      ExecuteDropTable: Transaction -> string -> int
      ExecuteCreateIndex: Transaction -> string -> IndexType -> string -> string list -> int
      ExecuteDropIndex: Transaction -> string -> int
      ExecuteCreateView: Transaction -> string -> string -> int
      ExecuteDropView: Transaction -> string -> int }

module Planner =
    let createQueryPlan queryPlanner (tx: Transaction) cmd =
        Parser.queryCommand cmd
        |> queryPlanner.CreatePlan tx

    let executeUpdate updatePlanner (tx: Transaction) cmd =
        if tx.ReadOnly then failwith "Read only transaction"
        match Parser.updateCommand cmd with
        | InsertData (tableName, fields, values) -> updatePlanner.ExecuteInsert tx tableName fields values
        | DeleteData (tableName, pred) -> updatePlanner.ExecuteDelete tx tableName pred
        | ModifyData (tableName, pred, fields) -> updatePlanner.ExecuteModify tx tableName pred fields
        | CreateTableData (tableName, schema) -> updatePlanner.ExecuteCreateTable tx tableName schema
        | DropTableData (tableName) -> updatePlanner.ExecuteDropTable tx tableName
        | CreateIndexData (indexName, indexType, tableName, fields) ->
            updatePlanner.ExecuteCreateIndex tx indexName indexType tableName fields
        | DropIndexData (indexName) -> updatePlanner.ExecuteDropIndex tx indexName
        | CreateViewData (viewName, viewDef) -> updatePlanner.ExecuteCreateView tx viewName viewDef
        | DropViewData (viewName) -> updatePlanner.ExecuteDropView tx viewName

    let newPlanner (queryPlanner: QueryPlanner) (updatePlanner: UpdatePlanner) =
        { CreateQueryPlan = createQueryPlan queryPlanner
          ExecuteUpdate = executeUpdate updatePlanner }

module QueryPlanner =
    let rec createPlan (fileMgr: FileManager)
                       (catalogMgr: CatalogManager)
                       (tx: Transaction)
                       (QueryData (projectionFields, tables, predicate, groupFields, aggregationFns, sortFields))
                       =
        let newSortScan = Materialize.newSortScan fileMgr tx
        tables
        |> List.map (fun tblname ->
            match catalogMgr.GetViewDef tx tblname with
            | Some (viewDef) ->
                Parser.queryCommand viewDef
                |> createPlan fileMgr catalogMgr tx
            | _ ->
                catalogMgr.GetTableInfo tx tblname
                |> Option.get
                |> Plan.newTablePlan tx)
        |> List.reduce Plan.newProductPlan
        |> Plan.newSelectPlan predicate
        |> Plan.newGroupByPlan newSortScan groupFields aggregationFns
        |> Plan.newProjectPlan projectionFields
        |> Plan.newSortPlan newSortScan sortFields

    let newQueryPlanner (fileMgr: FileManager) (catalogMgr: CatalogManager) =
        { CreatePlan = createPlan fileMgr catalogMgr }

module UpdatePlanner =
    let executeInsert (catalogMgr: CatalogManager) (tx: Transaction) tableName fieldNames values =
        let scan =
            catalogMgr.GetTableInfo tx tableName
            |> Option.get
            |> Plan.newTablePlan tx
            |> Plan.openScan

        scan.Insert()
        List.zip fieldNames values
        |> List.iter (fun (field, value) -> scan.SetVal field value)
        scan.Close()
        1

    let executeDelete (catalogMgr: CatalogManager) (tx: Transaction) tableName (predicate: Predicate) =
        let scan =
            catalogMgr.GetTableInfo tx tableName
            |> Option.get
            |> Plan.newTablePlan tx
            |> Plan.newSelectPlan predicate
            |> Plan.openScan

        let rec deleteAll i =
            if scan.Next() then
                scan.Delete()
                deleteAll (i + 1)
            else
                i

        scan.BeforeFirst()
        let count = deleteAll 0
        scan.Close()
        count

    let executeModify (catalogMgr: CatalogManager) (tx: Transaction) tableName (predicate: Predicate) fieldValues =
        let scan =
            catalogMgr.GetTableInfo tx tableName
            |> Option.get
            |> Plan.newTablePlan tx
            |> Plan.newSelectPlan predicate
            |> Plan.openScan

        let rec modifyAll i =
            if scan.Next() then
                fieldValues
                |> Map.iter (fun f e -> scan.SetVal f (Expression.evaluate scan.GetVal e))
                modifyAll (i + 1)
            else
                i

        scan.BeforeFirst()
        let count = modifyAll 0
        scan.Close()
        count

    let executeCreateTable (catalogMgr: CatalogManager) (tx: Transaction) tableName schema =
        catalogMgr.CreateTable tx tableName schema
        0

    let executeDropTable (catalogMgr: CatalogManager) (tx: Transaction) tableName =
        catalogMgr.DropTable tx tableName
        0

    let executeCreateIndex (catalogMgr: CatalogManager) (tx: Transaction) indexName indexType tableName fieldNames =
        catalogMgr.CreateIndex tx indexName indexType tableName fieldNames
        0

    let executeDropIndex (catalogMgr: CatalogManager) (tx: Transaction) indexName =
        catalogMgr.DropIndex tx indexName
        0

    let executeCreateView (catalogMgr: CatalogManager) (tx: Transaction) viewName viewDef =
        catalogMgr.CreateView tx viewName viewDef
        0

    let executeDropView (catalogMgr: CatalogManager) (tx: Transaction) viewName =
        catalogMgr.DropView tx viewName
        0

    let newUpdatePlanner (catalogMgr: CatalogManager) =
        { ExecuteInsert = executeInsert catalogMgr
          ExecuteDelete = executeDelete catalogMgr
          ExecuteModify = executeModify catalogMgr
          ExecuteCreateTable = executeCreateTable catalogMgr
          ExecuteDropTable = executeDropTable catalogMgr
          ExecuteCreateIndex = executeCreateIndex catalogMgr
          ExecuteDropIndex = executeDropIndex catalogMgr
          ExecuteCreateView = executeCreateView catalogMgr
          ExecuteDropView = executeDropView catalogMgr }
