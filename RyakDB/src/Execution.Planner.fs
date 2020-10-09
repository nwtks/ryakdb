module RyakDB.Execution.Planner

open RyakDB.DataType
open RyakDB.Table
open RyakDB.Index
open RyakDB.Query.Predicate
open RyakDB.Sql.Parse
open RyakDB.Transaction
open RyakDB.Catalog.CatalogService
open RyakDB.Execution.Scan
open RyakDB.Execution.Plan
open RyakDB.Execution.MergeSort

type Planner =
    { CreateQueryPlan: Transaction -> string -> Plan
      ExecuteUpdate: Transaction -> string -> int }

type QueryPlanner =
    { CreatePlan: Transaction -> QueryData -> Plan }

type UpdatePlanner =
    { ExecuteInsert: Transaction -> string -> string list -> DbConstant list -> int
      ExecuteDelete: Transaction -> string -> Predicate -> int
      ExecuteModify: Transaction -> string -> Predicate -> Map<string, Expression> -> int
      ExecuteCreateTable: Transaction -> string -> Schema -> int
      ExecuteDropTable: Transaction -> string -> int
      ExecuteCreateIndex: Transaction -> string -> IndexType -> string -> string list -> int
      ExecuteDropIndex: Transaction -> string -> int
      ExecuteCreateView: Transaction -> string -> string -> int
      ExecuteDropView: Transaction -> string -> int }

module Planner =
    let createQueryPlan queryPlanner tx cmd =
        Parser.queryCommand cmd
        |> queryPlanner.CreatePlan tx

    let executeUpdate updatePlanner tx cmd =
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

let newPlanner queryPlanner updatePlanner =
    { CreateQueryPlan = Planner.createQueryPlan queryPlanner
      ExecuteUpdate = Planner.executeUpdate updatePlanner }

module QueryPlanner =
    let rec createPlan fileService
                       bufferPool
                       catalogService
                       tx
                       (QueryData (projectionFields, tables, predicate, groupFields, aggregationFns, sortFields))
                       =
        let newSortScan =
            MergeSort.newSortScan fileService bufferPool tx

        tables
        |> List.map (fun tblname ->
            match catalogService.GetViewDef tx tblname with
            | Some viewDef ->
                Parser.queryCommand viewDef
                |> createPlan fileService bufferPool catalogService tx
            | _ ->
                match catalogService.GetTableInfo tx tblname with
                | Some ti -> Plan.newTablePlan tx ti
                | _ -> failwith ("Not found table" + tblname))
        |> List.reduce Plan.newProductPlan
        |> Plan.newSelectPlan predicate
        |> Plan.newGroupByPlan newSortScan groupFields aggregationFns
        |> Plan.newProjectPlan projectionFields
        |> Plan.newSortPlan newSortScan sortFields

let newQueryPlanner fileService bufferPool catalogService =
    { CreatePlan = QueryPlanner.createPlan fileService bufferPool catalogService }

module UpdatePlanner =
    let executeInsert fileService catalogService tx tableName fieldNames values =
        let scan =
            match catalogService.GetTableInfo tx tableName with
            | Some ti -> Plan.newTablePlan tx ti
            | _ -> failwith ("Not found table" + tableName)
            |> Plan.openScan fileService

        scan.Insert()
        List.zip fieldNames values
        |> List.iter (fun (field, value) -> scan.SetVal field value)
        scan.Close()
        1

    let executeDelete fileService catalogService tx tableName predicate =
        let scan =
            match catalogService.GetTableInfo tx tableName with
            | Some ti -> Plan.newTablePlan tx ti
            | _ -> failwith ("Not found table" + tableName)
            |> Plan.newSelectPlan predicate
            |> Plan.openScan fileService

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

    let executeModify fileService catalogService tx tableName predicate fieldValues =
        let scan =
            match catalogService.GetTableInfo tx tableName with
            | Some ti -> Plan.newTablePlan tx ti
            | _ -> failwith ("Not found table" + tableName)
            |> Plan.newSelectPlan predicate
            |> Plan.openScan fileService

        let rec modifyAll i =
            if scan.Next() then
                fieldValues
                |> Map.iter (fun f e -> Expression.evaluate scan.GetVal e |> scan.SetVal f)
                modifyAll (i + 1)
            else
                i

        scan.BeforeFirst()
        let count = modifyAll 0
        scan.Close()
        count

    let executeCreateTable catalogService tx tableName schema =
        catalogService.CreateTable tx tableName schema
        0

    let executeDropTable catalogService tx tableName =
        catalogService.DropTable tx tableName
        0

    let executeCreateIndex catalogService tx indexName indexType tableName fieldNames =
        catalogService.CreateIndex tx indexName indexType tableName fieldNames
        0

    let executeDropIndex catalogService tx indexName =
        catalogService.DropIndex tx indexName
        0

    let executeCreateView catalogService tx viewName viewDef =
        catalogService.CreateView tx viewName viewDef
        0

    let executeDropView catalogService tx viewName =
        catalogService.DropView tx viewName
        0

let newUpdatePlanner fileService catalogService =
    { ExecuteInsert = UpdatePlanner.executeInsert fileService catalogService
      ExecuteDelete = UpdatePlanner.executeDelete fileService catalogService
      ExecuteModify = UpdatePlanner.executeModify fileService catalogService
      ExecuteCreateTable = UpdatePlanner.executeCreateTable catalogService
      ExecuteDropTable = UpdatePlanner.executeDropTable catalogService
      ExecuteCreateIndex = UpdatePlanner.executeCreateIndex catalogService
      ExecuteDropIndex = UpdatePlanner.executeDropIndex catalogService
      ExecuteCreateView = UpdatePlanner.executeCreateView catalogService
      ExecuteDropView = UpdatePlanner.executeDropView catalogService }
