module RyakDB.Execution.Planner

open RyakDB.DataType
open RyakDB.Table
open RyakDB.Index
open RyakDB.Query.Predicate
open RyakDB.Sql.Parse
open RyakDB.Transaction
open RyakDB.Catalog.CatalogManager
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
    let rec createPlan fileMgr
                       catalogMgr
                       tx
                       (QueryData (projectionFields, tables, predicate, groupFields, aggregationFns, sortFields))
                       =
        let newSortScan = MergeSort.newSortScan fileMgr tx
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

let newQueryPlanner fileMgr catalogMgr =
    { CreatePlan = QueryPlanner.createPlan fileMgr catalogMgr }

module UpdatePlanner =
    let executeInsert fileMgr catalogMgr tx tableName fieldNames values =
        let scan =
            catalogMgr.GetTableInfo tx tableName
            |> Option.get
            |> Plan.newTablePlan tx
            |> Plan.openScan fileMgr

        scan.Insert()
        List.zip fieldNames values
        |> List.iter (fun (field, value) -> scan.SetVal field value)
        scan.Close()
        1

    let executeDelete fileMgr catalogMgr tx tableName predicate =
        let scan =
            catalogMgr.GetTableInfo tx tableName
            |> Option.get
            |> Plan.newTablePlan tx
            |> Plan.newSelectPlan predicate
            |> Plan.openScan fileMgr

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

    let executeModify fileMgr catalogMgr tx tableName predicate fieldValues =
        let scan =
            catalogMgr.GetTableInfo tx tableName
            |> Option.get
            |> Plan.newTablePlan tx
            |> Plan.newSelectPlan predicate
            |> Plan.openScan fileMgr

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

    let executeCreateTable catalogMgr tx tableName schema =
        catalogMgr.CreateTable tx tableName schema
        0

    let executeDropTable catalogMgr tx tableName =
        catalogMgr.DropTable tx tableName
        0

    let executeCreateIndex catalogMgr tx indexName indexType tableName fieldNames =
        catalogMgr.CreateIndex tx indexName indexType tableName fieldNames
        0

    let executeDropIndex catalogMgr tx indexName =
        catalogMgr.DropIndex tx indexName
        0

    let executeCreateView catalogMgr tx viewName viewDef =
        catalogMgr.CreateView tx viewName viewDef
        0

    let executeDropView catalogMgr tx viewName =
        catalogMgr.DropView tx viewName
        0

let newUpdatePlanner fileMgr catalogMgr =
    { ExecuteInsert = UpdatePlanner.executeInsert fileMgr catalogMgr
      ExecuteDelete = UpdatePlanner.executeDelete fileMgr catalogMgr
      ExecuteModify = UpdatePlanner.executeModify fileMgr catalogMgr
      ExecuteCreateTable = UpdatePlanner.executeCreateTable catalogMgr
      ExecuteDropTable = UpdatePlanner.executeDropTable catalogMgr
      ExecuteCreateIndex = UpdatePlanner.executeCreateIndex catalogMgr
      ExecuteDropIndex = UpdatePlanner.executeDropIndex catalogMgr
      ExecuteCreateView = UpdatePlanner.executeCreateView catalogMgr
      ExecuteDropView = UpdatePlanner.executeDropView catalogMgr }
