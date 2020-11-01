module RyakDB.Execution.Planner

open RyakDB.Sql.Parse
open RyakDB.Transaction
open RyakDB.Execution.Plan
open RyakDB.Execution.QueryPlanner
open RyakDB.Execution.UpdatePlanner

type Planner =
    { CreateQueryPlan: Transaction -> string -> Plan
      ExecuteUpdate: Transaction -> string -> int }

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
