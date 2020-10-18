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
                | Some ti -> Plan.newTablePlan fileService tx ti
                | _ -> failwith ("Not found table:" + tblname))
        |> List.reduce Plan.pipeProductPlan
        |> Plan.pipeSelectPlan predicate
        |> Plan.pipeGroupByPlan newSortScan groupFields aggregationFns
        |> Plan.pipeProjectPlan projectionFields
        |> Plan.pipeSortPlan newSortScan sortFields

let newQueryPlanner fileService bufferPool catalogService =
    { CreatePlan = QueryPlanner.createPlan fileService bufferPool catalogService }

module IndexSelector =
    let selectIndexSelectPlan fileService tx predicate candidates =
        let selectIndexInfo bestIndex searchRanges indexInfo =
            let indexType = IndexInfo.indexType indexInfo

            let ranges =
                IndexInfo.fieldNames indexInfo
                |> List.map (fun f -> f, Predicate.toConstantRange f predicate)
                |> List.filter (fun (_, range) -> Option.isSome range)
                |> List.map (fun (f, range) -> f, Option.get range)
                |> List.filter (fun (_, range) ->
                    indexType = BTree
                    || indexType = Hash && range.IsConstant())
                |> List.fold (fun ranges (f, range) -> ranges |> Map.add f range) Map.empty

            if Map.count ranges > Map.count searchRanges
            then (Some indexInfo), ranges
            else bestIndex, searchRanges

        match candidates
              |> List.fold (fun (bestIndex, searchRanges) ii -> selectIndexInfo bestIndex searchRanges ii)
                     (None, Map.empty) with
        | (Some bestIndex), searchRanges ->
            Some
                (Plan.pipeIndexSelectPlan
                    fileService
                     tx
                     bestIndex
                     (SearchRange.newSearchRangeByFieldsRanges (IndexInfo.fieldNames bestIndex) searchRanges))
        | _ -> None

    let selectByBestMatchedIndex fileService catalogService tx tableName predicate excludedFields =
        catalogService.GetIndexInfosByTable tx tableName
        |> List.filter (fun ii ->
            not
                (IndexInfo.fieldNames ii
                 |> List.exists (fun f -> List.contains f excludedFields)))
        |> selectIndexSelectPlan fileService tx predicate

module UpdatePlanner =
    let executeInsert fileService catalogService tx tableName fieldNames values =
        let fieldValues = List.zip fieldNames values |> Map.ofList

        use scan =
            match catalogService.GetTableInfo tx tableName with
            | Some ti -> Plan.newTablePlan fileService tx ti
            | _ -> failwith ("Not found table:" + tableName)
            |> Plan.openScan

        scan.Insert()
        fieldNames
        |> List.iter (fun field -> scan.SetVal field fieldValues.[field])
        let rid = scan.GetRecordId()

        catalogService.GetIndexInfosByTable tx tableName
        |> List.iter (fun ii ->
            let key =
                IndexInfo.fieldNames ii
                |> List.map (fun field -> fieldValues.[field])
                |> SearchKey.newSearchKey

            use index = IndexFactory.newIndex fileService tx ii
            index.Insert true key rid)

        1

    let executeDelete fileService catalogService tx tableName predicate =
        let rec deleteAll indexInfos selectPlan useIndex scan i =
            if scan.Next() then
                let rid = scan.GetRecordId()
                indexInfos
                |> List.iter (fun ii ->
                    let key =
                        IndexInfo.fieldNames ii
                        |> List.map scan.GetVal
                        |> SearchKey.newSearchKey

                    use index = IndexFactory.newIndex fileService tx ii
                    index.Delete true key rid)
                scan.Delete()

                let nextScan =
                    if useIndex then
                        scan.Close()
                        let nextScan = Plan.openScan selectPlan
                        nextScan.BeforeFirst()
                        nextScan
                    else
                        scan

                deleteAll indexInfos selectPlan useIndex nextScan (i + 1)
            else
                scan, i

        let indexInfos =
            catalogService.GetIndexInfosByTable tx tableName

        let newIndexSelecPlan =
            IndexSelector.selectByBestMatchedIndex fileService catalogService tx tableName predicate []

        let selectPlan =
            match catalogService.GetTableInfo tx tableName with
            | Some ti -> Plan.newTablePlan fileService tx ti
            | _ -> failwith ("Not found table:" + tableName)
            |> Option.defaultValue id newIndexSelecPlan
            |> Plan.pipeSelectPlan predicate

        let scan = Plan.openScan selectPlan
        scan.BeforeFirst()

        let scan, count =
            deleteAll indexInfos selectPlan (Option.isSome newIndexSelecPlan) scan 0

        scan.Close()
        count

    let executeModify fileService catalogService tx tableName predicate fieldValues =
        let rec modifyAll indexInfos scan i =
            if scan.Next() then
                let oldValues =
                    fieldValues |> Map.map (fun f _ -> scan.GetVal f)

                let newValues =
                    fieldValues
                    |> Map.map (fun _ e -> Expression.evaluate scan.GetVal e)

                newValues |> Map.iter scan.SetVal

                let rid = scan.GetRecordId()
                indexInfos
                |> List.iter (fun ii ->
                    use index = IndexFactory.newIndex fileService tx ii

                    let oldKey =
                        IndexInfo.fieldNames ii
                        |> List.map (fun f -> if oldValues.ContainsKey f then oldValues.[f] else scan.GetVal f)
                        |> SearchKey.newSearchKey

                    index.Delete true oldKey rid

                    let newKey =
                        IndexInfo.fieldNames ii
                        |> List.map (fun f -> if newValues.ContainsKey f then newValues.[f] else scan.GetVal f)
                        |> SearchKey.newSearchKey

                    index.Insert true newKey rid)

                modifyAll indexInfos scan (i + 1)
            else
                i

        let indexInfos =
            catalogService.GetIndexInfosByTable tx tableName

        let newIndexSelecPlan =
            IndexSelector.selectByBestMatchedIndex
                fileService
                catalogService
                tx
                tableName
                predicate
                (fieldValues |> Map.toList |> List.map fst)

        use scan =
            match catalogService.GetTableInfo tx tableName with
            | Some ti -> Plan.newTablePlan fileService tx ti
            | _ -> failwith ("Not found table:" + tableName)
            |> Option.defaultValue id newIndexSelecPlan
            |> Plan.pipeSelectPlan predicate
            |> Plan.openScan

        scan.BeforeFirst()
        modifyAll indexInfos scan 0

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
