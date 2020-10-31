module RyakDB.Execution.UpdatePlanner

open RyakDB.DataType
open RyakDB.Table
open RyakDB.Index
open RyakDB.Query.Predicate
open RyakDB.Transaction
open RyakDB.Catalog.CatalogService
open RyakDB.Execution.Scan
open RyakDB.Execution.Plan
open RyakDB.Execution.QueryPlanner

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

module UpdatePlanner =
    let executeInsert fileService catalogService tx tableName fieldNames values =
        let fieldValues = List.zip fieldNames values |> Map.ofList

        let rid =
            using
                (match catalogService.GetTableInfo tx tableName with
                 | Some ti -> Plan.newTablePlan fileService tx ti
                 | _ -> failwith ("Not found table:" + tableName)
                 |> Plan.openScan) (fun scan ->
                scan.Insert()
                fieldNames
                |> List.iter (fun field -> scan.SetVal field fieldValues.[field])
                scan.GetRecordId())

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
                        let nextScan = selectPlan |> Plan.openScan
                        nextScan.BeforeFirst()
                        nextScan
                    else
                        scan

                deleteAll indexInfos selectPlan useIndex nextScan (i + 1)
            else
                scan, i

        let newIndexSelecPlan =
            QueryPlanner.makeIndexSelectPlan fileService catalogService tx tableName predicate []

        let selectPlan =
            match catalogService.GetTableInfo tx tableName with
            | Some ti -> Plan.newTablePlan fileService tx ti
            | _ -> failwith ("Not found table:" + tableName)
            |> Option.defaultValue id newIndexSelecPlan
            |> Plan.pipeSelectPlan predicate

        let scan = selectPlan |> Plan.openScan
        scan.BeforeFirst()

        let scan, count =
            deleteAll
                (catalogService.GetIndexInfosByTable tx tableName)
                selectPlan
                (Option.isSome newIndexSelecPlan)
                scan
                0

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
                        |> List.map (fun f -> if oldValues |> Map.containsKey f then oldValues.[f] else scan.GetVal f)
                        |> SearchKey.newSearchKey

                    index.Delete true oldKey rid

                    let newKey =
                        IndexInfo.fieldNames ii
                        |> List.map (fun f -> if newValues |> Map.containsKey f then newValues.[f] else scan.GetVal f)
                        |> SearchKey.newSearchKey

                    index.Insert true newKey rid)

                modifyAll indexInfos scan (i + 1)
            else
                i

        let newIndexSelecPlan =
            QueryPlanner.makeIndexSelectPlan
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
        modifyAll (catalogService.GetIndexInfosByTable tx tableName) scan 0

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
