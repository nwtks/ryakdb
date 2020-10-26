module RyakDB.Catalog.IndexCatalogService

open RyakDB.DataType
open RyakDB.Table
open RyakDB.Index
open RyakDB.Table.TableFile
open RyakDB.Transaction
open RyakDB.Catalog.TableCatalogService

type IndexCatalogService =
    { CreateIndex: Transaction -> string -> IndexType -> string -> string list -> unit
      DropIndex: Transaction -> string -> unit
      GetIndexInfoByName: Transaction -> string -> IndexInfo option
      GetIndexInfosByTable: Transaction -> string -> IndexInfo list
      InitIndexCatalogService: Transaction -> unit }

module IndexCatalogService =
    let Icat = "cat_idx"
    let IcatIdxName = "idx_name"
    let IcatTblName = "tbl_name"
    let IcatIdxType = "idx_type"

    let Kcat = "cat_idx_key"
    let KcatIdxName = "idx_name"
    let KcatKeyName = "key_name"

    let createIcat tableService tx =
        let icatSchema = Schema.newSchema ()

        VarcharDbType TableCatalogService.MaxName
        |> icatSchema.AddField IcatIdxName

        VarcharDbType TableCatalogService.MaxName
        |> icatSchema.AddField IcatTblName

        icatSchema.AddField IcatIdxType IntDbType

        tableService.CreateTable tx Icat icatSchema

    let createKcat tableService tx =
        let kcatSchema = Schema.newSchema ()

        VarcharDbType TableCatalogService.MaxName
        |> kcatSchema.AddField KcatIdxName

        VarcharDbType TableCatalogService.MaxName
        |> kcatSchema.AddField KcatKeyName

        tableService.CreateTable tx Kcat kcatSchema

    let readInfo tableService tx tf =
        match tf.GetVal IcatTblName
              |> DbConstant.toString
              |> tableService.GetTableInfo tx with
        | Some tblInfo ->
            let indexName =
                tf.GetVal IcatIdxName |> DbConstant.toString

            let indexTypeNum =
                tf.GetVal IcatIdxType |> DbConstant.toInt

            let indeType =
                match indexTypeNum with
                | 1 -> Hash
                | 2 -> BTree
                | _ ->
                    failwith
                        ("Not supported index type:"
                         + indexTypeNum.ToString())

            (indexName, indeType, tblInfo) |> Some
        | _ -> None

    let rec findIcatfileByIndexName tableService tx tf indexName =
        if tf.Next() then
            let info =
                if tf.GetVal IcatIdxName
                   |> DbConstant.toString = indexName then
                    readInfo tableService tx tf
                else
                    None

            if info |> Option.isSome
            then info
            else findIcatfileByIndexName tableService tx tf indexName
        else
            None

    let findIcatByIndexName fileService tableService tx indexName =
        tableService.GetTableInfo tx Icat
        |> Option.bind (fun ti ->
            use tf =
                newTableFile fileService tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true ti

            tf.BeforeFirst()
            findIcatfileByIndexName tableService tx tf indexName)

    let rec findIcatfileByTableName tableService tx tf tableName indexes =
        if tf.Next() then
            if tf.GetVal IcatTblName
               |> DbConstant.toString = tableName then
                readInfo tableService tx tf
                |> Option.map (fun i -> i :: indexes)
                |> Option.defaultValue indexes
            else
                indexes
            |> findIcatfileByTableName tableService tx tf tableName
        else
            indexes

    let findIcatByTableName fileService tableService tx tableName =
        tableService.GetTableInfo tx Icat
        |> Option.map (fun ti ->
            use tf =
                newTableFile fileService tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true ti

            tf.BeforeFirst()
            findIcatfileByTableName tableService tx tf tableName []
            |> List.rev)
        |> Option.defaultValue []

    let rec findKcatfile tf indexName fields =
        if tf.Next() then
            if tf.GetVal KcatIdxName
               |> DbConstant.toString = indexName then
                (tf.GetVal KcatKeyName |> DbConstant.toString)
                :: fields
            else
                fields
            |> findKcatfile tf indexName
        else
            fields

    let findFields fileService tableService tx indexName =
        tableService.GetTableInfo tx Kcat
        |> Option.map (fun ti ->
            use tf =
                newTableFile fileService tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true ti

            tf.BeforeFirst()
            findKcatfile tf indexName [] |> List.rev)
        |> Option.defaultValue []

    let createIndex fileService tableService tx indexName indexType tableName fields =
        let createIcatfile tableService =
            tableService.GetTableInfo tx Icat
            |> Option.iter (fun ti ->
                use tf =
                    newTableFile fileService tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true ti

                tf.Insert()

                DbConstant.newVarchar indexName
                |> tf.SetVal IcatIdxName

                DbConstant.newVarchar tableName
                |> tf.SetVal IcatTblName

                match indexType with
                | Hash -> 1
                | BTree -> 2
                |> IntDbConstant
                |> tf.SetVal IcatIdxType)

        let createKcatfile tableService =
            tableService.GetTableInfo tx Kcat
            |> Option.iter (fun ti ->
                use tf =
                    newTableFile fileService tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true ti

                fields
                |> List.iter (fun field ->
                    tf.Insert()

                    DbConstant.newVarchar indexName
                    |> tf.SetVal KcatIdxName

                    DbConstant.newVarchar field
                    |> tf.SetVal KcatKeyName))

        createIcatfile tableService
        createKcatfile tableService

    let dropIndex fileService tableService tx indexName =
        let rec deleteIcatfile tf indexName =
            if tf.Next() then
                if tf.GetVal IcatIdxName
                   |> DbConstant.toString = indexName then
                    tf.Delete()
                deleteIcatfile tf indexName

        let dropIcat tableService =
            tableService.GetTableInfo tx Icat
            |> Option.iter (fun ti ->
                use tf =
                    newTableFile fileService tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true ti

                tf.BeforeFirst()
                deleteIcatfile tf indexName)

        let rec deleteKcatfile tf indexName =
            if tf.Next() then
                if tf.GetVal KcatIdxName
                   |> DbConstant.toString = indexName then
                    tf.Delete()
                deleteKcatfile tf indexName

        let dropKcat tableService =
            tableService.GetTableInfo tx Kcat
            |> Option.iter (fun ti ->
                use tf =
                    newTableFile fileService tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true ti

                tf.BeforeFirst()
                deleteKcatfile tf indexName)

        dropIcat tableService
        dropKcat tableService

    let getIndexInfoByName fileService tableService tx indexName =
        findIcatByIndexName fileService tableService tx indexName
        |> Option.map (fun (idxName, idxType, tblInfo) ->
            findFields fileService tableService tx indexName
            |> IndexInfo.newIndexInfo idxName idxType tblInfo)

    let getIndexInfosByTable fileService tableService tx tableName =
        findIcatByTableName fileService tableService tx tableName
        |> List.map (fun (idxName, idxType, tblInfo) ->
            findFields fileService tableService tx idxName
            |> IndexInfo.newIndexInfo idxName idxType tblInfo)

    let initIndexCatalogService tableService tx =
        createIcat tableService tx
        createKcat tableService tx

let newIndexCatalogService fileService tableService =
    { CreateIndex = IndexCatalogService.createIndex fileService tableService
      DropIndex = IndexCatalogService.dropIndex fileService tableService
      GetIndexInfoByName = IndexCatalogService.getIndexInfoByName fileService tableService
      GetIndexInfosByTable = IndexCatalogService.getIndexInfosByTable fileService tableService
      InitIndexCatalogService = IndexCatalogService.initIndexCatalogService tableService }
