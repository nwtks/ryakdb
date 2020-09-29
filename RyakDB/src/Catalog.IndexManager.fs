module RyakDB.Catalog.IndexManager

open RyakDB.DataType
open RyakDB.Table
open RyakDB.Index
open RyakDB.Table.TableFile
open RyakDB.Transaction
open RyakDB.Catalog.TableManager

type IndexManager =
    { CreateIndex: Transaction -> string -> IndexType -> string -> string list -> unit
      DropIndex: Transaction -> string -> unit
      GetIndexInfoByName: Transaction -> string -> IndexInfo option
      GetIndexInfoByField: Transaction -> string -> string -> IndexInfo list
      GetIndexedFields: Transaction -> string -> string list
      InitIndexManager: Transaction -> unit }

module IndexManager =
    let Icat = "cat_idx"
    let IcatIdxName = "idx_name"
    let IcatTblName = "tbl_name"
    let IcatIdxType = "idx_type"

    let Kcat = "cat_idx_key"
    let KcatIdxName = "idx_name"
    let KcatKeyName = "key_name"

    let readInfo tblMgr tx tf =
        match tf.GetVal IcatTblName
              |> DbConstant.toString
              |> tblMgr.GetTableInfo tx with
        | Some tblInfo ->
            (tf.GetVal IcatIdxName |> DbConstant.toString,
             tf.GetVal IcatIdxType
             |> DbConstant.toInt
             |> enum<IndexType>,
             tblInfo)
            |> Some
        | _ -> None

    let rec findIcatfileByIndexName tblMgr tx (tf: TableFile) indexName =
        if tf.Next() then
            if tf.GetVal IcatIdxName
               |> DbConstant.toString = indexName then
                readInfo tblMgr tx tf
            else
                None
            |> Option.orElseWith (fun () -> findIcatfileByIndexName tblMgr tx tf indexName)
        else
            None

    let findIcatByIndexName fileMgr tblMgr tx indexName =
        tblMgr.GetTableInfo tx Icat
        |> Option.map (newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
        |> Option.bind (fun tf ->
            tf.BeforeFirst()

            let index =
                findIcatfileByIndexName tblMgr tx tf indexName

            tf.Close()
            index)

    let rec findIcatfileByTableName tblMgr tx (tf: TableFile) tableName indexes =
        if tf.Next() then
            if tf.GetVal IcatTblName
               |> DbConstant.toString = tableName then
                readInfo tblMgr tx tf
                |> Option.map (fun i -> i :: indexes)
                |> Option.defaultValue indexes
            else
                indexes
            |> findIcatfileByTableName tblMgr tx tf tableName
        else
            indexes

    let findIcatByTableName fileMgr tblMgr tx tableName =
        tblMgr.GetTableInfo tx Icat
        |> Option.map (newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
        |> Option.map (fun tf ->
            tf.BeforeFirst()

            let indexes =
                findIcatfileByTableName tblMgr tx tf tableName []

            tf.Close()
            indexes)
        |> Option.defaultValue []

    let rec findKcatfile (tf: TableFile) indexName fields =
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

    let findFields fileMgr tblMgr tx indexName =
        tblMgr.GetTableInfo tx Kcat
        |> Option.map (newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
        |> Option.map (fun tf ->
            tf.BeforeFirst()
            let fields = findKcatfile tf indexName []
            tf.Close()
            fields)
        |> Option.defaultValue []

    let createIndex fileMgr tblMgr tx indexName indexType tableName fields =
        let createIcatfile tblMgr =
            tblMgr.GetTableInfo tx Icat
            |> Option.map (newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
            |> Option.iter (fun tf ->
                tf.Insert()
                DbConstant.newVarchar indexName
                |> tf.SetVal IcatIdxName
                DbConstant.newVarchar tableName
                |> tf.SetVal IcatTblName
                IntDbConstant(int32 indexType)
                |> tf.SetVal IcatIdxType
                tf.Close())

        let createKcatfile tblMgr =
            tblMgr.GetTableInfo tx Kcat
            |> Option.map (newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
            |> Option.iter (fun tf ->
                fields
                |> List.iter (fun field ->
                    tf.Insert()
                    DbConstant.newVarchar indexName
                    |> tf.SetVal KcatIdxName
                    DbConstant.newVarchar field
                    |> tf.SetVal KcatKeyName
                    tf.Close()))

        createIcatfile tblMgr
        createKcatfile tblMgr

    let dropIndex fileMgr tblMgr tx indexName =
        let rec deleteIcatfile (tf: TableFile) indexName =
            if tf.Next() then
                if tf.GetVal IcatIdxName
                   |> DbConstant.toString = indexName then
                    tf.Delete()
                deleteIcatfile tf indexName

        let dropIcat tblMgr =
            tblMgr.GetTableInfo tx Icat
            |> Option.map (newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
            |> Option.iter (fun tf ->
                tf.BeforeFirst()
                deleteIcatfile tf indexName
                tf.Close())

        let rec deleteKcatfile (tf: TableFile) indexName =
            if tf.Next() then
                if tf.GetVal KcatIdxName
                   |> DbConstant.toString = indexName then
                    tf.Delete()
                deleteKcatfile tf indexName

        let dropKcat tblMgr =
            tblMgr.GetTableInfo tx Kcat
            |> Option.map (newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
            |> Option.iter (fun tf ->
                tf.BeforeFirst()
                deleteKcatfile tf indexName
                tf.Close())

        dropIcat tblMgr
        dropKcat tblMgr

    let getIndexInfoByName fileMgr tblMgr tx indexName =
        findIcatByIndexName fileMgr tblMgr tx indexName
        |> Option.map (fun (idxName, idxType, tblInfo) ->
            findFields fileMgr tblMgr tx indexName
            |> IndexInfo.newIndexInfo idxName idxType tblInfo)

    let getIndexInfoByField fileMgr tblMgr tx tableName field =
        findIcatByTableName fileMgr tblMgr tx tableName
        |> List.map (fun (idxName, idxType, tblInfo) -> idxName, idxType, tblInfo, findFields fileMgr tblMgr tx idxName)
        |> List.filter (fun (_, _, _, fields) -> fields |> List.contains field)
        |> List.map (fun (idxName, idxType, tblInfo, fields) -> IndexInfo.newIndexInfo idxName idxType tblInfo fields)

    let getIndexedFields fileMgr tblMgr tx tableName =
        findIcatByTableName fileMgr tblMgr tx tableName
        |> List.map (fun (idxName, _, _) -> findFields fileMgr tblMgr tx idxName)
        |> List.collect id
        |> Set.ofList
        |> Set.toList

    let initIndexManager tblMgr tx =
        let createIcat tblMgr =
            let icatSchema = Schema.newSchema ()
            VarcharDbType TableManager.MaxName
            |> icatSchema.AddField IcatIdxName
            VarcharDbType TableManager.MaxName
            |> icatSchema.AddField IcatTblName
            icatSchema.AddField IcatIdxType IntDbType
            tblMgr.CreateTable tx Icat icatSchema

        let createKcat tblMgr =
            let kcatSchema = Schema.newSchema ()
            VarcharDbType TableManager.MaxName
            |> kcatSchema.AddField KcatIdxName
            VarcharDbType TableManager.MaxName
            |> kcatSchema.AddField KcatKeyName
            tblMgr.CreateTable tx Kcat kcatSchema

        createIcat tblMgr
        createKcat tblMgr

let newIndexManager fileMgr tblMgr =
    { CreateIndex = IndexManager.createIndex fileMgr tblMgr
      DropIndex = IndexManager.dropIndex fileMgr tblMgr
      GetIndexInfoByName = IndexManager.getIndexInfoByName fileMgr tblMgr
      GetIndexInfoByField = IndexManager.getIndexInfoByField fileMgr tblMgr
      GetIndexedFields = IndexManager.getIndexedFields fileMgr tblMgr
      InitIndexManager = IndexManager.initIndexManager tblMgr }
