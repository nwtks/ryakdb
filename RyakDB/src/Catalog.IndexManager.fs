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

    let createIndex fileMgr tblMgr tx indexName indexType tableName fields =
        let createIcat tblMgr =
            tblMgr.GetTableInfo tx Icat
            |> Option.map (newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
            |> Option.iter (fun tf ->
                tf.Insert()
                tf.SetVal IcatIdxName (DbConstant.newVarchar indexName)
                tf.SetVal IcatTblName (DbConstant.newVarchar tableName)
                tf.SetVal IcatIdxType (IntDbConstant(int32 indexType))
                tf.Close())

        let createKcat tblMgr =
            tblMgr.GetTableInfo tx Kcat
            |> Option.map (newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
            |> Option.iter (fun tf ->
                fields
                |> List.iter (fun field ->
                    tf.Insert()
                    tf.SetVal KcatIdxName (DbConstant.newVarchar indexName)
                    tf.SetVal KcatKeyName (DbConstant.newVarchar field)
                    tf.Close()))

        createIcat tblMgr
        createKcat tblMgr

    let dropIndex fileMgr tblMgr tx indexName =
        let rec loopIcat (tf: TableFile) indexName =
            if tf.Next() then
                if tf.GetVal IcatIdxName |> Option.get = indexName
                then tf.Delete()
                loopIcat tf indexName

        let dropIcat tblMgr =
            tblMgr.GetTableInfo tx Icat
            |> Option.map (newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
            |> Option.iter (fun tf ->
                tf.BeforeFirst()
                loopIcat tf (DbConstant.newVarchar indexName)
                tf.Close())

        let rec loopVcat (tf: TableFile) indexName =
            if tf.Next() then
                if tf.GetVal KcatIdxName |> Option.get = indexName
                then tf.Delete()
                loopVcat tf indexName

        let dropKcat tblMgr =
            tblMgr.GetTableInfo tx Kcat
            |> Option.map (newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
            |> Option.iter (fun tf ->
                tf.BeforeFirst()
                loopVcat tf (DbConstant.newVarchar indexName)
                tf.Close())

        dropIcat tblMgr
        dropKcat tblMgr

    let getIndexInfoByName fileMgr tblMgr tx indexName =
        let rec loopIcat (tf: TableFile) indexName =
            if tf.Next() then
                if tf.GetVal IcatIdxName |> Option.get = indexName then
                    match tf.GetVal IcatIdxName
                          |> Option.map DbConstant.toString,
                          tf.GetVal IcatTblName
                          |> Option.map DbConstant.toString
                          |> Option.bind (tblMgr.GetTableInfo tx),
                          tf.GetVal IcatIdxType
                          |> Option.map DbConstant.toInt
                          |> Option.map enum<IndexType> with
                    | Some (idxName), (Some tblInfo), (Some idxType) -> Some(idxName, tblInfo, idxType)
                    | _ -> loopIcat tf indexName
                else
                    loopIcat tf indexName
            else
                None

        let findIcat tblMgr indexName =
            tblMgr.GetTableInfo tx Icat
            |> Option.map (newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
            |> Option.bind (fun tf ->
                tf.BeforeFirst()

                let tbl =
                    loopIcat tf (DbConstant.newVarchar indexName)

                tf.Close()
                tbl)

        let rec loopKcat (tf: TableFile) indexName fields =
            if tf.Next() then
                tf.GetVal KcatIdxName
                |> Option.filter (fun v -> v = indexName)
                |> Option.bind (fun _ ->
                    tf.GetVal KcatKeyName
                    |> Option.map DbConstant.toString)
                |> Option.map (fun field -> field :: fields)
                |> Option.defaultValue fields
                |> loopKcat tf indexName
            else
                fields

        let findKcat tblMgr indexName =
            tblMgr.GetTableInfo tx Kcat
            |> Option.map (newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
            |> Option.map (fun tf ->
                tf.BeforeFirst()

                let fields =
                    loopKcat tf (DbConstant.newVarchar indexName) []

                tf.Close()
                fields)

        findIcat tblMgr indexName
        |> Option.bind (fun (idxName, tblInfo, idxType) ->
            findKcat tblMgr indexName
            |> Option.map (IndexInfo.newIndexInfo idxName idxType tblInfo))

    let getIndexInfoByField fileMgr tblMgr tx tableName field =
        let rec loopIcat (tf: TableFile) tableName indexes =
            if tf.Next() then
                if tf.GetVal IcatTblName |> Option.get = tableName then
                    match tf.GetVal IcatIdxName
                          |> Option.map DbConstant.toString,
                          tf.GetVal IcatTblName
                          |> Option.map DbConstant.toString
                          |> Option.bind (tblMgr.GetTableInfo tx),
                          tf.GetVal IcatIdxType
                          |> Option.map DbConstant.toInt
                          |> Option.map enum<IndexType> with
                    | Some (idxName), (Some tblInfo), (Some idxType) -> (idxName, tblInfo, idxType) :: indexes
                    | _ -> indexes
                else
                    indexes
                |> loopIcat tf tableName
            else
                indexes

        let fineIcat tblMgr tableName =
            tblMgr.GetTableInfo tx Icat
            |> Option.map (newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
            |> Option.map (fun tf ->
                tf.BeforeFirst()

                let indexes =
                    loopIcat tf (DbConstant.newVarchar tableName) []

                tf.Close()
                indexes)

        let rec loopKcat (tf: TableFile) indexName fields =
            if tf.Next() then
                tf.GetVal KcatIdxName
                |> Option.filter (fun v -> v = indexName)
                |> Option.bind (fun _ ->
                    tf.GetVal KcatKeyName
                    |> Option.map DbConstant.toString)
                |> Option.map (fun f -> f :: fields)
                |> Option.defaultValue fields
                |> loopKcat tf indexName
            else
                fields

        let findKcat fileMgr tblMgr idxName idxType tblInfo =
            tblMgr.GetTableInfo tx Kcat
            |> Option.map (newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
            |> Option.bind (fun tf ->
                tf.BeforeFirst()

                let fields =
                    loopKcat tf (DbConstant.newVarchar idxName) []

                tf.Close()
                if fields |> List.contains field
                then Some(IndexInfo.newIndexInfo idxName idxType tblInfo fields)
                else None)

        fineIcat tblMgr tableName
        |> Option.map (List.choose (fun (idxName, tblInfo, idxType) -> findKcat fileMgr tblMgr idxName idxType tblInfo))
        |> Option.defaultValue []

    let getIndexedFields fileMgr tblMgr tx tableName =
        let rec loopIcat (tf: TableFile) tableName indexes =
            if tf.Next() then
                if tf.GetVal IcatTblName |> Option.get = tableName then
                    match tf.GetVal IcatIdxName
                          |> Option.map DbConstant.toString with
                    | Some (idxName) -> idxName :: indexes
                    | _ -> indexes
                else
                    indexes
                |> loopIcat tf tableName
            else
                indexes

        let findIcat tblMgr tableName =
            tblMgr.GetTableInfo tx Icat
            |> Option.map (newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
            |> Option.map (fun tf ->
                tf.BeforeFirst()

                let indexes =
                    loopIcat tf (DbConstant.newVarchar tableName) []

                tf.Close()
                indexes)

        let rec loopKcat (tf: TableFile) indexName fields =
            if tf.Next() then
                tf.GetVal KcatIdxName
                |> Option.filter (fun v -> v = indexName)
                |> Option.bind (fun _ ->
                    tf.GetVal KcatKeyName
                    |> Option.map DbConstant.toString)
                |> Option.map (fun field -> field :: fields)
                |> Option.defaultValue fields
                |> loopKcat tf indexName
            else
                fields

        let findKcat tblMgr indexName =
            tblMgr.GetTableInfo tx Kcat
            |> Option.map (newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
            |> Option.map (fun tf ->
                tf.BeforeFirst()

                let fields =
                    loopKcat tf (DbConstant.newVarchar indexName) []

                tf.Close()
                fields)

        findIcat tblMgr tableName
        |> Option.map (List.choose (findKcat tblMgr))
        |> Option.map (List.collect id)
        |> Option.defaultValue []
        |> Set.ofList
        |> Set.toList

    let initIndexManager tblMgr tx =
        let createIcat tblMgr =
            let icatSchema = Schema.newSchema ()
            icatSchema.AddField IcatIdxName (VarcharDbType TableManager.MaxName)
            icatSchema.AddField IcatTblName (VarcharDbType TableManager.MaxName)
            icatSchema.AddField IcatIdxType IntDbType
            tblMgr.CreateTable tx Icat icatSchema

        let createKcat tblMgr =
            let kcatSchema = Schema.newSchema ()
            kcatSchema.AddField KcatIdxName (VarcharDbType TableManager.MaxName)
            kcatSchema.AddField KcatKeyName (VarcharDbType TableManager.MaxName)
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
