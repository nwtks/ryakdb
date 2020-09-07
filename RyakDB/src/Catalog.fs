namespace RyakDB.Catalog

open RyakDB.DataType
open RyakDB.Query
open RyakDB.Sql.Parse
open RyakDB.Transaction
open RyakDB.Table.Record
open RyakDB.Table
open RyakDB.Index

type CatalogManager =
    { CreateTable: Transaction -> string -> Schema -> unit
      DropTable: Transaction -> string -> unit
      GetTableInfo: Transaction -> string -> TableInfo option
      CreateIndex: Transaction -> string -> IndexType -> string -> string list -> unit
      DropIndex: Transaction -> string -> unit
      GetIndexInfoByName: Transaction -> string -> IndexInfo option
      GetIndexInfoByField: Transaction -> string -> string -> IndexInfo list
      GetIndexedFields: Transaction -> string -> string list
      CreateView: Transaction -> string -> string -> unit
      DropView: Transaction -> string -> unit
      GetViewDef: Transaction -> string -> string option
      GetViewNamesByTable: Transaction -> string -> string list
      InitCatalogManager: Transaction -> unit }

type TableManager =
    { CreateTable: Transaction -> string -> Schema -> unit
      DropTable: Transaction -> string -> unit
      GetTableInfo: Transaction -> string -> TableInfo option
      InitTableManager: Transaction -> unit }

type IndexManager =
    { CreateIndex: Transaction -> string -> IndexType -> string -> string list -> unit
      DropIndex: Transaction -> string -> unit
      GetIndexInfoByName: Transaction -> string -> IndexInfo option
      GetIndexInfoByField: Transaction -> string -> string -> IndexInfo list
      GetIndexedFields: Transaction -> string -> string list
      InitIndexManager: Transaction -> unit }

type ViewManager =
    { CreateView: Transaction -> string -> string -> unit
      DropView: Transaction -> string -> unit
      GetViewDef: Transaction -> string -> string option
      GetViewNamesByTable: Transaction -> string -> string list
      InitViewManager: Transaction -> unit }

module TableManager =
    let Tcat = "cat_tbl"
    let TcatTableName = "tbl_name"

    let Fcat = "cat_tbl_fld"
    let FcatTableName = "tbl_name"
    let FcatFieldName = "fld_name"
    let FcatType = "type"
    let FcatTypeArg = "type_arg"

    let MaxName = 30

    let formatFileHeader fileMgr tx tableName =
        RecordFile.formatFileHeader fileMgr tx (tableName + ".tbl")

    let newTcatInfo () =
        let tcatSchema = Schema.newSchema ()
        tcatSchema.AddField TcatTableName (VarcharSqlType MaxName)
        TableInfo.newTableInfo Tcat tcatSchema

    let newFcatInfo () =
        let fcatSchema = Schema.newSchema ()
        fcatSchema.AddField FcatTableName (VarcharSqlType MaxName)
        fcatSchema.AddField FcatFieldName (VarcharSqlType MaxName)
        fcatSchema.AddField FcatType IntSqlType
        fcatSchema.AddField FcatTypeArg IntSqlType
        TableInfo.newTableInfo Fcat fcatSchema

    let getTableInfo fileMgr tx tableName =
        let rec findTcatfile (tcatfile: RecordFile) =
            if tcatfile.Next() then
                if tcatfile.GetVal TcatTableName
                   |> Option.map SqlConstant.toString
                   |> Option.map (fun tn -> tn = tableName)
                   |> Option.defaultValue false then
                    true
                else
                    findTcatfile tcatfile
            else
                false

        let findTcatInfo tcatInfo =
            let tcatfile =
                RecordFile.newRecordFile fileMgr tx true tcatInfo

            tcatfile.BeforeFirst()
            let found = findTcatfile tcatfile
            tcatfile.Close()
            found

        let rec addField (fcatfile: RecordFile) schema =
            if fcatfile.Next() then
                if fcatfile.GetVal FcatTableName
                   |> Option.map SqlConstant.toString
                   |> Option.map (fun tn -> tn = tableName)
                   |> Option.defaultValue false then
                    match fcatfile.GetVal FcatFieldName
                          |> Option.map SqlConstant.toString,
                          fcatfile.GetVal FcatType
                          |> Option.map SqlConstant.toInt,
                          fcatfile.GetVal FcatTypeArg
                          |> Option.map SqlConstant.toInt with
                    | Some (fn), Some (ft), Some (fa) -> SqlType.fromInt ft fa |> schema.AddField fn
                    | _ -> ()
                addField fcatfile schema
            else
                schema

        let createSchema fcatInfo =
            let fcatfile =
                RecordFile.newRecordFile fileMgr tx true fcatInfo

            fcatfile.BeforeFirst()
            let schema = Schema.newSchema () |> addField fcatfile
            fcatfile.Close()
            schema

        if newTcatInfo () |> findTcatInfo then
            Some
                (newFcatInfo ()
                 |> createSchema
                 |> TableInfo.newTableInfo tableName)
        else
            None

    let createTable fileMgr tx tableName schema =
        let addTcatInfo tcatInfo =
            let tcatfile =
                RecordFile.newRecordFile fileMgr tx true tcatInfo

            tcatfile.Insert()
            SqlConstant.newVarchar tableName
            |> tcatfile.SetVal TcatTableName
            tcatfile.Close()

        let addFieldName (fcatfile: RecordFile) fieldName =
            fcatfile.Insert()
            fcatfile.SetVal FcatTableName (SqlConstant.newVarchar tableName)
            fcatfile.SetVal FcatFieldName (SqlConstant.newVarchar fieldName)
            let fldType = schema.SqlType fieldName
            fcatfile.SetVal FcatType (SqlType.toInt fldType |> IntSqlConstant)
            fcatfile.SetVal FcatTypeArg (SqlType.argument fldType |> IntSqlConstant)

        let addFcatInfo fcatInfo =
            let fcatfile =
                RecordFile.newRecordFile fileMgr tx true fcatInfo

            schema.Fields()
            |> List.iter (addFieldName fcatfile)
            fcatfile.Close()

        if tableName <> Tcat && tableName <> Fcat
        then formatFileHeader fileMgr tx tableName

        newTcatInfo () |> addTcatInfo
        newFcatInfo () |> addFcatInfo

    let dropTable fileMgr (catalogMgr: CatalogManager) tx tableName =
        let removeTableInfo fileMgr =
            getTableInfo fileMgr tx tableName
            |> Option.map (fun ti -> RecordFile.newRecordFile fileMgr tx true ti)
            |> Option.iter (fun rf -> rf.Delete())

        let rec deleteTcatfile (tcatfile: RecordFile) =
            if tcatfile.Next() then
                if tcatfile.GetVal TcatTableName
                   |> Option.map SqlConstant.toString
                   |> Option.map (fun tn -> tn = tableName)
                   |> Option.defaultValue false then
                    tcatfile.Delete()
                deleteTcatfile tcatfile

        let removeTcatInfo tcatInfo =
            let tcatfile =
                RecordFile.newRecordFile fileMgr tx true tcatInfo

            tcatfile.BeforeFirst()
            deleteTcatfile tcatfile
            tcatfile.Close()

        let rec deleteFcatfile (fcatfile: RecordFile) =
            if fcatfile.Next() then
                if fcatfile.GetVal FcatTableName
                   |> Option.map SqlConstant.toString
                   |> Option.map (fun tn -> tn = tableName)
                   |> Option.defaultValue false then
                    fcatfile.Delete()
                deleteFcatfile fcatfile

        let removeFcatInfo fcatInfo =
            let fcatfile =
                RecordFile.newRecordFile fileMgr tx true fcatInfo

            fcatfile.BeforeFirst()
            deleteFcatfile fcatfile
            fcatfile.Close()

        removeTableInfo fileMgr
        newTcatInfo () |> removeTcatInfo
        newFcatInfo () |> removeFcatInfo

        catalogMgr.GetIndexedFields tx tableName
        |> List.collect (catalogMgr.GetIndexInfoByField tx tableName)
        |> List.iter (fun ii -> catalogMgr.DropIndex tx ii.IndexName)

        catalogMgr.GetViewNamesByTable tx tableName
        |> List.iter (catalogMgr.DropView tx)

    let initTableManager fileMgr tx =
        formatFileHeader fileMgr tx Tcat
        formatFileHeader fileMgr tx Fcat
        createTable fileMgr tx Tcat (newTcatInfo ()).Schema
        createTable fileMgr tx Fcat (newFcatInfo ()).Schema

    let newTableManager fileMgr catalogMgr =
        { CreateTable = createTable fileMgr
          DropTable = dropTable fileMgr catalogMgr
          GetTableInfo = getTableInfo fileMgr
          InitTableManager = initTableManager fileMgr }

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
            |> Option.map (fun ti -> RecordFile.newRecordFile fileMgr tx true ti)
            |> Option.iter (fun rf ->
                rf.Insert()
                rf.SetVal IcatIdxName (SqlConstant.newVarchar indexName)
                rf.SetVal IcatTblName (SqlConstant.newVarchar tableName)
                rf.SetVal IcatIdxType (IntSqlConstant(int32 indexType))
                rf.Close())

        let createKcat tblMgr =
            tblMgr.GetTableInfo tx Kcat
            |> Option.map (fun ti -> RecordFile.newRecordFile fileMgr tx true ti)
            |> Option.iter (fun rf ->
                fields
                |> List.iter (fun field ->
                    rf.Insert()
                    rf.SetVal KcatIdxName (SqlConstant.newVarchar indexName)
                    rf.SetVal KcatKeyName (SqlConstant.newVarchar field)
                    rf.Close()))

        createIcat tblMgr
        createKcat tblMgr

    let dropIndex fileMgr tblMgr tx indexName =
        let rec loopIcat (rf: RecordFile) indexName =
            if rf.Next() then
                if rf.GetVal IcatIdxName |> Option.get = indexName
                then rf.Delete()
                loopIcat rf indexName

        let dropIcat tblMgr =
            tblMgr.GetTableInfo tx Icat
            |> Option.map (fun ti -> RecordFile.newRecordFile fileMgr tx true ti)
            |> Option.iter (fun rf ->
                rf.BeforeFirst()
                loopIcat rf (SqlConstant.newVarchar indexName)
                rf.Close())

        let rec loopVcat (rf: RecordFile) indexName =
            if rf.Next() then
                if rf.GetVal KcatIdxName |> Option.get = indexName
                then rf.Delete()
                loopVcat rf indexName

        let dropKcat tblMgr =
            tblMgr.GetTableInfo tx Kcat
            |> Option.map (fun ti -> RecordFile.newRecordFile fileMgr tx true ti)
            |> Option.iter (fun rf ->
                rf.BeforeFirst()
                loopVcat rf (SqlConstant.newVarchar indexName)
                rf.Close())

        dropIcat tblMgr
        dropKcat tblMgr

    let getIndexInfoByName fileMgr tblMgr tx indexName =
        let rec loopIcat (rf: RecordFile) indexName =
            if rf.Next() then
                if rf.GetVal IcatIdxName |> Option.get = indexName then
                    match rf.GetVal IcatIdxName
                          |> Option.map SqlConstant.toString,
                          rf.GetVal IcatTblName
                          |> Option.map SqlConstant.toString
                          |> Option.bind (tblMgr.GetTableInfo tx),
                          rf.GetVal IcatIdxType
                          |> Option.map SqlConstant.toInt
                          |> Option.map enum<IndexType> with
                    | Some (idxName), (Some tblInfo), (Some idxType) -> Some(idxName, tblInfo, idxType)
                    | _ -> loopIcat rf indexName
                else
                    loopIcat rf indexName
            else
                None

        let findIcat tblMgr indexName =
            tblMgr.GetTableInfo tx Icat
            |> Option.map (fun ti -> RecordFile.newRecordFile fileMgr tx true ti)
            |> Option.bind (fun rf ->
                rf.BeforeFirst()

                let tbl =
                    loopIcat rf (SqlConstant.newVarchar indexName)

                rf.Close()
                tbl)

        let rec loopKcat (rf: RecordFile) indexName fields =
            if rf.Next() then
                rf.GetVal KcatIdxName
                |> Option.filter (fun v -> v = indexName)
                |> Option.bind (fun _ ->
                    rf.GetVal KcatKeyName
                    |> Option.map SqlConstant.toString)
                |> Option.map (fun field -> field :: fields)
                |> Option.defaultValue fields
                |> loopKcat rf indexName
            else
                fields

        let findKcat tblMgr indexName =
            tblMgr.GetTableInfo tx Kcat
            |> Option.map (fun ti -> RecordFile.newRecordFile fileMgr tx true ti)
            |> Option.map (fun rf ->
                rf.BeforeFirst()

                let fields =
                    loopKcat rf (SqlConstant.newVarchar indexName) []

                rf.Close()
                fields)

        findIcat tblMgr indexName
        |> Option.bind (fun (idxName, tblInfo, idxType) ->
            findKcat tblMgr indexName
            |> Option.map (fun fields -> IndexInfo.newIndexInfo idxName idxType tblInfo fields))

    let getIndexInfoByField fileMgr tblMgr tx tableName field =
        let rec loopIcat (rf: RecordFile) tableName indexes =
            if rf.Next() then
                if rf.GetVal IcatTblName |> Option.get = tableName then
                    match rf.GetVal IcatIdxName
                          |> Option.map SqlConstant.toString,
                          rf.GetVal IcatTblName
                          |> Option.map SqlConstant.toString
                          |> Option.bind (tblMgr.GetTableInfo tx),
                          rf.GetVal IcatIdxType
                          |> Option.map SqlConstant.toInt
                          |> Option.map enum<IndexType> with
                    | Some (idxName), (Some tblInfo), (Some idxType) -> (idxName, tblInfo, idxType) :: indexes
                    | _ -> indexes
                else
                    indexes
                |> loopIcat rf tableName
            else
                indexes

        let fineIcat tblMgr tableName =
            tblMgr.GetTableInfo tx Icat
            |> Option.map (fun ti -> RecordFile.newRecordFile fileMgr tx true ti)
            |> Option.map (fun rf ->
                rf.BeforeFirst()

                let indexes =
                    loopIcat rf (SqlConstant.newVarchar tableName) []

                rf.Close()
                indexes)

        let rec loopKcat (rf: RecordFile) indexName fields =
            if rf.Next() then
                rf.GetVal KcatIdxName
                |> Option.filter (fun v -> v = indexName)
                |> Option.bind (fun _ ->
                    rf.GetVal KcatKeyName
                    |> Option.map SqlConstant.toString)
                |> Option.map (fun f -> f :: fields)
                |> Option.defaultValue fields
                |> loopKcat rf indexName
            else
                fields

        let findKcat fileMgr tblMgr idxName idxType tblInfo =
            tblMgr.GetTableInfo tx Kcat
            |> Option.map (fun ti -> RecordFile.newRecordFile fileMgr tx true ti)
            |> Option.bind (fun rf ->
                rf.BeforeFirst()

                let fields =
                    loopKcat rf (SqlConstant.newVarchar idxName) []

                rf.Close()
                if fields |> List.contains field
                then Some(IndexInfo.newIndexInfo idxName idxType tblInfo fields)
                else None)

        fineIcat tblMgr tableName
        |> Option.map (List.choose (fun (idxName, tblInfo, idxType) -> findKcat fileMgr tblMgr idxName idxType tblInfo))
        |> Option.defaultValue []

    let getIndexedFields fileMgr tblMgr tx tableName =
        let rec loopIcat (rf: RecordFile) tableName indexes =
            if rf.Next() then
                if rf.GetVal IcatTblName |> Option.get = tableName then
                    match rf.GetVal IcatIdxName
                          |> Option.map SqlConstant.toString with
                    | Some (idxName) -> idxName :: indexes
                    | _ -> indexes
                else
                    indexes
                |> loopIcat rf tableName
            else
                indexes

        let findIcat tblMgr tableName =
            tblMgr.GetTableInfo tx Icat
            |> Option.map (fun ti -> RecordFile.newRecordFile fileMgr tx true ti)
            |> Option.map (fun rf ->
                rf.BeforeFirst()

                let indexes =
                    loopIcat rf (SqlConstant.newVarchar tableName) []

                rf.Close()
                indexes)

        let rec loopKcat (rf: RecordFile) indexName fields =
            if rf.Next() then
                rf.GetVal KcatIdxName
                |> Option.filter (fun v -> v = indexName)
                |> Option.bind (fun _ ->
                    rf.GetVal KcatKeyName
                    |> Option.map SqlConstant.toString)
                |> Option.map (fun field -> field :: fields)
                |> Option.defaultValue fields
                |> loopKcat rf indexName
            else
                fields

        let findKcat tblMgr indexName =
            tblMgr.GetTableInfo tx Kcat
            |> Option.map (fun ti -> RecordFile.newRecordFile fileMgr tx true ti)
            |> Option.map (fun rf ->
                rf.BeforeFirst()

                let fields =
                    loopKcat rf (SqlConstant.newVarchar indexName) []

                rf.Close()
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
            icatSchema.AddField IcatIdxName (VarcharSqlType TableManager.MaxName)
            icatSchema.AddField IcatTblName (VarcharSqlType TableManager.MaxName)
            icatSchema.AddField IcatIdxType IntSqlType
            tblMgr.CreateTable tx Icat icatSchema

        let createKcat tblMgr =
            let kcatSchema = Schema.newSchema ()
            kcatSchema.AddField KcatIdxName (VarcharSqlType TableManager.MaxName)
            kcatSchema.AddField KcatKeyName (VarcharSqlType TableManager.MaxName)
            tblMgr.CreateTable tx Kcat kcatSchema

        createIcat tblMgr
        createKcat tblMgr

    let newIndexManager fileMgr tblMgr =
        { CreateIndex = createIndex fileMgr tblMgr
          DropIndex = dropIndex fileMgr tblMgr
          GetIndexInfoByName = getIndexInfoByName fileMgr tblMgr
          GetIndexInfoByField = getIndexInfoByField fileMgr tblMgr
          GetIndexedFields = getIndexedFields fileMgr tblMgr
          InitIndexManager = initIndexManager tblMgr }

module ViewManager =
    let Vcat = "cat_view"
    let VcatVname = "view_name"
    let VcatVdef = "view_def"
    let MaxViewDef = 300

    let createView fileMgr tblMgr tx viewName viewDef =
        let createVcat tblMgr =
            tblMgr.GetTableInfo tx Vcat
            |> Option.map (fun ti -> RecordFile.newRecordFile fileMgr tx true ti)
            |> Option.iter (fun rf ->
                rf.Insert()
                rf.SetVal VcatVname (SqlConstant.newVarchar viewName)
                rf.SetVal VcatVdef (SqlConstant.newVarchar viewDef)
                rf.Close())

        createVcat tblMgr

    let dropView fileMgr tblMgr tx viewName =
        let rec loopVcat (rf: RecordFile) viewName =
            if rf.Next() then
                if rf.GetVal VcatVname |> Option.get = viewName
                then rf.Delete()
                loopVcat rf viewName

        let dropVcat tblMgr =
            tblMgr.GetTableInfo tx Vcat
            |> Option.map (fun ti -> RecordFile.newRecordFile fileMgr tx true ti)
            |> Option.iter (fun rf ->
                rf.BeforeFirst()
                loopVcat rf (SqlConstant.newVarchar viewName)
                rf.Close())

        dropVcat tblMgr

    let getViewDef fileMgr tblMgr tx viewName =
        let rec loopVcat (rf: RecordFile) viewName =
            if rf.Next() then
                if rf.GetVal VcatVname |> Option.get = viewName then
                    rf.GetVal VcatVdef
                    |> Option.map SqlConstant.toString
                else
                    loopVcat rf viewName
            else
                None

        let findVcat tblMgr viewName =
            tblMgr.GetTableInfo tx Vcat
            |> Option.map (fun ti -> RecordFile.newRecordFile fileMgr tx true ti)
            |> Option.bind (fun rf ->
                rf.BeforeFirst()

                let tn =
                    loopVcat rf (SqlConstant.newVarchar viewName)

                rf.Close()
                tn)

        findVcat tblMgr viewName

    let getViewNamesByTable fileMgr tblMgr tx tableName =
        let rec loopVcat (rf: RecordFile) tableName viewNames =
            if rf.Next() then
                rf.GetVal VcatVdef
                |> Option.map SqlConstant.toString
                |> Option.map Parser.queryCommand
                |> Option.map (fun qd ->
                    let QueryData(tables = tables) = qd
                    tables)
                |> Option.filter (List.contains tableName)
                |> Option.bind (fun _ ->
                    rf.GetVal VcatVname
                    |> Option.map SqlConstant.toString)
                |> Option.map (fun viewName -> viewName :: viewNames)
                |> Option.defaultValue viewNames
                |> loopVcat rf tableName
            else
                viewNames

        let findVcat tblMgr tableName =
            tblMgr.GetTableInfo tx Vcat
            |> Option.map (fun ti -> RecordFile.newRecordFile fileMgr tx true ti)
            |> Option.map (fun rf ->
                rf.BeforeFirst()
                let viewNames = loopVcat rf tableName []
                rf.Close()
                viewNames)

        findVcat tblMgr tableName
        |> Option.defaultValue []

    let initViewManager tblMgr tx =
        let schema = Schema.newSchema ()
        schema.AddField VcatVname (VarcharSqlType TableManager.MaxName)
        schema.AddField VcatVdef (VarcharSqlType MaxViewDef)
        tblMgr.CreateTable tx Vcat schema

    let newViewManager fileMgr tblMgr =
        { CreateView = createView fileMgr tblMgr
          DropView = dropView fileMgr tblMgr
          GetViewDef = getViewDef fileMgr tblMgr
          GetViewNamesByTable = getViewNamesByTable fileMgr tblMgr
          InitViewManager = initViewManager tblMgr }

module CatalogManager =
    let newCatalogManager fileMgr =
        let mutable tblMgr = None
        let mutable idxMgr = None
        let mutable viewMgr = None

        let catalogManager =
            { CreateTable = fun tx tableName schema -> (tblMgr |> Option.get).CreateTable tx tableName schema
              DropTable = fun tx tableName -> (tblMgr |> Option.get).DropTable tx tableName
              GetTableInfo = fun tx tableName -> (tblMgr |> Option.get).GetTableInfo tx tableName
              CreateIndex =
                  fun tx indexName indexType tableName fieldNames ->
                      (idxMgr |> Option.get).CreateIndex tx indexName indexType tableName fieldNames
              DropIndex = fun tx indexName -> (idxMgr |> Option.get).DropIndex tx indexName
              GetIndexInfoByName = fun tx indexName -> (idxMgr |> Option.get).GetIndexInfoByName tx indexName
              GetIndexInfoByField =
                  fun tx indexName fieldName -> (idxMgr |> Option.get).GetIndexInfoByField tx indexName fieldName
              GetIndexedFields = fun tx indexName -> (idxMgr |> Option.get).GetIndexedFields tx indexName
              CreateView = fun tx viewName query -> (viewMgr |> Option.get).CreateView tx viewName query
              DropView = fun tx viewName -> (viewMgr |> Option.get).DropView tx viewName
              GetViewDef = fun tx viewName -> (viewMgr |> Option.get).GetViewDef tx viewName
              GetViewNamesByTable = fun tx tableName -> (viewMgr |> Option.get).GetViewNamesByTable tx tableName
              InitCatalogManager =
                  fun tx ->
                      (tblMgr |> Option.get).InitTableManager tx
                      (idxMgr |> Option.get).InitIndexManager tx
                      (viewMgr |> Option.get).InitViewManager tx }

        let tableManager =
            TableManager.newTableManager fileMgr catalogManager

        let indexManager =
            IndexManager.newIndexManager fileMgr tableManager

        let viewManager =
            ViewManager.newViewManager fileMgr tableManager

        tblMgr <- Some tableManager
        idxMgr <- Some indexManager
        viewMgr <- Some viewManager
        catalogManager