module RyakDB.Catalog.ViewCatalogService

open RyakDB.DataType
open RyakDB.Table
open RyakDB.Sql.Parse
open RyakDB.Transaction
open RyakDB.Table.TableFile
open RyakDB.Catalog.TableCatalogService

type ViewCatalogService =
    { CreateView: Transaction -> string -> string -> unit
      DropView: Transaction -> string -> unit
      GetViewDef: Transaction -> string -> string option
      GetViewNamesByTable: Transaction -> string -> string list
      InitViewCatalogService: Transaction -> unit }

module ViewCatalogService =
    let Vcat = "cat_view"
    let VcatVname = "view_name"
    let VcatVdef = "view_def"
    let MaxViewDef = 300

    let createVcat tableService tx =
        let schema = Schema.newSchema ()

        VarcharDbType TableCatalogService.MaxName
        |> schema.AddField VcatVname

        VarcharDbType MaxViewDef
        |> schema.AddField VcatVdef

        tableService.CreateTable tx Vcat schema

    let rec findVcatfileByViewName tf viewName =
        if tf.Next() then
            if tf.GetVal VcatVname
               |> DbConstant.toString = viewName then
                tf.GetVal VcatVdef |> DbConstant.toString |> Some
            else
                findVcatfileByViewName tf viewName
        else
            None

    let findViewDefByViewName fileService tableService tx viewName =
        tableService.GetTableInfo tx Vcat
        |> Option.bind (fun ti ->
            use tf = newTableFile fileService tx true ti
            tf.BeforeFirst()
            findVcatfileByViewName tf viewName)

    let rec findVcatfileByTableName tf tableName viewNames =
        if tf.Next() then
            let QueryData(tables = tables) =
                tf.GetVal VcatVdef
                |> DbConstant.toString
                |> Parser.queryCommand

            if tables |> List.contains tableName then
                (tf.GetVal VcatVname |> DbConstant.toString)
                :: viewNames
            else
                viewNames
            |> findVcatfileByTableName tf tableName
        else
            viewNames

    let findViewNamestByTableName fileService tableService tx tableName =
        tableService.GetTableInfo tx Vcat
        |> Option.map (fun ti ->
            use tf = newTableFile fileService tx true ti
            tf.BeforeFirst()
            findVcatfileByTableName tf tableName []
            |> List.rev)
        |> Option.defaultValue []

    let createView fileService tableService tx viewName viewDef =
        let createVcatfile tableService =
            tableService.GetTableInfo tx Vcat
            |> Option.iter (fun ti ->
                use tf = newTableFile fileService tx true ti
                tf.Insert()

                DbConstant.newVarchar viewName
                |> tf.SetVal VcatVname

                DbConstant.newVarchar viewDef
                |> tf.SetVal VcatVdef)

        createVcatfile tableService

    let dropView fileService tableService tx viewName =
        let rec deleteVcatfile tf viewName =
            if tf.Next() then
                if tf.GetVal VcatVname
                   |> DbConstant.toString = viewName then
                    tf.Delete()
                deleteVcatfile tf viewName

        let dropVcat tableService =
            tableService.GetTableInfo tx Vcat
            |> Option.iter (fun ti ->
                use tf = newTableFile fileService tx true ti
                tf.BeforeFirst()
                deleteVcatfile tf viewName)

        dropVcat tableService

    let getViewDef fileService tableService tx viewName =
        findViewDefByViewName fileService tableService tx viewName

    let getViewNamesByTable fileService tableService tx tableName =
        findViewNamestByTableName fileService tableService tx tableName

    let initViewCatalogService tableService tx = createVcat tableService tx

let newViewCatalogService fileService tableService =
    { CreateView = ViewCatalogService.createView fileService tableService
      DropView = ViewCatalogService.dropView fileService tableService
      GetViewDef = ViewCatalogService.getViewDef fileService tableService
      GetViewNamesByTable = ViewCatalogService.getViewNamesByTable fileService tableService
      InitViewCatalogService = ViewCatalogService.initViewCatalogService tableService }
