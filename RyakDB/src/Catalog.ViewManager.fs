module RyakDB.Catalog.ViewManager

open RyakDB.DataType
open RyakDB.Table
open RyakDB.Sql.Parse
open RyakDB.Table.TableFile
open RyakDB.Transaction
open RyakDB.Catalog.TableManager

type ViewManager =
    { CreateView: Transaction -> string -> string -> unit
      DropView: Transaction -> string -> unit
      GetViewDef: Transaction -> string -> string option
      GetViewNamesByTable: Transaction -> string -> string list
      InitViewManager: Transaction -> unit }

module ViewManager =
    let Vcat = "cat_view"
    let VcatVname = "view_name"
    let VcatVdef = "view_def"
    let MaxViewDef = 300

    let rec findVcatfileByViewName tf viewName =
        if tf.Next() then
            if tf.GetVal VcatVname
               |> Option.map DbConstant.toString
               |> Option.map (fun name -> name = viewName)
               |> Option.defaultValue false then
                tf.GetVal VcatVdef
                |> Option.map DbConstant.toString
            else
                findVcatfileByViewName tf viewName
        else
            None

    let findViewDefByViewName fileMgr tblMgr tx viewName =
        tblMgr.GetTableInfo tx Vcat
        |> Option.map (newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
        |> Option.bind (fun tf ->
            tf.BeforeFirst()
            let viewDef = findVcatfileByViewName tf viewName
            tf.Close()
            viewDef)

    let rec findVcatfileByTableName tf tableName viewNames =
        if tf.Next() then
            tf.GetVal VcatVdef
            |> Option.map DbConstant.toString
            |> Option.map Parser.queryCommand
            |> Option.map (fun qd ->
                let QueryData(tables = tables) = qd
                tables)
            |> Option.filter (List.contains tableName)
            |> Option.bind (fun _ ->
                tf.GetVal VcatVname
                |> Option.map DbConstant.toString)
            |> Option.map (fun viewName -> viewName :: viewNames)
            |> Option.defaultValue viewNames
            |> findVcatfileByTableName tf tableName
        else
            viewNames

    let findViewNamestByTableName fileMgr tblMgr tx tableName =
        tblMgr.GetTableInfo tx Vcat
        |> Option.map (newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
        |> Option.map (fun tf ->
            tf.BeforeFirst()
            let viewNames = findVcatfileByTableName tf tableName []
            tf.Close()
            viewNames)
        |> Option.defaultValue []

    let createView fileMgr tblMgr tx viewName viewDef =
        let createVcatfile tblMgr =
            tblMgr.GetTableInfo tx Vcat
            |> Option.map (newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
            |> Option.iter (fun tf ->
                tf.Insert()
                tf.SetVal VcatVname (DbConstant.newVarchar viewName)
                tf.SetVal VcatVdef (DbConstant.newVarchar viewDef)
                tf.Close())

        createVcatfile tblMgr

    let dropView fileMgr tblMgr tx viewName =
        let rec deleteVcatfile tf viewName =
            if tf.Next() then
                if tf.GetVal VcatVname
                   |> Option.map DbConstant.toString
                   |> Option.map (fun name -> name = viewName)
                   |> Option.defaultValue false then
                    tf.Delete()
                deleteVcatfile tf viewName

        let dropVcat tblMgr =
            tblMgr.GetTableInfo tx Vcat
            |> Option.map (newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
            |> Option.iter (fun tf ->
                tf.BeforeFirst()
                deleteVcatfile tf viewName
                tf.Close())

        dropVcat tblMgr

    let getViewDef fileMgr tblMgr tx viewName =
        findViewDefByViewName fileMgr tblMgr tx viewName

    let getViewNamesByTable fileMgr tblMgr tx tableName =
        findViewNamestByTableName fileMgr tblMgr tx tableName

    let initViewManager tblMgr tx =
        let schema = Schema.newSchema ()
        schema.AddField VcatVname (VarcharDbType TableManager.MaxName)
        schema.AddField VcatVdef (VarcharDbType MaxViewDef)
        tblMgr.CreateTable tx Vcat schema

let newViewManager fileMgr tblMgr =
    { CreateView = ViewManager.createView fileMgr tblMgr
      DropView = ViewManager.dropView fileMgr tblMgr
      GetViewDef = ViewManager.getViewDef fileMgr tblMgr
      GetViewNamesByTable = ViewManager.getViewNamesByTable fileMgr tblMgr
      InitViewManager = ViewManager.initViewManager tblMgr }
