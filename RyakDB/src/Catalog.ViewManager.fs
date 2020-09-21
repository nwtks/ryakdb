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

    let createView fileMgr tblMgr tx viewName viewDef =
        let createVcat tblMgr =
            tblMgr.GetTableInfo tx Vcat
            |> Option.map (newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
            |> Option.iter (fun tf ->
                tf.Insert()
                tf.SetVal VcatVname (DbConstant.newVarchar viewName)
                tf.SetVal VcatVdef (DbConstant.newVarchar viewDef)
                tf.Close())

        createVcat tblMgr

    let dropView fileMgr tblMgr tx viewName =
        let rec loopVcat tf viewName =
            if tf.Next() then
                if tf.GetVal VcatVname |> Option.get = viewName
                then tf.Delete()
                loopVcat tf viewName

        let dropVcat tblMgr =
            tblMgr.GetTableInfo tx Vcat
            |> Option.map (newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
            |> Option.iter (fun tf ->
                tf.BeforeFirst()
                loopVcat tf (DbConstant.newVarchar viewName)
                tf.Close())

        dropVcat tblMgr

    let getViewDef fileMgr tblMgr tx viewName =
        let rec loopVcat tf viewName =
            if tf.Next() then
                if tf.GetVal VcatVname |> Option.get = viewName then
                    tf.GetVal VcatVdef
                    |> Option.map DbConstant.toString
                else
                    loopVcat tf viewName
            else
                None

        let findVcat tblMgr viewName =
            tblMgr.GetTableInfo tx Vcat
            |> Option.map (newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
            |> Option.bind (fun tf ->
                tf.BeforeFirst()

                let tn =
                    loopVcat tf (DbConstant.newVarchar viewName)

                tf.Close()
                tn)

        findVcat tblMgr viewName

    let getViewNamesByTable fileMgr tblMgr tx tableName =
        let rec loopVcat tf tableName viewNames =
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
                |> loopVcat tf tableName
            else
                viewNames

        let findVcat tblMgr tableName =
            tblMgr.GetTableInfo tx Vcat
            |> Option.map (newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
            |> Option.map (fun tf ->
                tf.BeforeFirst()
                let viewNames = loopVcat tf tableName []
                tf.Close()
                viewNames)

        findVcat tblMgr tableName
        |> Option.defaultValue []

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
