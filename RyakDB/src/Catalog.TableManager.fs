module RyakDB.Catalog.TableManager

open RyakDB.DataType
open RyakDB.Table
open RyakDB.Table.TableFile
open RyakDB.Transaction

type TableManager =
    { CreateTable: Transaction -> string -> Schema -> unit
      DropTable: Transaction -> string -> unit
      GetTableInfo: Transaction -> string -> TableInfo option
      InitTableManager: Transaction -> unit }

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
        tableName
        + ".tbl"
        |> TableFile.formatFileHeader fileMgr tx.Buffer tx.Concurrency

    let newTcatInfo () =
        let tcatSchema = Schema.newSchema ()

        VarcharDbType MaxName
        |> tcatSchema.AddField TcatTableName

        TableInfo.newTableInfo Tcat tcatSchema

    let newFcatInfo () =
        let fcatSchema = Schema.newSchema ()

        VarcharDbType MaxName
        |> fcatSchema.AddField FcatTableName

        VarcharDbType MaxName
        |> fcatSchema.AddField FcatFieldName

        fcatSchema.AddField FcatType IntDbType

        fcatSchema.AddField FcatTypeArg IntDbType

        TableInfo.newTableInfo Fcat fcatSchema

    let getTableInfo fileMgr tx tableName =
        let rec findTcatfile tcatfile =
            if tcatfile.Next() then
                if tcatfile.GetVal TcatTableName
                   |> DbConstant.toString = tableName then
                    true
                else
                    findTcatfile tcatfile
            else
                false

        let findTcatInfo tcatInfo =
            let tcatfile =
                newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true tcatInfo

            tcatfile.BeforeFirst()
            let found = findTcatfile tcatfile
            tcatfile.Close()

            found

        let rec addField fcatfile schema =
            if fcatfile.Next() then
                if fcatfile.GetVal FcatTableName
                   |> DbConstant.toString = tableName then
                    DbType.fromInt
                        (fcatfile.GetVal FcatType |> DbConstant.toInt)
                        (fcatfile.GetVal FcatTypeArg |> DbConstant.toInt)
                    |> schema.AddField
                        (fcatfile.GetVal FcatFieldName
                         |> DbConstant.toString)
                addField fcatfile schema
            else
                schema

        let createSchema fcatInfo =
            let fcatfile =
                newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true fcatInfo

            fcatfile.BeforeFirst()
            let schema = Schema.newSchema () |> addField fcatfile
            fcatfile.Close()

            schema

        if newTcatInfo () |> findTcatInfo then
            newFcatInfo ()
            |> createSchema
            |> TableInfo.newTableInfo tableName
            |> Some
        else
            None

    let createTable fileMgr tx tableName schema =
        let addTcatInfo tcatInfo =
            let tcatfile =
                newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true tcatInfo

            tcatfile.Insert()

            DbConstant.newVarchar tableName
            |> tcatfile.SetVal TcatTableName

            tcatfile.Close()

        let addFieldName fcatfile fieldName =
            fcatfile.Insert()

            DbConstant.newVarchar tableName
            |> fcatfile.SetVal FcatTableName

            DbConstant.newVarchar fieldName
            |> fcatfile.SetVal FcatFieldName

            let fldType = schema.DbType fieldName
            DbType.toInt fldType
            |> IntDbConstant
            |> fcatfile.SetVal FcatType

            DbType.argument fldType
            |> IntDbConstant
            |> fcatfile.SetVal FcatTypeArg

        let addFcatInfo fcatInfo =
            let fcatfile =
                newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true fcatInfo

            schema.Fields()
            |> List.iter (addFieldName fcatfile)

            fcatfile.Close()

        if tableName <> Tcat && tableName <> Fcat
        then formatFileHeader fileMgr tx tableName

        newTcatInfo () |> addTcatInfo
        newFcatInfo () |> addFcatInfo

    let dropTable fileMgr tx tableName =
        let removeTableFile fileMgr =
            getTableInfo fileMgr tx tableName
            |> Option.map (newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
            |> Option.iter (fun tf -> tf.Remove())

        let rec deleteTcatfile tcatfile =
            if tcatfile.Next() then
                if tcatfile.GetVal TcatTableName
                   |> DbConstant.toString = tableName then
                    tcatfile.Delete()
                deleteTcatfile tcatfile

        let deleteTcatInfo tcatInfo =
            let tcatfile =
                newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true tcatInfo

            tcatfile.BeforeFirst()
            deleteTcatfile tcatfile
            tcatfile.Close()

        let rec deleteFcatfile fcatfile =
            if fcatfile.Next() then
                if fcatfile.GetVal FcatTableName
                   |> DbConstant.toString = tableName then
                    fcatfile.Delete()
                deleteFcatfile fcatfile

        let deleteFcatInfo fcatInfo =
            let fcatfile =
                newTableFile fileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true fcatInfo

            fcatfile.BeforeFirst()
            deleteFcatfile fcatfile
            fcatfile.Close()

        removeTableFile fileMgr
        newTcatInfo () |> deleteTcatInfo
        newFcatInfo () |> deleteFcatInfo

    let initTableManager fileMgr tx =
        formatFileHeader fileMgr tx Tcat
        formatFileHeader fileMgr tx Fcat

        createTable fileMgr tx Tcat (newTcatInfo ()).Schema
        createTable fileMgr tx Fcat (newFcatInfo ()).Schema

let newTableManager fileMgr =
    { CreateTable = TableManager.createTable fileMgr
      DropTable = TableManager.dropTable fileMgr
      GetTableInfo = TableManager.getTableInfo fileMgr
      InitTableManager = TableManager.initTableManager fileMgr }
