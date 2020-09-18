module RyakDB.Test.TestInit

open RyakDB.DataType
open RyakDB.Table
open RyakDB.Table.TableFile
open RyakDB.Transaction
open RyakDB.Database

module TestInit =
    let deptMax = 40
    let courseMax = 300
    let studentMax = 900
    let sectionMax = 1200
    let enrollMax = 2000

    let setupDeptTable db =
        let tx =
            db.TxMgr.NewTransaction false Serializable

        Schema.newSchema ()
        |> (fun sch ->
            sch.AddField "d_id" IntDbType
            sch.AddField "d_name" (VarcharDbType 20)
            db.CatalogMgr.CreateTable tx "dept" sch)

        db.CatalogMgr.GetTableInfo tx "dept"
        |> Option.map (newTableFile db.FileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
        |> Option.iter (fun tf ->
            for i in 1 .. deptMax do
                tf.Insert()
                tf.SetVal "d_id" (IntDbConstant i)
                tf.SetVal "d_name" (DbConstant.newVarchar ("dept " + i.ToString())))

        tx.Commit()

    let setupCourseTable db =
        let tx =
            db.TxMgr.NewTransaction false Serializable

        Schema.newSchema ()
        |> (fun sch ->
            sch.AddField "c_id" IntDbType
            sch.AddField "title" (VarcharDbType 20)
            sch.AddField "dept_id" IntDbType
            db.CatalogMgr.CreateTable tx "course" sch)

        db.CatalogMgr.GetTableInfo tx "course"
        |> Option.map (newTableFile db.FileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
        |> Option.iter (fun tf ->
            for i in 1 .. courseMax do
                tf.Insert()
                tf.SetVal "c_id" (IntDbConstant i)
                tf.SetVal "title" (DbConstant.newVarchar ("course " + i.ToString()))
                tf.SetVal "dept_id" (IntDbConstant(i % deptMax + 1)))

        tx.Commit()

    let setupStudentTable db =
        let tx =
            db.TxMgr.NewTransaction false Serializable

        Schema.newSchema ()
        |> (fun sch ->
            sch.AddField "s_id" IntDbType
            sch.AddField "s_name" (VarcharDbType 20)
            sch.AddField "major_id" IntDbType
            sch.AddField "grad_year" IntDbType
            db.CatalogMgr.CreateTable tx "student" sch)

        db.CatalogMgr.GetTableInfo tx "student"
        |> Option.map (newTableFile db.FileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
        |> Option.iter (fun tf ->
            for i in 1 .. studentMax do
                tf.Insert()
                tf.SetVal "s_id" (IntDbConstant i)
                tf.SetVal "s_name" (DbConstant.newVarchar ("student " + i.ToString()))
                tf.SetVal "major_id" (IntDbConstant(i % deptMax + 1))
                tf.SetVal "grad_year" (IntDbConstant(i % 50 + 1960)))

        tx.Commit()

    let setupSelectionTable db =
        let tx =
            db.TxMgr.NewTransaction false Serializable

        Schema.newSchema ()
        |> (fun sch ->
            sch.AddField "sec_id" IntDbType
            sch.AddField "prof" (VarcharDbType 20)
            sch.AddField "course_id" IntDbType
            sch.AddField "year_offered" IntDbType
            db.CatalogMgr.CreateTable tx "section" sch)

        db.CatalogMgr.GetTableInfo tx "section"
        |> Option.map (newTableFile db.FileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
        |> Option.iter (fun tf ->
            for i in 1 .. sectionMax do
                tf.Insert()
                tf.SetVal "sec_id" (IntDbConstant i)
                tf.SetVal "prof" (DbConstant.newVarchar ("prof " + (i % 20 + 1).ToString()))
                tf.SetVal "course_id" (IntDbConstant(i % courseMax + 1))
                tf.SetVal "year_offered" (IntDbConstant(i % 30 + 1980)))

        tx.Commit()

    let setupEnrollTable db =
        let tx =
            db.TxMgr.NewTransaction false Serializable

        Schema.newSchema ()
        |> (fun sch ->
            sch.AddField "e_id" IntDbType
            sch.AddField "grade" (VarcharDbType 2)
            sch.AddField "student_id" IntDbType
            sch.AddField "section_id" IntDbType
            db.CatalogMgr.CreateTable tx "enroll" sch)

        db.CatalogMgr.GetTableInfo tx "enroll"
        |> Option.map (newTableFile db.FileMgr tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true)
        |> Option.iter (fun tf ->
            let grades = [| "A+"; "A"; "B"; "C"; "D" |]
            for i in 1 .. enrollMax do
                tf.Insert()
                tf.SetVal "e_id" (IntDbConstant i)
                tf.SetVal "grade" (DbConstant.newVarchar (grades.[i % grades.Length]))
                tf.SetVal "student_id" (IntDbConstant(i % studentMax + 1))
                tf.SetVal "section_id" (IntDbConstant(i % sectionMax + 1)))

        tx.Commit()

    let setupDb db =
        setupDeptTable db
        setupCourseTable db
        setupStudentTable db
        setupSelectionTable db
        setupEnrollTable db
