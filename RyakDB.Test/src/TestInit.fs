namespace RyakDB.Test.TestInit

open RyakDB.DataType
open RyakDB.Table
open RyakDB.Table.Record
open RyakDB.Concurrency
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
            sch.AddField "d_id" IntSqlType
            sch.AddField "d_name" (VarcharSqlType 20)
            db.CatalogMgr.CreateTable tx "dept" sch)

        db.CatalogMgr.GetTableInfo tx "dept"
        |> Option.map (RecordFile.newRecordFile db.FileMgr tx true)
        |> Option.iter (fun rf ->
            for i in 1 .. deptMax do
                rf.Insert()
                rf.SetVal "d_id" (IntSqlConstant i)
                rf.SetVal "d_name" (SqlConstant.newVarchar ("dept " + i.ToString())))

        tx.Commit()

    let setupCourseTable db =
        let tx =
            db.TxMgr.NewTransaction false Serializable

        Schema.newSchema ()
        |> (fun sch ->
            sch.AddField "c_id" IntSqlType
            sch.AddField "title" (VarcharSqlType 20)
            sch.AddField "dept_id" IntSqlType
            db.CatalogMgr.CreateTable tx "course" sch)

        db.CatalogMgr.GetTableInfo tx "course"
        |> Option.map (RecordFile.newRecordFile db.FileMgr tx true)
        |> Option.iter (fun rf ->
            for i in 1 .. courseMax do
                rf.Insert()
                rf.SetVal "c_id" (IntSqlConstant i)
                rf.SetVal "title" (SqlConstant.newVarchar ("course " + i.ToString()))
                rf.SetVal "dept_id" (IntSqlConstant(i % deptMax + 1)))

        tx.Commit()

    let setupStudentTable db =
        let tx =
            db.TxMgr.NewTransaction false Serializable

        Schema.newSchema ()
        |> (fun sch ->
            sch.AddField "s_id" IntSqlType
            sch.AddField "s_name" (VarcharSqlType 20)
            sch.AddField "major_id" IntSqlType
            sch.AddField "grad_year" IntSqlType
            db.CatalogMgr.CreateTable tx "student" sch)

        db.CatalogMgr.GetTableInfo tx "student"
        |> Option.map (RecordFile.newRecordFile db.FileMgr tx true)
        |> Option.iter (fun rf ->
            for i in 1 .. studentMax do
                rf.Insert()
                rf.SetVal "s_id" (IntSqlConstant i)
                rf.SetVal "s_name" (SqlConstant.newVarchar ("student " + i.ToString()))
                rf.SetVal "major_id" (IntSqlConstant(i % deptMax + 1))
                rf.SetVal "grad_year" (IntSqlConstant(i % 50 + 1960)))

        tx.Commit()

    let setupSelectionTable db =
        let tx =
            db.TxMgr.NewTransaction false Serializable

        Schema.newSchema ()
        |> (fun sch ->
            sch.AddField "sec_id" IntSqlType
            sch.AddField "prof" (VarcharSqlType 20)
            sch.AddField "course_id" IntSqlType
            sch.AddField "year_offered" IntSqlType
            db.CatalogMgr.CreateTable tx "section" sch)

        db.CatalogMgr.GetTableInfo tx "section"
        |> Option.map (RecordFile.newRecordFile db.FileMgr tx true)
        |> Option.iter (fun rf ->
            for i in 1 .. sectionMax do
                rf.Insert()
                rf.SetVal "sec_id" (IntSqlConstant i)
                rf.SetVal "prof" (SqlConstant.newVarchar ("prof " + (i % 20 + 1).ToString()))
                rf.SetVal "course_id" (IntSqlConstant(i % courseMax + 1))
                rf.SetVal "year_offered" (IntSqlConstant(i % 30 + 1980)))

        tx.Commit()

    let setupEnrollTable db =
        let tx =
            db.TxMgr.NewTransaction false Serializable

        Schema.newSchema ()
        |> (fun sch ->
            sch.AddField "e_id" IntSqlType
            sch.AddField "grade" (VarcharSqlType 2)
            sch.AddField "student_id" IntSqlType
            sch.AddField "section_id" IntSqlType
            db.CatalogMgr.CreateTable tx "enroll" sch)

        db.CatalogMgr.GetTableInfo tx "enroll"
        |> Option.map (RecordFile.newRecordFile db.FileMgr tx true)
        |> Option.iter (fun rf ->
            let grades = [| "A+"; "A"; "B"; "C"; "D" |]
            for i in 1 .. enrollMax do
                rf.Insert()
                rf.SetVal "e_id" (IntSqlConstant i)
                rf.SetVal "grade" (SqlConstant.newVarchar (grades.[i % grades.Length]))
                rf.SetVal "student_id" (IntSqlConstant(i % studentMax + 1))
                rf.SetVal "section_id" (IntSqlConstant(i % sectionMax + 1)))

        tx.Commit()

    let setupDb db =
        setupDeptTable db
        setupCourseTable db
        setupStudentTable db
        setupSelectionTable db
        setupEnrollTable db
