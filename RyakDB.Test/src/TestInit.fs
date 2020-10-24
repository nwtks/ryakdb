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
            db.Transaction.NewTransaction false Serializable

        Schema.newSchema ()
        |> (fun sch ->
            sch.AddField "d_id" IntDbType
            sch.AddField "d_name" (VarcharDbType 20)
            db.Catalog.CreateTable tx "dept" sch)

        db.Catalog.GetTableInfo tx "dept"
        |> Option.iter (fun ti ->
            use tf =
                newTableFile db.File tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true ti

            [ 1 .. deptMax ]
            |> List.iter (fun i ->
                tf.Insert()
                tf.SetVal "d_id" (IntDbConstant i)
                tf.SetVal "d_name" (DbConstant.newVarchar ("dept " + i.ToString()))))

        tx.Commit()

    let setupCourseTable db =
        let tx =
            db.Transaction.NewTransaction false Serializable

        Schema.newSchema ()
        |> (fun sch ->
            sch.AddField "c_id" IntDbType
            sch.AddField "title" (VarcharDbType 20)
            sch.AddField "dept_id" IntDbType
            db.Catalog.CreateTable tx "course" sch)

        db.Catalog.GetTableInfo tx "course"
        |> Option.iter (fun ti ->
            use tf =
                newTableFile db.File tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true ti

            [ 1 .. courseMax ]
            |> List.iter (fun i ->
                tf.Insert()
                tf.SetVal "c_id" (IntDbConstant i)
                tf.SetVal "title" (DbConstant.newVarchar ("course " + i.ToString()))
                tf.SetVal "dept_id" (IntDbConstant(i % deptMax + 1))))

        tx.Commit()

    let setupStudentTable db =
        let tx =
            db.Transaction.NewTransaction false Serializable

        Schema.newSchema ()
        |> (fun sch ->
            sch.AddField "s_id" IntDbType
            sch.AddField "s_name" (VarcharDbType 20)
            sch.AddField "major_id" IntDbType
            sch.AddField "grad_year" IntDbType
            db.Catalog.CreateTable tx "student" sch)

        db.Catalog.GetTableInfo tx "student"
        |> Option.iter (fun ti ->
            use tf =
                newTableFile db.File tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true ti

            [ 1 .. studentMax ]
            |> List.iter (fun i ->
                tf.Insert()
                tf.SetVal "s_id" (IntDbConstant i)
                tf.SetVal "s_name" (DbConstant.newVarchar ("student " + i.ToString()))
                tf.SetVal "major_id" (IntDbConstant(i % deptMax + 1))
                tf.SetVal "grad_year" (IntDbConstant(i % 50 + 1960))))

        tx.Commit()

    let setupSelectionTable db =
        let tx =
            db.Transaction.NewTransaction false Serializable

        Schema.newSchema ()
        |> (fun sch ->
            sch.AddField "sec_id" IntDbType
            sch.AddField "prof" (VarcharDbType 20)
            sch.AddField "course_id" IntDbType
            sch.AddField "year_offered" IntDbType
            db.Catalog.CreateTable tx "section" sch)

        db.Catalog.GetTableInfo tx "section"
        |> Option.iter (fun ti ->
            use tf =
                newTableFile db.File tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true ti

            [ 1 .. sectionMax ]
            |> List.iter (fun i ->
                tf.Insert()
                tf.SetVal "sec_id" (IntDbConstant i)
                tf.SetVal "prof" (DbConstant.newVarchar ("prof " + (i % 20 + 1).ToString()))
                tf.SetVal "course_id" (IntDbConstant(i % courseMax + 1))
                tf.SetVal "year_offered" (IntDbConstant(i % 30 + 1980))))

        tx.Commit()

    let setupEnrollTable db =
        let tx =
            db.Transaction.NewTransaction false Serializable

        Schema.newSchema ()
        |> (fun sch ->
            sch.AddField "e_id" IntDbType
            sch.AddField "grade" (VarcharDbType 2)
            sch.AddField "student_id" IntDbType
            sch.AddField "section_id" IntDbType
            db.Catalog.CreateTable tx "enroll" sch)

        let grades = [| "A+"; "A"; "B"; "C"; "D" |]
        db.Catalog.GetTableInfo tx "enroll"
        |> Option.iter (fun ti ->
            use tf =
                newTableFile db.File tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true ti

            [ 1 .. enrollMax ]
            |> List.iter (fun i ->
                tf.Insert()
                tf.SetVal "e_id" (IntDbConstant i)
                tf.SetVal "grade" (DbConstant.newVarchar (grades.[i % Array.length grades]))
                tf.SetVal "student_id" (IntDbConstant(i % studentMax + 1))
                tf.SetVal "section_id" (IntDbConstant(i % sectionMax + 1))))

        tx.Commit()

    let setupDb db =
        setupDeptTable db
        setupCourseTable db
        setupStudentTable db
        setupSelectionTable db
        setupEnrollTable db
