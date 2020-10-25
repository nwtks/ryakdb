module RyakDB.Test.TestInit

open RyakDB.DataType
open RyakDB.Table
open RyakDB.Index
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

    db.Catalog.CreateIndex tx "dept_idx1" BTree "dept" [ "d_id" ]
    db.Catalog.CreateIndex tx "dept_idx2" BTree "dept" [ "d_name" ]

    use tf =
        db.Catalog.GetTableInfo tx "dept"
        |> Option.get
        |> newTableFile db.File tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true

    use idx1 =
        db.Catalog.GetIndexInfoByName tx "dept_idx1"
        |> Option.get
        |> IndexFactory.newIndex db.File tx

    use idx2 =
        db.Catalog.GetIndexInfoByName tx "dept_idx2"
        |> Option.get
        |> IndexFactory.newIndex db.File tx

    [ 1 .. TestInit.deptMax ]
    |> List.iter (fun i ->
        let name = "dept " + i.ToString()
        tf.Insert()
        tf.SetVal "d_id" (IntDbConstant i)
        tf.SetVal "d_name" (DbConstant.newVarchar name)
        idx1.Insert false (SearchKey.newSearchKey [ IntDbConstant i ]) (tf.CurrentRecordId())
        idx2.Insert false (SearchKey.newSearchKey [ DbConstant.newVarchar name ]) (tf.CurrentRecordId()))

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

    use tf =
        db.Catalog.GetTableInfo tx "course"
        |> Option.get
        |> newTableFile db.File tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true

    [ 1 .. TestInit.courseMax ]
    |> List.iter (fun i ->
        let title = "course " + i.ToString()
        let deptId = i % TestInit.deptMax + 1
        tf.Insert()
        tf.SetVal "c_id" (IntDbConstant i)
        tf.SetVal "title" (DbConstant.newVarchar title)
        tf.SetVal "dept_id" (IntDbConstant deptId))

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

    db.Catalog.CreateIndex tx "student_idx1" BTree "student" [ "s_id" ]
    db.Catalog.CreateIndex tx "student_idx2" BTree "student" [ "major_id" ]

    use tf =
        db.Catalog.GetTableInfo tx "student"
        |> Option.get
        |> newTableFile db.File tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true

    use idx1 =
        db.Catalog.GetIndexInfoByName tx "student_idx1"
        |> Option.get
        |> IndexFactory.newIndex db.File tx

    use idx2 =
        db.Catalog.GetIndexInfoByName tx "student_idx2"
        |> Option.get
        |> IndexFactory.newIndex db.File tx

    [ 1 .. TestInit.studentMax ]
    |> List.iter (fun i ->
        let name = "student " + i.ToString()
        let majorId = i % TestInit.deptMax + 1
        let gradYear = i % 50 + 1960
        tf.Insert()
        tf.SetVal "s_id" (IntDbConstant i)
        tf.SetVal "s_name" (DbConstant.newVarchar name)
        tf.SetVal "major_id" (IntDbConstant majorId)
        tf.SetVal "grad_year" (IntDbConstant gradYear)
        idx1.Insert false (SearchKey.newSearchKey [ IntDbConstant i ]) (tf.CurrentRecordId())
        idx2.Insert false (SearchKey.newSearchKey [ IntDbConstant majorId ]) (tf.CurrentRecordId()))

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

    use tf =
        db.Catalog.GetTableInfo tx "section"
        |> Option.get
        |> newTableFile db.File tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true

    [ 1 .. TestInit.sectionMax ]
    |> List.iter (fun i ->
        let prof = "prof " + (i % 20 + 1).ToString()
        let courseId = i % TestInit.courseMax + 1
        let yearOffered = i % 30 + 1980
        tf.Insert()
        tf.SetVal "sec_id" (IntDbConstant i)
        tf.SetVal "prof" (DbConstant.newVarchar prof)
        tf.SetVal "course_id" (IntDbConstant courseId)
        tf.SetVal "year_offered" (IntDbConstant yearOffered))

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

    use tf =
        db.Catalog.GetTableInfo tx "enroll"
        |> Option.get
        |> newTableFile db.File tx.Buffer tx.Concurrency tx.Recovery tx.ReadOnly true

    [ 1 .. TestInit.enrollMax ]
    |> List.iter (fun i ->
        let grade = grades.[i % Array.length grades]
        let studentId = i % TestInit.studentMax + 1
        let sectionId = i % TestInit.sectionMax + 1
        tf.Insert()
        tf.SetVal "e_id" (IntDbConstant i)
        tf.SetVal "grade" (DbConstant.newVarchar grade)
        tf.SetVal "student_id" (IntDbConstant studentId)
        tf.SetVal "section_id" (IntDbConstant sectionId))

    tx.Commit()

let setupDb db =
    setupDeptTable db
    setupCourseTable db
    setupStudentTable db
    setupSelectionTable db
    setupEnrollTable db
