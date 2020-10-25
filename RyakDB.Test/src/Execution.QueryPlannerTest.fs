module RyakDB.Test.Execution.QueryPlannerTest

open Xunit
open FsUnit.Xunit
open RyakDB.DataType
open RyakDB.Transaction
open RyakDB.Execution.Plan
open RyakDB.Database
open RyakDB.Test.TestInit

[<Fact>]
let query () =
    use db =
        { Database.defaultConfig () with
              BlockSize = 1024
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    setupStudentTable db
    setupDeptTable db

    let tx =
        db.Transaction.NewTransaction false Serializable

    use scan =
        db.Planner.CreateQueryPlan tx """
            select s_id, s_name, d_name, grad_year
            from student, dept
            where s_id < 200 and major_id=d_id and d_name='dept 20'
            order by s_id
            """
        |> Plan.openScan

    scan.BeforeFirst()
    scan.Next() |> should be True
    scan.GetVal "s_id"
    |> DbConstant.toInt
    |> should equal 19
    scan.GetVal "s_name"
    |> DbConstant.toString
    |> should equal "student 19"
    scan.GetVal "d_name"
    |> DbConstant.toString
    |> should equal "dept 20"
    scan.GetVal "grad_year"
    |> DbConstant.toInt
    |> should equal 1979

    scan.Next() |> should be True
    scan.GetVal "s_id"
    |> DbConstant.toInt
    |> should equal 59
    scan.GetVal "s_name"
    |> DbConstant.toString
    |> should equal "student 59"
    scan.GetVal "d_name"
    |> DbConstant.toString
    |> should equal "dept 20"
    scan.GetVal "grad_year"
    |> DbConstant.toInt
    |> should equal 1969

    scan.Next() |> should be True
    scan.GetVal "s_id"
    |> DbConstant.toInt
    |> should equal 99
    scan.GetVal "s_name"
    |> DbConstant.toString
    |> should equal "student 99"
    scan.GetVal "d_name"
    |> DbConstant.toString
    |> should equal "dept 20"
    scan.GetVal "grad_year"
    |> DbConstant.toInt
    |> should equal 2009

    scan.Next() |> should be True
    scan.GetVal "s_id"
    |> DbConstant.toInt
    |> should equal 139
    scan.GetVal "s_name"
    |> DbConstant.toString
    |> should equal "student 139"
    scan.GetVal "d_name"
    |> DbConstant.toString
    |> should equal "dept 20"
    scan.GetVal "grad_year"
    |> DbConstant.toInt
    |> should equal 1999

    scan.Next() |> should be True
    scan.GetVal "s_id"
    |> DbConstant.toInt
    |> should equal 179
    scan.GetVal "s_name"
    |> DbConstant.toString
    |> should equal "student 179"
    scan.GetVal "d_name"
    |> DbConstant.toString
    |> should equal "dept 20"
    scan.GetVal "grad_year"
    |> DbConstant.toInt
    |> should equal 1989

    scan.Next() |> should be False

    db.Catalog.DropTable tx "student"
    db.Catalog.DropTable tx "dept"
    tx.Commit()


[<Fact>]
let view () =
    use db =
        { Database.defaultConfig () with
              BlockSize = 1024
              InMemory = true }
        |> newDatabase ("test_dbs_" + System.DateTime.Now.Ticks.ToString())

    setupStudentTable db
    setupDeptTable db

    let tx =
        db.Transaction.NewTransaction false Serializable

    db.Planner.ExecuteUpdate tx """
        create view student_dept20 as
        select s_id, s_name, major_id, d_name, grad_year
        from student, dept
        where major_id=d_id and d_name='dept 20'
        """
    |> ignore

    use scan =
        db.Planner.CreateQueryPlan tx """
            select s_id, s_name, d_name, grad_year
            from student_dept20
            where s_id < 100
            order by s_id
            """
        |> Plan.openScan

    scan.BeforeFirst()
    scan.Next() |> should be True
    scan.GetVal "s_id"
    |> DbConstant.toInt
    |> should equal 19
    scan.GetVal "s_name"
    |> DbConstant.toString
    |> should equal "student 19"
    scan.GetVal "d_name"
    |> DbConstant.toString
    |> should equal "dept 20"
    scan.GetVal "grad_year"
    |> DbConstant.toInt
    |> should equal 1979

    scan.Next() |> should be True
    scan.GetVal "s_id"
    |> DbConstant.toInt
    |> should equal 59
    scan.GetVal "s_name"
    |> DbConstant.toString
    |> should equal "student 59"
    scan.GetVal "d_name"
    |> DbConstant.toString
    |> should equal "dept 20"
    scan.GetVal "grad_year"
    |> DbConstant.toInt
    |> should equal 1969

    scan.Next() |> should be True
    scan.GetVal "s_id"
    |> DbConstant.toInt
    |> should equal 99
    scan.GetVal "s_name"
    |> DbConstant.toString
    |> should equal "student 99"
    scan.GetVal "d_name"
    |> DbConstant.toString
    |> should equal "dept 20"
    scan.GetVal "grad_year"
    |> DbConstant.toInt
    |> should equal 2009

    scan.Next() |> should be False

    db.Catalog.DropTable tx "student"
    db.Catalog.DropTable tx "dept"
    tx.Commit()
