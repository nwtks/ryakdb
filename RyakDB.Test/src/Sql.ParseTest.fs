module RyakDB.Test.Sql.ParseTest

open Xunit
open FsUnit.Xunit
open RyakDB.DataType
open RyakDB.Index
open RyakDB.Query
open RyakDB.Query.Predicate
open RyakDB.Sql.Parse

[<Fact>]
let ``parse select`` () =
    let sql = """
        select
        student_name, student_grade, count(sname), avg(score), sum(sid), max(grade), min(level)
        from student, dept
        where name='Scott' and sdid = did
        group by student_grade, student_name
        order by sid desc, dsid, avg(score) asc
        """

    match Parser.queryCommand sql with
    | QueryData (projectionFields, tables, predicate, groupFields, aggregationFns, sortFields) ->
        projectionFields
        |> should
            equal
               [ "student_name"
                 "student_grade"
                 "count_of_sname"
                 "avg_of_score"
                 "sum_of_sid"
                 "max_of_grade"
                 "min_of_level" ]
        tables |> should equal [ "student"; "dept" ]
        predicate
        |> should
            equal
               (Predicate [ Term
                                (EqualOperator,
                                 FieldNameExpression "name",
                                 ConstantExpression(DbConstant.newVarchar "Scott"))
                            Term(EqualOperator, FieldNameExpression "sdid", FieldNameExpression "did") ])
        groupFields
        |> should equal [ "student_grade"; "student_name" ]
        aggregationFns
        |> should
            equal
               [ CountFn("sname")
                 AvgFn("score")
                 SumFn("sid")
                 MaxFn("grade")
                 MinFn("level") ]
        sortFields
        |> should
            equal
               [ SortField("sid", SortDesc)
                 SortField("dsid", SortAsc)
                 SortField("avg_of_score", SortAsc) ]

[<Fact>]
let ``parse insert`` () =
    let sql = """
        INSERT INTO test_lab
        (id, name, l_budget, l_serial)
        values
        (11, 'net DB', 700.26, -1234567891025)
        """

    match Parser.updateCommand sql with
    | InsertData (tableName, fields, values) ->
        tableName |> should equal "test_lab"
        fields
        |> should equal [ "id"; "name"; "l_budget"; "l_serial" ]
        values
        |> should
            equal
               [ DoubleDbConstant(11.0)
                 DbConstant.newVarchar "net DB"
                 DoubleDbConstant(700.26)
                 DoubleDbConstant(-1234567891025.0) ]
    | _ -> failwith "cant parse insert"

[<Fact>]
let ``parse update`` () =
    let sql = """
        UPDATE class_room
        set days=add(days,2), rate = mul(0.2, rate), use = 'yes', level=SUB(level, 1), summary=div(total,cnt)
        WHERE cid >= 2165 and sid<=25000
        """

    match Parser.updateCommand sql with
    | ModifyData (tableName, predicate, fields) ->
        tableName |> should equal "class_room"
        fields.["days"]
        |> should
            equal
               (BinaryArithmeticExpression
                   (AddOperator, FieldNameExpression "days", ConstantExpression(DoubleDbConstant 2.0)))
        fields.["rate"]
        |> should
            equal
               (BinaryArithmeticExpression
                   (MulOperator, ConstantExpression(DoubleDbConstant 0.2), FieldNameExpression "rate"))
        fields.["use"]
        |> should equal (ConstantExpression(DbConstant.newVarchar "yes"))
        fields.["level"]
        |> should
            equal
               (BinaryArithmeticExpression
                   (SubOperator, FieldNameExpression "level", ConstantExpression(DoubleDbConstant 1.0)))
        fields.["summary"]
        |> should
            equal
               (BinaryArithmeticExpression(DivOperator, FieldNameExpression "total", FieldNameExpression "cnt"))
        predicate
        |> should
            equal
               (Predicate [ Term
                                (GraterThanEqualOperator,
                                 FieldNameExpression "cid",
                                 ConstantExpression(DoubleDbConstant 2165.0))
                            Term
                                (LessThanEqualOperator,
                                 FieldNameExpression "sid",
                                 ConstantExpression(DoubleDbConstant 25000.0)) ])
    | _ -> failwith "cant parse update"

[<Fact>]
let ``parse delete`` () =
    let sql = """
        DELETE FROM student
        WHERE sid < 555 and sdid > 3021
        """

    match Parser.updateCommand sql with
    | DeleteData (tableName, predicate) ->
        tableName |> should equal "student"
        predicate
        |> should
            equal
               (Predicate [ Term
                                (LessThanOperator, FieldNameExpression "sid", ConstantExpression(DoubleDbConstant 555.0))
                            Term
                                (GraterThanOperator,
                                 FieldNameExpression "sdid",
                                 ConstantExpression(DoubleDbConstant 3021.0)) ])
    | _ -> failwith "cant parse delete"

[<Fact>]
let ``parse create table`` () =
    let sql = """
        create table Enro11(
        Eid int,
        Student_Id bigint,
        Section_Id double,
        Grade varchar(2))
        """

    match Parser.updateCommand sql with
    | CreateTableData (tableName, schema) ->
        tableName |> should equal "enro11"
        schema.DbType "eid" |> should equal IntDbType
        schema.DbType "student_id"
        |> should equal BigIntDbType
        schema.DbType "student_id"
        |> should equal BigIntDbType
        schema.DbType "section_id"
        |> should equal DoubleDbType
    | _ -> failwith "cant parse create table"

[<Fact>]
let ``parse create B-tree index`` () =
    let sql = """
        CREATE INDEX idx1
        ON tbl1 (col5, col12, col8)
        USING BTREE
        """

    match Parser.updateCommand sql with
    | CreateIndexData (indexName, indexType, tableName, fields) ->
        indexName |> should equal "idx1"
        indexType |> should equal BTree
        tableName |> should equal "tbl1"
        fields |> should equal [ "col5"; "col12"; "col8" ]
    | _ -> failwith "cant parse create index"

[<Fact>]
let ``parse create hash index`` () =
    let sql = """
        CREATE INDEX idx1
        ON tbl1 (col5)
        USING HASH
        """

    match Parser.updateCommand sql with
    | CreateIndexData (indexName, indexType, tableName, fields) ->
        indexName |> should equal "idx1"
        indexType |> should equal Hash
        tableName |> should equal "tbl1"
        fields |> should equal [ "col5" ]
    | _ -> failwith "cant parse create index"

[<Fact>]
let ``parse drop table`` () =
    let sql = """
        Drop table Tbl
        """

    match Parser.updateCommand sql with
    | DropTableData (tableName) -> tableName |> should equal "tbl"
    | _ -> failwith "cant parse drop table"

[<Fact>]
let ``parse drop index`` () =
    let sql = """
        DROP INDEX IDX1
        """

    match Parser.updateCommand sql with
    | DropIndexData (indexName) -> indexName |> should equal "idx1"
    | _ -> failwith "cant parse drop index"

[<Fact>]
let ``parse drop view`` () =
    let sql = """
        drop view v
        """

    match Parser.updateCommand sql with
    | DropViewData (viewName) -> viewName |> should equal "v"
    | _ -> failwith "cant parse drop view"
