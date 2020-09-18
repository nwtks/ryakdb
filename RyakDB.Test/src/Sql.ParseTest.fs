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
        Assert.Equal
            ([ "student_name"
               "student_grade"
               "count_of_sname"
               "avg_of_score"
               "sum_of_sid"
               "max_of_grade"
               "min_of_level" ],
             projectionFields |> List.toSeq)
        Assert.Equal([ "student"; "dept" ], tables |> List.toSeq)
        Assert.Equal
            (Predicate
                ([ Term(EqualOperator, FieldNameExpression "name", ConstantExpression(DbConstant.newVarchar "Scott"))
                   Term(EqualOperator, FieldNameExpression "sdid", FieldNameExpression "did") ]),
             predicate)
        Assert.Equal([ "student_grade"; "student_name" ], groupFields |> List.toSeq)
        Assert.Equal
            ([ CountFn("sname")
               AvgFn("score")
               SumFn("sid")
               MaxFn("grade")
               MinFn("level") ],
             aggregationFns |> List.toSeq)
        Assert.Equal
            ([ SortField("sid", SortDesc)
               SortField("dsid", SortAsc)
               SortField("avg_of_score", SortAsc) ],
             sortFields |> List.toSeq)

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
        Assert.Equal("test_lab", tableName)
        Assert.Equal([ "id"; "name"; "l_budget"; "l_serial" ], fields |> List.toSeq)
        Assert.Equal
            ([ DoubleDbConstant(11.0)
               DbConstant.newVarchar "net DB"
               DoubleDbConstant(700.26)
               DoubleDbConstant(-1234567891025.0) ],
             values |> List.toSeq)
    | _ -> Assert.True(false)

[<Fact>]
let ``parse update`` () =
    let sql = """
        UPDATE class_room
        set days=add(days,2), rate = mul(0.2, rate), use = 'yes', level=SUB(level, 1), summary=div(total,cnt)
        WHERE cid >= 2165 and sid<=25000
        """

    match Parser.updateCommand sql with
    | ModifyData (tableName, predicate, fields) ->
        Assert.Equal("class_room", tableName)
        Assert.Equal
            ([ "days"
               "level"
               "rate"
               "summary"
               "use" ],
             fields |> Map.toSeq |> Seq.map (fun (k, _) -> k))
        Assert.Equal
            (BinaryArithmeticExpression
                (AddOperator, FieldNameExpression "days", ConstantExpression(DoubleDbConstant 2.0)),
             fields.["days"])
        Assert.Equal
            (BinaryArithmeticExpression
                (MulOperator, ConstantExpression(DoubleDbConstant 0.2), FieldNameExpression "rate"),
             fields.["rate"])
        Assert.Equal(ConstantExpression(DbConstant.newVarchar "yes"), fields.["use"])
        Assert.Equal
            (BinaryArithmeticExpression
                (SubOperator, FieldNameExpression "level", ConstantExpression(DoubleDbConstant 1.0)),
             fields.["level"])
        Assert.Equal
            (BinaryArithmeticExpression(DivOperator, FieldNameExpression "total", FieldNameExpression "cnt"),
             fields.["summary"])
        Assert.Equal
            (Predicate
                ([ Term(GraterThanEqualOperator, FieldNameExpression "cid", ConstantExpression(DoubleDbConstant 2165.0))
                   Term(LessThanEqualOperator, FieldNameExpression "sid", ConstantExpression(DoubleDbConstant 25000.0)) ]),
             predicate)
    | _ -> Assert.True(false)

[<Fact>]
let ``parse delete`` () =
    let sql = """
        DELETE FROM student
        WHERE sid < 555 and sdid > 3021
        """

    match Parser.updateCommand sql with
    | DeleteData (tableName, predicate) ->
        Assert.Equal("student", tableName)
        Assert.Equal
            (Predicate
                ([ Term(LessThanOperator, FieldNameExpression "sid", ConstantExpression(DoubleDbConstant 555.0))
                   Term(GraterThanOperator, FieldNameExpression "sdid", ConstantExpression(DoubleDbConstant 3021.0)) ]),
             predicate)
    | _ -> Assert.True(false)

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
        Assert.Equal("enro11", tableName)
        Assert.Equal(IntDbType, schema.DbType "eid")
        Assert.Equal(BigIntDbType, schema.DbType "student_id")
        Assert.Equal(BigIntDbType, schema.DbType "student_id")
        Assert.Equal(DoubleDbType, schema.DbType "section_id")
    | _ -> Assert.True(false)

[<Fact>]
let ``parse create index`` () =
    let sql = """
        CREATE INDEX idx1
        ON tbl1 (col5, col12, col8)
        USING BTREE
        """

    match Parser.updateCommand sql with
    | CreateIndexData (indexName, indexType, tableName, fields) ->
        Assert.Equal("idx1", indexName)
        Assert.Equal(IndexType.BTree, indexType)
        Assert.Equal("tbl1", tableName)
        Assert.Equal([ "col5"; "col12"; "col8" ], fields |> List.toSeq)
    | _ -> Assert.True(false)
