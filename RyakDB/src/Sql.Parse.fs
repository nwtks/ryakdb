namespace RyakDB.Sql.Parse

open RyakDB.DataType
open RyakDB.Table
open RyakDB.Query
open RyakDB.Query.Predicate

type QueryData = QueryData of projectionFields: string list * tables: string list * predicate: Predicate * groupFields: string list * aggregationFns: AggregationFn list * sortFields: SortField list

type UpdateData =
    | InsertData of tableName: string * fieldNames: string list * values: SqlConstant list
    | DeleteData of tableName: string * predicate: Predicate
    | ModifyData of tableName: string * predicate: Predicate * fieldValues: Map<string, Expression>
    | CreateTableData of tableName: string * schema: Schema
    | DropTableData of tableName: string
    | CreateIndexData of indexName: string * indexType: IndexType * tableName: string * fieldNames: string list
    | DropIndexData of indexName: string
    | CreateViewData of viewName: string * viewDef: string
    | DropViewData of viewName: string

module Parser =
    type Lexer(text: string) =
        let mutable pointer = 0

        member this.SkipWhitespace() =
            while pointer < text.Length
                  && System.Char.IsWhiteSpace(text.[pointer]) do
                pointer <- pointer + 1

        member this.MatchDelimiter delimiter =
            this.SkipWhitespace()
            pointer < text.Length
            && text.[pointer] = delimiter

        member this.MatchNumericConstant() =
            this.SkipWhitespace()
            (pointer < text.Length
             && System.Char.IsDigit(text.[pointer]))
            || (pointer
                + 1 < text.Length
                && text.[pointer] = '-'
                && System.Char.IsDigit(text.[pointer + 1]))

        member this.MatchStringConstant() =
            this.SkipWhitespace()
            pointer < text.Length && text.[pointer] = '\''

        member this.MatchIdentity() =
            this.SkipWhitespace()
            pointer < text.Length
            && System.Char.IsLetter(text.[pointer])

        member this.MatchKeyword(keyword: string) =
            this.MatchIdentity()
            && pointer
            + keyword.Length
            <= text.Length
            && text.[pointer..(pointer + keyword.Length - 1)].ToLower() = keyword.ToLower()

        member this.EatDelimimiter delimiter =
            if not (this.MatchDelimiter delimiter)
            then failwith ("Syntax error: " + delimiter.ToString())
            pointer <- pointer + 1

        member this.EatNumericConstant() =
            if not (this.MatchNumericConstant()) then failwith "Syntax error: number"

            let rec skip n =
                if pointer
                   + n < text.Length
                   && (System.Char.IsDigit(text.[pointer + n])
                       || text.[pointer + n] = '.') then
                    n + 1 |> skip
                else
                    n

            let n = skip 1
            let s = text.[pointer..(pointer + n - 1)]
            pointer <- pointer + n
            System.Double.Parse(s)

        member this.EatStringConstant() =
            if not (this.MatchStringConstant()) then failwith "Syntax error: string"

            let rec build (sb: System.Text.StringBuilder) n =
                if pointer + n >= text.Length then failwith "Syntax error: string"
                if text.[pointer + n] <> '\'' then
                    sb.Append(text.[pointer + n]) |> ignore
                    n + 1 |> build sb
                elif pointer
                     + n
                     + 1 < text.Length
                     && text.[pointer + n + 1] = '\'' then
                    sb.Append('\'') |> ignore
                    n + 2 |> build sb
                else
                    n + 1

            let sb = System.Text.StringBuilder()
            let n = build sb 1
            pointer <- pointer + n
            sb.ToString()

        member this.EatIdentity() =
            if not (this.MatchIdentity()) then failwith "Syntax error: identity"

            let rec skip n =
                if pointer
                   + n < text.Length
                   && (System.Char.IsLetterOrDigit(text.[pointer + n])
                       || text.[pointer + n] = '_') then
                    n + 1 |> skip
                else
                    n

            let n = skip 1
            let s = text.[pointer..(pointer + n - 1)]
            pointer <- pointer + n
            s.ToLower()

        member this.EatKeyword(keyword: string) =
            if not (this.MatchKeyword keyword) then failwith ("Syntax error: " + keyword)
            pointer <- pointer + keyword.Length

        member this.Tail() =
            this.SkipWhitespace()
            let current = pointer
            pointer <- text.Length
            text.[current..]

    type ProjectEl =
        | ProjectFd of field: string
        | ProjectAggFn of aggFn: AggregationFn

    type ProjectList = ProjectList of els: ProjectEl list

    module ProjectList =
        let addField field (ProjectList els) = ProjectFd field :: els |> ProjectList

        let addAggFn aggFn (ProjectList els) = ProjectAggFn aggFn :: els |> ProjectList

        let fieldNames (ProjectList els) =
            els
            |> List.rev
            |> List.map (fun el ->
                match el with
                | ProjectFd field -> field
                | ProjectAggFn aggFn -> AggregationFn.fieldName aggFn)

        let aggregationFns (ProjectList els) =
            els
            |> List.rev
            |> List.choose (fun el ->
                match el with
                | ProjectAggFn aggFn -> Some aggFn
                | _ -> None)

    type SortEl =
        | SortFld of field: string * dir: SortDirection
        | SortAggFn of aggFn: AggregationFn * dir: SortDirection

    type SortList = SortList of els: SortEl list

    module SortList =
        let addField field dir (SortList els) = SortFld(field, dir) :: els |> SortList

        let addAggFn aggFn dir (SortList els) = SortAggFn(aggFn, dir) :: els |> SortList

        let fieldList (SortList els) =
            els
            |> List.rev
            |> List.map (fun el ->
                match el with
                | SortFld (field, dir) -> SortField(field, dir)
                | SortAggFn (aggFn, dir) -> SortField(AggregationFn.fieldName aggFn, dir))

    let identityList (lex: Lexer) =
        let rec eat list =
            if lex.MatchDelimiter ',' then
                lex.EatDelimimiter ','
                lex.EatIdentity() :: list |> eat
            else
                List.rev list

        [ lex.EatIdentity() ] |> eat

    let constant (lex: Lexer) =
        if lex.MatchStringConstant()
        then lex.EatStringConstant() |> SqlConstant.newVarchar
        elif lex.MatchNumericConstant()
        then lex.EatNumericConstant() |> DoubleSqlConstant
        else failwith "Syntax error: constant"

    let constantList (lex: Lexer) =
        let rec eat list =
            if lex.MatchDelimiter ',' then
                lex.EatDelimimiter ','
                constant lex :: list |> eat
            else
                List.rev list

        [ constant lex ] |> eat

    let queryExpression (lex: Lexer) =
        if lex.MatchIdentity() then lex.EatIdentity() |> FieldNameExpression else constant lex |> ConstantExpression

    let aggregationFn (lex: Lexer) =
        if lex.MatchKeyword "avg" then
            lex.EatKeyword "avg"
            lex.EatDelimimiter '('
            let aggFn = lex.EatIdentity() |> AvgFn
            lex.EatDelimimiter ')'
            Some aggFn
        elif lex.MatchKeyword "count" then
            lex.EatKeyword "count"
            lex.EatDelimimiter '('
            let aggFn = lex.EatIdentity() |> CountFn
            lex.EatDelimimiter ')'
            Some aggFn
        elif lex.MatchKeyword "max" then
            lex.EatKeyword "max"
            lex.EatDelimimiter '('
            let aggFn = lex.EatIdentity() |> MaxFn
            lex.EatDelimimiter ')'
            Some aggFn
        elif lex.MatchKeyword "min" then
            lex.EatKeyword "min"
            lex.EatDelimimiter '('
            let aggFn = lex.EatIdentity() |> MinFn
            lex.EatDelimimiter ')'
            Some aggFn
        elif lex.MatchKeyword "sum" then
            lex.EatKeyword "sum"
            lex.EatDelimimiter '('
            let aggFn = lex.EatIdentity() |> SumFn
            lex.EatDelimimiter ')'
            Some aggFn
        else
            None

    let projectList (lex: Lexer) =
        let rec addToList list =
            match aggregationFn lex with
            | Some aggFn -> list |> ProjectList.addAggFn aggFn
            | _ -> list |> ProjectList.addField (lex.EatIdentity())
            |> fun nextlist ->
                if lex.MatchDelimiter ',' then
                    lex.EatDelimimiter ','
                    nextlist |> addToList
                else
                    nextlist

        ProjectList [] |> addToList

    let term (lex: Lexer) =
        let lhs = queryExpression lex

        let op =
            if lex.MatchDelimiter '=' then
                lex.EatDelimimiter '='
                EqualOperator
            elif lex.MatchDelimiter '>' then
                lex.EatDelimimiter '>'
                if lex.MatchDelimiter '=' then
                    lex.EatDelimimiter '='
                    GraterThanEqualOperator
                else
                    GraterThanOperator
            elif lex.MatchDelimiter '<' then
                lex.EatDelimimiter '<'
                if lex.MatchDelimiter '=' then
                    lex.EatDelimimiter '='
                    LessThanEqualOperator
                else
                    LessThanOperator
            else
                failwith "Syntax error: operator"

        let rhs = queryExpression lex
        Term(op, lhs, rhs)

    let predicate (lex: Lexer) =
        let rec loopTerm terms =
            term lex
            :: terms
            |> fun terms2 ->
                if lex.MatchKeyword "and" then
                    lex.EatKeyword "and"
                    loopTerm terms2
                else
                    terms2

        [] |> loopTerm |> List.rev |> Predicate

    let sortDirection (lex: Lexer) =
        if lex.MatchKeyword "asc" then
            lex.EatKeyword "asc"
            SortAsc
        elif lex.MatchKeyword "desc" then
            lex.EatKeyword "desc"
            SortDesc
        else
            SortAsc

    let sortList (lex: Lexer) =
        let rec addToList list =
            match aggregationFn lex with
            | Some aggFn ->
                let dir = sortDirection lex
                list |> SortList.addAggFn aggFn dir
            | _ ->
                let fld = lex.EatIdentity()
                let dir = sortDirection lex
                list |> SortList.addField fld dir
            |> fun nextlist ->
                if lex.MatchDelimiter ',' then
                    lex.EatDelimimiter ','
                    nextlist |> addToList
                else
                    nextlist

        [] |> SortList |> addToList

    let select (lex: Lexer) =
        lex.EatKeyword "select"
        let projs = projectList lex
        lex.EatKeyword "from"
        let tables = identityList lex

        let pred =
            if lex.MatchKeyword "where" then
                lex.EatKeyword "where"
                predicate lex
            else
                Predicate []

        let groupFields =
            if lex.MatchKeyword "group" then
                lex.EatKeyword "group"
                lex.EatKeyword "by"
                identityList lex
            else
                []

        let sortList =
            if lex.MatchKeyword "order" then
                lex.EatKeyword "order"
                lex.EatKeyword "by"
                sortList lex
            else
                SortList []

        QueryData
            (projs |> ProjectList.fieldNames,
             tables,
             pred,
             groupFields,
             projs |> ProjectList.aggregationFns,
             sortList |> SortList.fieldList)

    let insert (lex: Lexer) =
        lex.EatKeyword "insert"
        lex.EatKeyword "into"
        let tblname = lex.EatIdentity()
        lex.EatDelimimiter '('
        let flds = identityList lex
        lex.EatDelimimiter ')'
        lex.EatKeyword "values"
        lex.EatDelimimiter '('
        let vals = constantList lex
        lex.EatDelimimiter ')'
        InsertData(tblname, flds, vals)

    let delete (lex: Lexer) =
        lex.EatKeyword "delete"
        lex.EatKeyword "from"
        let tblname = lex.EatIdentity()

        let pred =
            if lex.MatchKeyword "where" then
                lex.EatKeyword "where"
                predicate lex
            else
                Predicate []

        DeleteData(tblname, pred)

    let modifyExpression (lex: Lexer) =
        if lex.MatchKeyword "add" then
            lex.EatKeyword "add"
            lex.EatDelimimiter '('
            let lhs = queryExpression lex
            lex.EatDelimimiter ','
            let rhs = queryExpression lex
            lex.EatDelimimiter ')'
            BinaryArithmeticExpression(AddOperator, lhs, rhs)
        elif lex.MatchKeyword "sub" then
            lex.EatKeyword "sub"
            lex.EatDelimimiter '('
            let lhs = queryExpression lex
            lex.EatDelimimiter ','
            let rhs = queryExpression lex
            lex.EatDelimimiter ')'
            BinaryArithmeticExpression(SubOperator, lhs, rhs)
        elif lex.MatchKeyword "mul" then
            lex.EatKeyword "mul"
            lex.EatDelimimiter '('
            let lhs = queryExpression lex
            lex.EatDelimimiter ','
            let rhs = queryExpression lex
            lex.EatDelimimiter ')'
            BinaryArithmeticExpression(MulOperator, lhs, rhs)
        elif lex.MatchKeyword "div" then
            lex.EatKeyword "div"
            lex.EatDelimimiter '('
            let lhs = queryExpression lex
            lex.EatDelimimiter ','
            let rhs = queryExpression lex
            lex.EatDelimimiter ')'
            BinaryArithmeticExpression(DivOperator, lhs, rhs)
        elif lex.MatchIdentity() then
            lex.EatIdentity() |> FieldNameExpression
        else
            constant lex |> ConstantExpression

    let modify (lex: Lexer) =
        lex.EatKeyword "update"
        let tblname = lex.EatIdentity()
        lex.EatKeyword "set"

        let rec eat (map: Map<string, Expression>) =
            if not (lex.MatchKeyword "where")
               && lex.MatchIdentity() then
                let fldname = lex.EatIdentity()
                lex.EatDelimimiter '='
                let newval = modifyExpression lex
                if lex.MatchDelimiter ',' then lex.EatDelimimiter ','
                map.Add(fldname, newval) |> eat
            else
                map

        let values = Map.empty |> eat

        let pred =
            if lex.MatchKeyword "where" then
                lex.EatKeyword "where"
                predicate lex
            else
                Predicate []

        ModifyData(tblname, pred, values)

    let fieldType (lex: Lexer) fieldName =
        let schema = Schema.newSchema ()
        if lex.MatchKeyword "int" then
            lex.EatKeyword "int"
            schema.AddField fieldName IntSqlType
            schema
        elif lex.MatchKeyword "bigint" then
            lex.EatKeyword "bigint"
            schema.AddField fieldName BigIntSqlType
            schema
        elif lex.MatchKeyword "double" then
            lex.EatKeyword "double"
            schema.AddField fieldName DoubleSqlType
            schema
        else
            lex.EatKeyword "varchar"
            lex.EatDelimimiter '('
            let arg = lex.EatNumericConstant()
            lex.EatDelimimiter ')'
            VarcharSqlType(int arg)
            |> schema.AddField fieldName
            schema

    let fieldDef (lex: Lexer) = lex.EatIdentity() |> fieldType lex

    let rec fieldDefs (lex: Lexer) =
        let schema = fieldDef lex
        if lex.MatchDelimiter ',' then
            lex.EatDelimimiter ','
            fieldDefs lex |> schema.AddAll
        schema

    let createTable (lex: Lexer) =
        lex.EatKeyword "table"
        let tblname = lex.EatIdentity()
        lex.EatDelimimiter '('
        let sch = fieldDefs lex
        lex.EatDelimimiter ')'
        CreateTableData(tblname, sch)

    let dropTable (lex: Lexer) =
        lex.EatKeyword "table"
        lex.EatIdentity() |> DropTableData

    let createIndex (lex: Lexer) =
        lex.EatKeyword "index"
        let idxname = lex.EatIdentity()
        lex.EatKeyword "on"
        let tblname = lex.EatIdentity()
        lex.EatDelimimiter '('
        let fldNames = identityList lex
        lex.EatDelimimiter ')'

        let idxType =
            if lex.MatchKeyword "using" then
                lex.EatKeyword "using"
                if lex.MatchKeyword "hash" then
                    lex.EatKeyword "hash"
                    IndexType.Hash
                elif lex.MatchKeyword "btree" then
                    lex.EatKeyword "btree"
                    IndexType.BTree
                else
                    failwith "Syntax error: index type"
            else
                IndexType.Hash

        CreateIndexData(idxname, idxType, tblname, fldNames)

    let dropIndex (lex: Lexer) =
        lex.EatKeyword "index"
        lex.EatIdentity() |> DropIndexData

    let createView (lex: Lexer) =
        lex.EatKeyword "view"
        let viewname = lex.EatIdentity()
        lex.EatKeyword "as"
        CreateViewData(viewname, lex.Tail())

    let dropView (lex: Lexer) =
        lex.EatKeyword "view"
        lex.EatIdentity() |> DropViewData

    let queryCommand cmd = Lexer cmd |> select

    let updateCommand cmd =
        let lex = Lexer cmd
        if lex.MatchKeyword "insert" then
            insert lex
        elif lex.MatchKeyword "delete" then
            delete lex
        elif lex.MatchKeyword "update" then
            modify lex
        elif lex.MatchKeyword "create" then
            lex.EatKeyword "create"
            if lex.MatchKeyword "table" then createTable lex
            elif lex.MatchKeyword "index" then createIndex lex
            elif lex.MatchKeyword "view" then createView lex
            else failwith "Syntax error: create"
        elif lex.MatchKeyword "drop" then
            lex.EatKeyword "drop"
            if lex.MatchKeyword "table" then dropTable lex
            elif lex.MatchKeyword "index" then dropIndex lex
            elif lex.MatchKeyword "view" then dropView lex
            else failwith "Syntax error: drop"
        else
            failwith "Syntax error: command"
