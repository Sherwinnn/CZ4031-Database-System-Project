# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from mo_parsing.helpers import delimited_list, restOfLine
from mo_parsing.whitespaces import NO_WHITESPACE, Whitespace

from mo_sql_parsing.keywords import *
from mo_sql_parsing.utils import *
from mo_sql_parsing.windows import window


def combined_parser():
    combined_ident = Combine(delimited_list(
        ansi_ident | mysql_backtick_ident | sqlserver_ident | Word(IDENT_CHAR),
        separator=".",
        combine=True,
    )).set_parser_name("identifier")

    return parser(ansi_string, combined_ident)


def mysql_parser():
    mysql_string = ansi_string | mysql_doublequote_string
    mysql_ident = Combine(delimited_list(
        mysql_backtick_ident | sqlserver_ident | Word(IDENT_CHAR),
        separator=".",
        combine=True,
    )).set_parser_name("mysql identifier")

    return parser(mysql_string, mysql_ident)


def parser(literal_string, ident):
    with Whitespace() as engine:
        engine.add_ignore(Literal("--") + restOfLine)
        engine.add_ignore(Literal("#") + restOfLine)

        var_name = ~RESERVED + ident

        # EXPRESSIONS
        column_definition = Forward()
        column_type = Forward()
        expr = Forward()

        # CASE
        case = (
            CASE
            + Group(ZeroOrMore(
                (WHEN + expr("when") + THEN + expr("then")) / to_when_call
            ))("case")
            + Optional(ELSE + expr("else"))
            + END
        ) / to_case_call

        # SWITCH
        switch = (
            CASE
            + expr("value")
            + Group(ZeroOrMore(
                (WHEN + expr("when") + THEN + expr("then")) / to_when_call
            ))("case")
            + Optional(ELSE + expr("else"))
            + END
        ) / to_switch_call

        # CAST
        cast = (
            Group(CAST("op") + LB + expr("params") + AS + known_types("params") + RB)
            / to_json_call
        )

        # TRIM
        trim = (
            Group(
                Keyword("trim", caseless=True).suppress()
                + LB
                + expr("chars")
                + Optional(FROM + expr("from"))
                + RB
            ).set_parser_name("trim")
            / to_trim_call
        )

        _standard_time_intervals = MatchFirst([
            Keyword(d, caseless=True) / (lambda t: durations[t[0].lower()])
            for d in durations.keys()
        ]).set_parser_name("duration")("params")

        duration = (real_num | int_num)("params") + _standard_time_intervals

        interval = (
            INTERVAL + ("'" + delimited_list(duration) + "'" | duration)
        ) / to_interval_call

        timestamp = (
            time_functions("op")
            + (
                literal_string("params")
                | MatchFirst([
                    Keyword(t, caseless=True) / (lambda t: t.lower()) for t in times
                ])("params")
            )
        ) / to_json_call

        extract = (
            Keyword("extract", caseless=True)("op")
            + LB
            + (_standard_time_intervals | expr("params"))
            + FROM
            + expr("params")
            + RB
        ) / to_json_call

        named_column = Group(
            Group(expr)("value") + Optional(Optional(AS) + Group(var_name))("name")
        )

        distinct = (
            DISTINCT("op") + delimited_list(named_column)("params")
        ) / to_json_call

        ordered_sql = Forward()

        call_function = (
            ident("op")
            + LB
            + Optional(Group(ordered_sql) | delimited_list(Group(expr)))("params")
            + Optional(
                Keyword("ignore", caseless=True) + Keyword("nulls", caseless=True)
            )("ignore_nulls")
            + RB
        ) / to_json_call

        with NO_WHITESPACE:

            def scale(tokens):
                return {"mul": [tokens[0], tokens[1]]}

            scale_function = ((real_num | int_num) + call_function) / scale
            scale_ident = ((real_num | int_num) + ident) / scale

        compound = (
                NULL
                | TRUE
                | FALSE
                | NOCASE
                | interval
                | timestamp
                | extract
                | case
                | switch
                | cast
                | distinct
                | trim
                | (LB + Group(ordered_sql) + RB)
                | (LB + Group(delimited_list(expr)) / to_tuple_call + RB)
                | literal_string.set_parser_name("string")
                | hex_num.set_parser_name("hex")
                | scale_function
                | scale_ident
                | real_num.set_parser_name("float")
                | int_num.set_parser_name("int")
                | call_function
                # | known_types
                | Combine(var_name + Optional(".*"))
        )

        sort_column = expr("value").set_parser_name("sort1") + Optional(
            DESC("sort") | ASC("sort")
        ) | expr("value").set_parser_name("sort2")

        expr << (
            (
                Literal("*")
                | infix_notation(
                    compound,
                    [
                        (
                            o,
                            1 if o in unary_ops else (3 if isinstance(o, tuple) else 2),
                            unary_ops[o] if o in unary_ops else LEFT_ASSOC,
                            to_json_operator,
                        )
                        for o in KNOWN_OPS
                    ],
                ).set_parser_name("expression")
            )("value")
            + Optional(window(expr, sort_column))
        ) / to_expression_call

        alias = (
            Group(var_name) + Optional(LB + delimited_list(ident("col")) + RB)
        )("name").set_parser_name("alias") / to_alias

        select_column = (
            Group(
                Group(expr).set_parser_name("expression1")("value")
                + Optional(Optional(AS) + alias)
                | Literal("*")("value")
            ).set_parser_name("column")
            / to_select_call
        )

        table_source = (
            (
                (LB + ordered_sql + RB) | call_function
            )("value").set_parser_name("table source")
            + Optional(Optional(AS) + alias)
            | (var_name("value").set_parser_name("table name") + Optional(AS) + alias)
            | var_name.set_parser_name("table name")
        )

        join = (
            Group(
                CROSS_JOIN
                | FULL_JOIN
                | FULL_OUTER_JOIN
                | INNER_JOIN
                | JOIN
                | LEFT_JOIN
                | LEFT_OUTER_JOIN
                | RIGHT_JOIN
                | RIGHT_OUTER_JOIN
            )("op")
            + Group(table_source)("join")
            + Optional((ON + expr("on")) | (USING + expr("using")))
        ) / to_join_call

        selection = (
            SELECT
            + DISTINCT
            + ON
            + LB
            + delimited_list(select_column)("distinct_on")
            + RB
            + delimited_list(select_column)("select")
            | SELECT + DISTINCT + delimited_list(select_column)("select_distinct")
            | (
                SELECT
                + Optional(
                    TOP
                    + expr("value")
                    + Optional(Keyword("percent", caseless=True))("percent")
                    + Optional(WITH + Keyword("ties", caseless=True))("ties")
                )("top")
                / to_top_clause
                + delimited_list(select_column)("select")
            )
        )

        unordered_sql = Group(
            selection
            + Optional(
                (FROM + delimited_list(Group(table_source)) + ZeroOrMore(join))("from")
                + Optional(WHERE + expr("where"))
                + Optional(GROUP_BY + delimited_list(Group(named_column))("groupby"))
                + Optional(HAVING + expr("having"))
            )
        ).set_parser_name("unordered sql")

        ordered_sql << (
            (
                unordered_sql
                + ZeroOrMore(
                    Group(UNION_ALL | UNION | INTERSECT | EXCEPT | MINUS)
                    + unordered_sql
                )
            )("union")
            + Optional(ORDER_BY + delimited_list(Group(sort_column))("orderby"))
            + Optional(LIMIT + expr("limit"))
            + Optional(OFFSET + expr("offset"))
        ).set_parser_name("ordered sql") / to_union_call

        statement = Forward()
        statement << (
            Optional(
                WITH
                + delimited_list(Group(
                    var_name("name") + AS + LB + (statement | expr)("value") + RB
                ))
            )("with")
            + Group(ordered_sql)("query")
        ) / to_statement

        #####################################################################
        # CREATE TABLE
        #####################################################################
        create_stmt = Forward()

        BigQuery_STRUCT = (
            Keyword("struct", caseless=True)("op")
            + Literal("<").suppress()
            + delimited_list(column_definition)("params")
            + Literal(">").suppress()
        ) / to_json_call

        BigQuery_ARRAY = (
            Keyword("array", caseless=True)("op")
            + Literal("<").suppress()
            + delimited_list(column_type)("params")
            + Literal(">").suppress()
        ) / to_json_call

        column_type << (
            BigQuery_STRUCT
            | BigQuery_ARRAY
            | Group(ident("op") + Optional(LB + delimited_list(int_num)("params") + RB))
            / to_json_call
        )

        column_def_references = (
            REFERENCES
            + var_name("table")
            + LB
            + delimited_list(var_name)("columns")
            + RB
        )("references")

        column_def_check = Keyword("check", caseless=True).suppress() + LB + expr + RB

        column_def_default = Keyword(
            "default", caseless=True
        ).suppress() + expr("default")

        column_options = ZeroOrMore(Group(
            (NOT + NULL) / (lambda: "not null")
            | NULL / (lambda t: "nullable")
            | Keyword("unique", caseless=True)
            | Keyword("primary key", caseless=True)
            | column_def_references
            | column_def_check("check")
            | column_def_default
        )).set_parser_name("column_options")

        column_definition << Group(
            var_name("name") / (lambda t: t[0].lower())
            + column_type("type")
            + Optional(column_options)("option")
        ).set_parser_name("column_definition")

        # MySQL's index_type := Using + ( "BTREE" | "HASH" )
        index_type = Optional(USING + ident("index_type"))

        index_column_names = LB + delimited_list(var_name("columns")) + RB

        column_def_foreign_key = FOREIGN_KEY + Optional(
            var_name("index_name") + index_column_names + column_def_references
        )

        index_options = ZeroOrMore(var_name)("table_constraint_options")

        table_constraint_definition = Optional(CONSTRAINT + var_name("name")) + (
            (
                PRIMARY_KEY + index_type + index_column_names + index_options
            )("primary_key")
            | (
                UNIQUE
                + Optional(INDEX | KEY)
                + Optional(var_name("index_name"))
                + index_type
                + index_column_names
                + index_options
            )("unique")
            | (
                (INDEX | KEY)
                + Optional(var_name("index_name"))
                + index_type
                + index_column_names
                + index_options
            )("index")
            | column_def_check("check")
            | column_def_foreign_key("foreign_key")
        )

        table_element = (
            column_definition("columns") | table_constraint_definition("constraint")
        )

        create_stmt << (
            CREATE_TABLE
            + (
                var_name("name")
                + Optional(LB + delimited_list(table_element) + RB)
                + Optional(
                    AS.suppress() + infix_notation(statement, [])
                )("select_statement")
            )("create table")
        )

        return (statement | create_stmt).finalize()
