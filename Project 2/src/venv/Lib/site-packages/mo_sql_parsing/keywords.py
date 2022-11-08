# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

# SQL CONSTANTS
from mo_parsing import *

from mo_sql_parsing.utils import SQL_NULL, int_num, to_json_call, ansi_string, ansi_ident

NULL = Keyword("null", caseless=True) / (lambda: SQL_NULL)
TRUE = Keyword("true", caseless=True) / (lambda: True)
FALSE = Keyword("false", caseless=True) / (lambda: False)
NOCASE = Keyword("nocase", caseless=True)
ASC = Keyword("asc", caseless=True)
DESC = Keyword("desc", caseless=True)

# SIMPLE KEYWORDS
AS = Keyword("as", caseless=True).suppress()
ALL = Keyword("all", caseless=True)
BY = Keyword("by", caseless=True).suppress()
CAST = Keyword("cast", caseless=True)
CONSTRAINT = Keyword("constraint", caseless=True).suppress()
CREATE = Keyword("create", caseless=True).suppress()
CROSS = Keyword("cross", caseless=True)
DISTINCT = Keyword("distinct", caseless=True)
EXCEPT = Keyword("except", caseless=True)
FROM = Keyword("from", caseless=True).suppress()
FULL = Keyword("full", caseless=True)
GROUP = Keyword("group", caseless=True).suppress()
HAVING = Keyword("having", caseless=True).suppress()
INNER = Keyword("inner", caseless=True)
INTERVAL = Keyword("interval", caseless=True)
JOIN = Keyword("join", caseless=True)
LEFT = Keyword("left", caseless=True)
LIKE = Keyword("like", caseless=True)
LIMIT = Keyword("limit", caseless=True).suppress()
MINUS = Keyword("minus", caseless=True)
OFFSET = Keyword("offset", caseless=True).suppress()
ON = Keyword("on", caseless=True).suppress()
ORDER = Keyword("order", caseless=True).suppress()
OUTER = Keyword("outer", caseless=True)
OVER = Keyword("over", caseless=True).suppress()
PARTITION = Keyword("partition", caseless=True).suppress()
# PERCENT = Keyword("percent", caseless=True).suppress()
RIGHT = Keyword("right", caseless=True)
RLIKE = Keyword("rlike", caseless=True)
SELECT = Keyword("select", caseless=True).suppress()
TABLE = Keyword("table", caseless=True).suppress()
THEN = Keyword("then", caseless=True).suppress()
TOP = Keyword("top", caseless=True).suppress()
UNION = Keyword("union", caseless=True)
INTERSECT = Keyword("intersect", caseless=True)
USING = Keyword("using", caseless=True).suppress()
WHEN = Keyword("when", caseless=True).suppress()
WHERE = Keyword("where", caseless=True).suppress()
WITH = Keyword("with", caseless=True).suppress()
WITHIN = Keyword("within", caseless=True).suppress()
PRIMARY = Keyword("primary", caseless=True).suppress()
FOREIGN = Keyword("foreign", caseless=True).suppress()
KEY = Keyword("key", caseless=True).suppress()
UNIQUE = Keyword("unique", caseless=True).suppress()
INDEX = Keyword("index", caseless=True).suppress()
REFERENCES = Keyword("references", caseless=True).suppress()

PRIMARY_KEY = Group(PRIMARY + KEY).set_parser_name("primary_key")
FOREIGN_KEY = Group(FOREIGN + KEY).set_parser_name("foreign_key")

# SIMPLE OPERATORS
CONCAT = Literal("||").set_parser_name("concat")
MUL = Literal("*").set_parser_name("mul")
DIV = Literal("/").set_parser_name("div")
MOD = Literal("%").set_parser_name("mod")
NEG = Literal("-").set_parser_name("neg")
ADD = Literal("+").set_parser_name("add")
SUB = Literal("-").set_parser_name("sub")
BINARY_NOT = Literal("~").set_parser_name("binary_not")
BINARY_AND = Literal("&").set_parser_name("binary_and")
BINARY_OR = Literal("|").set_parser_name("binary_or")
GTE = Literal(">=").set_parser_name("gte")
LTE = Literal("<=").set_parser_name("lte")
LT = Literal("<").set_parser_name("lt")
GT = Literal(">").set_parser_name("gt")
EQ = (
    Literal("==") | Literal("=")
).set_parser_name("eq")  # conservative equality  https://github.com/klahnakoski/jx-sqlite/blob/dev/docs/Logical%20Equality.md#definitions
DEQ = (
    Literal("<=>").set_parser_name("eq!")
)  # https://sparkbyexamples.com/apache-hive/hive-relational-arithmetic-logical-operators/
NEQ = (Literal("!=") | Literal("<>")).set_parser_name("neq")

AND = Keyword("and", caseless=True)
BETWEEN = Keyword("between", caseless=True)
CASE = Keyword("case", caseless=True).suppress()
COLLATE = Keyword("collate", caseless=True)
END = Keyword("end", caseless=True)
ELSE = Keyword("else", caseless=True).suppress()
IN = Keyword("in", caseless=True)
IS = Keyword("is", caseless=True)
NOT = Keyword("not", caseless=True)
OR = Keyword("or", caseless=True)

# COMPOUND KEYWORDS
CREATE_TABLE = Group(CREATE + TABLE).set_parser_name("create_table")
CROSS_JOIN = (CROSS + JOIN).set_parser_name("cross join")
FULL_JOIN = (FULL + JOIN).set_parser_name("full join")
FULL_OUTER_JOIN = (FULL + OUTER + JOIN).set_parser_name("full outer join")
GROUP_BY = Group(GROUP + BY).set_parser_name("group by")
INNER_JOIN = (INNER + JOIN).set_parser_name("inner join")
LEFT_JOIN = (LEFT + JOIN).set_parser_name("left join")
LEFT_OUTER_JOIN = (LEFT + OUTER + JOIN).set_parser_name("left outer join")
ORDER_BY = Group(ORDER + BY).set_parser_name("order by")
PARTITION_BY = Group(PARTITION + BY).set_parser_name("partition by")
RIGHT_JOIN = (RIGHT + JOIN).set_parser_name("right join")
RIGHT_OUTER_JOIN = (RIGHT + OUTER + JOIN).set_parser_name("right outer join")
SELECT_DISTINCT = Group(SELECT + DISTINCT).set_parser_name("select distinct")
UNION_ALL = (UNION + ALL).set_parser_name("union_all")
WITHIN_GROUP = Group(WITHIN + GROUP).set_parser_name("within_group")

# COMPOUND OPERATORS
AT_TIME_ZONE = Group(
    Keyword("at", caseless=True)
    + Keyword("time", caseless=True)
    + Keyword("zone", caseless=True)
)
NOT_BETWEEN = Group(NOT + BETWEEN).set_parser_name("not_between")
NOT_LIKE = Group(NOT + LIKE).set_parser_name("not_like")
NOT_RLIKE = Group(NOT + RLIKE).set_parser_name("not_rlike")
NOT_IN = Group(NOT + IN).set_parser_name("nin")
IS_NOT = Group(IS + NOT).set_parser_name("is_not")

_SIMILAR = Keyword("similar", caseless=True)
_TO = Keyword("to", caseless=True)
SIMILAR_TO = Group(_SIMILAR + _TO).set_parser_name("similar_to")
NOT_SIMILAR_TO = Group(NOT + _SIMILAR + _TO).set_parser_name("not_similar_to")

RESERVED = MatchFirst([
    # ONY INCLUDE SINGLE WORDS
    ALL,
    AND,
    AS,
    ASC,
    BETWEEN,
    BY,
    CASE,
    COLLATE,
    CONSTRAINT,
    CREATE,
    CROSS_JOIN,
    CROSS,
    DESC,
    DISTINCT,
    EXCEPT,
    ELSE,
    END,
    FALSE,
    FOREIGN,
    FROM,
    FULL,
    GROUP_BY,
    GROUP,
    HAVING,
    IN,
    INDEX,
    INNER,
    INTERSECT,
    INTERVAL,
    IS_NOT,
    IS,
    JOIN,
    KEY,
    LEFT,
    LIKE,
    LIMIT,
    MINUS,
    NOCASE,
    NOT,
    NULL,
    OFFSET,
    ON,
    OR,
    ORDER,
    OUTER,
    OVER,
    PARTITION,
    PRIMARY,
    REFERENCES,
    RIGHT,
    RLIKE,
    SELECT,
    THEN,
    TRUE,
    UNION,
    UNIQUE,
    USING,
    WHEN,
    WHERE,
    WITH,
    WITHIN,
])

LB = Literal("(").suppress()
RB = Literal(")").suppress()

join_keywords = {
    "join",
    "full join",
    "cross join",
    "inner join",
    "left join",
    "right join",
    "full outer join",
    "right outer join",
    "left outer join",
}

precedence = {
    # https://www.sqlite.org/lang_expr.html
    "literal": -1,
    "cast": 0,
    "collate": 0,
    "concat": 1,
    "mul": 2,
    "div": 1.5,
    "mod": 2,
    "neg": 3,
    "add": 3,
    "sub": 2.5,
    "binary_not": 4,
    "binary_and": 4,
    "binary_or": 4,
    "gte": 5,
    "lte": 5,
    "lt": 5,
    "gt": 6,
    "eq": 7,
    "neq": 7,
    "at_time_zone": 8,
    "between": 8,
    "not_between": 8,
    "in": 8,
    "nin": 8,
    "is": 8,
    "like": 8,
    "not_like": 8,
    "rlike": 8,
    "not_rlike": 8,
    "similar_to": 8,
    "not_similar_to": 8,
    "and": 10,
    "or": 11,
    "select": 30,
    "from": 30,
    "window": 35,
    "union": 40,
    "union_all": 40,
    "except": 40,
    "minus": 40,
    "intersect": 40,
    "order": 50,
}


KNOWN_OPS = [
    COLLATE,
    CONCAT,
    MUL | DIV | MOD,
    NEG,
    ADD | SUB,
    BINARY_NOT,
    BINARY_AND,
    BINARY_OR,
    GTE | LTE | LT | GT,
    EQ | NEQ | DEQ,
    AT_TIME_ZONE,
    (BETWEEN, AND),
    (NOT_BETWEEN, AND),
    IN,
    NOT_IN,
    IS_NOT,
    IS,
    LIKE,
    NOT_LIKE,
    RLIKE,
    NOT_RLIKE,
    SIMILAR_TO,
    NOT_SIMILAR_TO,
    NOT,
    AND,
    OR,
]

times = ["now", "today", "tomorrow", "eod"]

durations = {
    "microseconds": "microsecond",
    "microsecond": "microsecond",
    "microsecs": "microsecond",
    "microsec": "microsecond",
    "useconds": "microsecond",
    "usecond": "microsecond",
    "usecs": "microsecond",
    "usec": "microsecond",
    "us": "microsecond",
    "milliseconds": "millisecond",
    "millisecond": "millisecond",
    "millisecon": "millisecond",
    "mseconds": "millisecond",
    "msecond": "millisecond",
    "millisecs": "millisecond",
    "millisec": "millisecond",
    "msecs": "millisecond",
    "msec": "millisecond",
    "ms": "millisecond",
    "seconds": "second",
    "second": "second",
    "secs": "second",
    "sec": "second",
    "s": "second",
    "minutes": "minute",
    "minute": "minute",
    "mins": "minute",
    "min": "minute",
    "m": "minute",
    "hours": "hour",
    "hour": "hour",
    "hrs": "hour",
    "hr": "hour",
    "h": "hour",
    "days": "day",
    "day": "day",
    "d": "day",
    "dayofweek": "dow",
    "dow": "dow",
    "weekday": "dow",
    "weeks": "week",
    "week": "week",
    "w": "week",
    "months": "month",
    "mons": "month",
    "mon": "month",
    "quarters": "quarter",
    "quarter": "quarter",
    "years": "year",
    "year": "year",
    "decades": "decade",
    "decade": "decade",
    "decs": "decade",
    "dec": "decade",
    "centuries": "century",
    "century": "century",
    "cents": "century",
    "cent": "century",
    "c": "century",
    "millennia": "millennium",
    "millennium": "millennium",
    "mils": "millennium",
    "mil": "millennium",
    "epoch": "epoch",
}

_size = Optional(LB + int_num("params") + RB)
_sizes = Optional(LB + int_num("params") + "," + int_num("params") + RB)

# KNOWN TYPES
ARRAY = Group(Keyword("array", caseless=True)("op")) / to_json_call
BIGINT = Group(Keyword("bigint", caseless=True)("op")) / to_json_call
BOOL = Group(Keyword("bool", caseless=True)("op")) / to_json_call
BOOLEAN = Group(Keyword("boolean", caseless=True)("op")) / to_json_call
DOUBLE = Group(Keyword("double", caseless=True)("op")) / to_json_call
FLOAT64 = Group(Keyword("float64", caseless=True)("op")) / to_json_call
FLOAT = Group(Keyword("float", caseless=True)("op")) / to_json_call
GEOMETRY = Group(Keyword("geometry", caseless=True)("op")) / to_json_call
INTEGER = Group(Keyword("integer", caseless=True)("op")) / to_json_call
INT = Group(Keyword("int", caseless=True)("op")) / to_json_call
INT32 = Group(Keyword("int32", caseless=True)("op")) / to_json_call
INT64 = Group(Keyword("int64", caseless=True)("op")) / to_json_call
REAL = Group(Keyword("real", caseless=True)("op")) / to_json_call
TEXT = Group(Keyword("text", caseless=True)("op")) / to_json_call
SMALLINT = Group(Keyword("smallint", caseless=True)("op")) / to_json_call
STRING = Group(Keyword("string", caseless=True)("op")) / to_json_call
STRUCT = Group(Keyword("struct", caseless=True)("op")) / to_json_call

BLOB = (Keyword("blob", caseless=True)("op") + _size) / to_json_call
BYTES = (Keyword("bytes", caseless=True)("op") + _size) / to_json_call
CHAR = (Keyword("char", caseless=True)("op") + _size) / to_json_call
VARCHAR = (Keyword("varchar", caseless=True)("op") + _size) / to_json_call
VARBINARY = (Keyword("varbinary", caseless=True)("op") + _size) / to_json_call
TINYINT = (Keyword("tinyint", caseless=True)("op") + _size) / to_json_call

DECIMAL = (Keyword("decimal", caseless=True)("op") + _sizes) / to_json_call
DOUBLE_PRECISION = (
    Keyword("double", caseless=True) + Keyword("precision", caseless=True)("op")
) / (lambda: {"double_precision": {}})
NUMERIC = (Keyword("numeric", caseless=True)("op") + _sizes) / to_json_call


DATE = Keyword("date", caseless=True)
DATETIME = Keyword("datetime", caseless=True)
TIME = Keyword("time", caseless=True)
TIMESTAMP = Keyword("timestamp", caseless=True)
TIMESTAMPTZ = Keyword("timestamptz", caseless=True)
TIMETZ = Keyword("timetz", caseless=True)

time_functions = DATE | DATETIME | TIME | TIMESTAMP | TIMESTAMPTZ | TIMETZ

# KNOWNN TIME TYPES
_format = Optional((ansi_string | ansi_ident)("params"))

DATE_TYPE = (DATE("op") + _format) / to_json_call
DATETIME_TYPE = (DATETIME("op") + _format) / to_json_call
TIME_TYPE = (TIME("op") + _format) / to_json_call
TIMESTAMP_TYPE = (TIMESTAMP("op") + _format) / to_json_call
TIMESTAMPTZ_TYPE = (TIMESTAMPTZ("op") + _format) / to_json_call
TIMETZ_TYPE = (TIMETZ("op") + _format) / to_json_call

known_types = MatchFirst([
    ARRAY,
    BIGINT,
    BOOL,
    BOOLEAN,
    BLOB,
    BYTES,
    CHAR,
    DATE_TYPE,
    DATETIME_TYPE,
    DECIMAL,
    DOUBLE_PRECISION,
    DOUBLE,
    FLOAT64,
    FLOAT,
    GEOMETRY,
    INTEGER,
    INT,
    INT32,
    INT64,
    NUMERIC,
    REAL,
    TEXT,
    SMALLINT,
    STRING,
    STRUCT,
    TIME_TYPE,
    TIMESTAMP_TYPE,
    TIMESTAMPTZ_TYPE,
    TIMETZ_TYPE,
    VARCHAR,
    VARBINARY,
    TINYINT,
])

CASTING = (Literal("::").suppress() + known_types("params")).set_parser_name("cast")
KNOWN_OPS = [CASTING] + KNOWN_OPS
unary_ops = {
    NEG: RIGHT_ASSOC,
    NOT: RIGHT_ASSOC,
    BINARY_NOT: RIGHT_ASSOC,
    CASTING: LEFT_ASSOC,
}

from mo_sql_parsing import utils

utils.unary_ops = unary_ops
del utils
