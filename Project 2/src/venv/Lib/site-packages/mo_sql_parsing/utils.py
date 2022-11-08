# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import, division, unicode_literals

import ast

from mo_dots import is_data, is_null, Data, from_data
from mo_future import text, number_types, binary_type
from mo_parsing import *
from mo_parsing.utils import is_number, listwrap

unary_ops = None


class Call(object):
    __slots__ = ["op", "args", "kwargs"]

    def __init__(self, op, args, kwargs):
        self.op = op
        self.args = args
        self.kwargs = kwargs


IDENT_CHAR = Regex("[@_$0-9A-Za-zÀ-ÖØ-öø-ƿ]").expr.parser_config.include
SQL_NULL = Call("null", [], {})

null_locations = []


def simple_op(op, args, kwargs):
    if not args:
        kwargs[op] = {}
    else:
        kwargs[op] = args
    return kwargs


def normal_op(op, args, kwargs):
    output = Data(op=op)
    args = listwrap(args)
    if args and (not isinstance(args[0], dict) or args[0]):
        output.args = args
    if kwargs:
        output.kwargs = kwargs
    return from_data(output)


scrub_op = simple_op


def scrub(result):
    if result is SQL_NULL:
        return SQL_NULL
    elif result == None:
        return None
    elif isinstance(result, text):
        return result
    elif isinstance(result, binary_type):
        return result.decode("utf8")
    elif isinstance(result, number_types):
        return result
    elif isinstance(result, Call):
        kwargs = scrub(result.kwargs)
        args = scrub(result.args)
        if args is SQL_NULL:
            null_locations.append((kwargs, result.op))
        return scrub_op(result.op, args, kwargs)
    elif isinstance(result, dict) and not result:
        return result
    elif isinstance(result, list):
        output = [rr for r in result for rr in [scrub(r)]]

        if not output:
            return None
        elif len(output) == 1:
            return output[0]
        else:
            for i, v in enumerate(output):
                if v is SQL_NULL:
                    null_locations.append((output, i))
            return output
    else:
        # ATTEMPT A DICT INTERPRETATION
        try:
            kv_pairs = list(result.items())
        except Exception as c:
            print(c)
        output = {k: vv for k, v in kv_pairs for vv in [scrub(v)] if not is_null(vv)}
        if isinstance(result, dict) or output:
            for k, v in output.items():
                if v is SQL_NULL:
                    null_locations.append((output, k))
            return output
        return scrub(list(result))


def _chunk(values, size):
    acc = []
    for v in values:
        acc.append(v)
        if len(acc) == size:
            yield acc
            acc = []
    if acc:
        yield acc


def to_json_operator(tokens):
    # ARRANGE INTO {op: params} FORMAT
    length = len(tokens.tokens)
    if length == 2:
        if tokens.tokens[1].type.parser_name == "cast":
            return Call("cast", list(tokens), {})
        # UNARY OPERATOR
        op = tokens.tokens[0].type.parser_name
        if op == "neg" and is_number(tokens[1]):
            return -tokens[1]
        return Call(op, [tokens[1]], {})
    elif length == 5:
        # TRINARY OPERATOR
        return Call(
            tokens.tokens[1].type.parser_name, [tokens[0], tokens[2], tokens[4]], {}
        )

    op = tokens[1]
    if not isinstance(op, text):
        op = op.type.parser_name
    op = binary_ops.get(op, op)
    if op == "eq":
        if tokens[2] is SQL_NULL:
            return Call("missing", tokens[0], {})
        elif tokens[0] is SQL_NULL:
            return Call("missing", tokens[2], {})
    elif op == "neq":
        if tokens[2] is SQL_NULL:
            return Call("exists", tokens[0], {})
        elif tokens[0] is SQL_NULL:
            return Call("exists", tokens[2], {})
    elif op == "is":
        if tokens[2] is SQL_NULL:
            return Call("missing", tokens[0], {})
        else:
            return Call("exists", tokens[0], {})
    elif op == "is_not":
        if tokens[2] is SQL_NULL:
            return Call("exists", tokens[0], {})
        else:
            return Call("missing", tokens[0], {})

    operands = [tokens[0], tokens[2]]
    binary_op = Call(op, operands, {})

    if op in {"add", "mul", "and", "or"}:
        # ASSOCIATIVE OPERATORS
        acc = []
        for operand in operands:
            while isinstance(operand, ParseResults) and isinstance(operand.type, Group):
                # PARENTHESES CAUSE EXTRA GROUP LAYERS
                operand = operand[0]

            if isinstance(operand, Call) and operand.op == op:
                acc.extend(operand.args)
            elif isinstance(operand, list):
                acc.append(operand)
            elif isinstance(operand, dict) and operand.get(op):
                acc.extend(operand.get(op))
            else:
                acc.append(operand)
        binary_op = Call(op, acc, {})
    return binary_op


def to_tuple_call(tokens):
    # IS THIS ONE VALUE IN (), OR MANY?
    tokens = list(tokens)
    if len(tokens) == 1:
        return [tokens[0]]
    if all(isinstance(r, number_types) for r in tokens):
        return [tokens]
    if all(
        isinstance(r, number_types) or (is_data(r) and "literal" in r.keys())
        for r in tokens
    ):
        candidate = {"literal": [r["literal"] if is_data(r) else r for r in tokens]}
        return candidate

    return [tokens]


binary_ops = {
    "::": "cast",
    "COLLATE": "collate",
    "||": "concat",
    "*": "mul",
    "/": "div",
    "%": "mod",
    "+": "add",
    "-": "sub",
    "&": "binary_and",
    "|": "binary_or",
    "<": "lt",
    "<=": "lte",
    ">": "gt",
    ">=": "gte",
    "=": "eq",
    "==": "eq",
    "<=>": "eq!",  # https://sparkbyexamples.com/apache-hive/hive-relational-arithmetic-logical-operators/
    "!=": "neq",
    "<>": "neq",
    "not in": "nin",
    "in": "in",
    "is_not": "neq",
    "is": "eq",
    "similar_to": "similar_to",
    "like": "like",
    "rlike": "rlike",
    "not like": "not_like",
    "not_like": "not_like",
    "not rlike": "not_rlike",
    "not_rlike": "not_rlike",
    "not_simlilar_to": "not_similar_to",
    "or": "or",
    "and": "and",
    "union": "union",
    "union_all": "union_all",
    "union all": "union_all",
    "except": "except",
    "minus": "minus",
    "intersect": "intersect",
}

is_set_op = ("union", "union_all", "except", "minus", "intersect")


def to_trim_call(tokens):
    frum = tokens["from"]
    if not frum:
        return Call("trim", [tokens["chars"]], {})
    return Call("trim", [frum], {"characters": tokens["chars"]})


def to_json_call(tokens):
    # ARRANGE INTO {op: params} FORMAT
    op = tokens["op"].lower()
    op = binary_ops.get(op, op)

    params = listwrap(tokens["params"])
    if tokens["ignore_nulls"]:
        ignore_nulls = True
    else:
        ignore_nulls = None

    return ParseResults(
        tokens.type,
        tokens.start,
        tokens.end,
        [Call(op, params, {"ignore_nulls": ignore_nulls})],
        tokens.failures,
    )


def to_interval_call(tokens):
    # ARRANGE INTO {interval: [amount, type]} FORMAT
    params = tokens["params"]
    if not params:
        params = {}
    if params.length() == 2:
        return Call("interval", params, {})

    return Call("add", [Call("interval", p, {}) for p in _chunk(params, size=2)], {})


def to_case_call(tokens):
    cases = list(tokens["case"])
    elze = tokens["else"]
    if elze != None:
        cases.append(elze)
    return Call("case", cases, {})


def to_switch_call(tokens):
    # CONVERT TO CLASSIC CASE STATEMENT
    value = tokens["value"]
    acc = []
    for c in list(tokens["case"]):
        acc.append(Call("when", [Call("eq", [value] + c.args, {})], c.kwargs))
    elze = tokens["else"]
    if elze != None:
        acc.append(elze)
    return Call("case", acc, {})


def to_when_call(tokens):
    tok = tokens
    return Call("when", [tok["when"]], {"then": tok["then"]})


def to_join_call(tokens):
    op = " ".join(tokens["op"])
    if tokens["join"]["name"]:
        output = {op: {
            "name": tokens["join"]["name"],
            "value": tokens["join"]["value"],
        }}
    else:
        output = {op: tokens["join"]}

    output["on"] = tokens["on"]
    output["using"] = tokens["using"]
    return output


def to_expression_call(tokens):
    over = tokens["over"]
    within = tokens["within"]
    if over or within:
        return

    expr = ParseResults(
        tokens.type,
        tokens.start,
        tokens.end,
        listwrap(tokens["value"]),
        tokens.failures,
    )
    return expr


def to_alias(tokens):
    cols = tokens["col"]
    name = tokens[0][0]
    if cols:
        return {name: cols}
    return name


def to_top_clause(tokens):
    value = tokens["value"]
    if not value:
        return None
    elif tokens["ties"]:
        output = {}
        output["ties"] = True
        if tokens["percent"]:
            output["percent"] = value
        else:
            output["value"] = value
        return output
    elif tokens["percent"]:
        return {"percent": value}
    else:
        return [value]


def to_select_call(tokens):
    value = tokens["value"]
    if value.value() == "*":
        return ["*"]

    if value["over"] or value["within"]:
        output = ParseResults(
            tokens.type, tokens.start, tokens.end, value.tokens, tokens.failures
        )
        output["name"] = tokens["name"]
        return output
    else:
        return


def to_union_call(tokens):
    unions = tokens["union"]
    if unions.type.parser_name == "unordered sql":
        output = {k: v for k, v in unions.items()}  # REMOVE THE Group()
    else:
        unions = list(unions)
        sources = [unions[i] for i in range(0, len(unions), 2)]
        operators = ["_".join(unions[i]) for i in range(1, len(unions), 2)]
        acc = sources[0]
        last_union = None
        for op, so in list(zip(operators, sources[1:])):
            if op == last_union and "union" in op:
                acc[op] = acc[op] + [so]
            else:
                acc = {op: [acc, so]}
            last_union = op

        if not tokens["orderby"] and not tokens["offset"] and not tokens["limit"]:
            return acc
        else:
            output = {"from": acc}

    output["orderby"] = tokens["orderby"]
    output["offset"] = tokens["offset"]
    output["limit"] = tokens["limit"]
    return output


def to_statement(tokens):
    output = tokens["query"][0]
    output["with"] = tokens["with"]
    return output


def unquote(tokens):
    val = tokens[0]
    if val.startswith("'") and val.endswith("'"):
        val = "'" + val[1:-1].replace("''", "\\'") + "'"
    elif val.startswith('"') and val.endswith('"'):
        val = '"' + val[1:-1].replace('""', '\\"') + '"'
    elif val.startswith("`") and val.endswith("`"):
        val = '"' + val[1:-1].replace("``", "`").replace('"', '\\"') + '"'
    elif val.startswith("[") and val.endswith("]"):
        val = '"' + val[1:-1].replace("]]", "]").replace('"', '\\"') + '"'
    elif val.startswith("+"):
        val = val[1:]
    un = ast.literal_eval(val).replace(".", "\\.")
    return un


def to_string(tokens):
    val = tokens[0]
    val = "'" + val[1:-1].replace("''", "\\'") + "'"
    return {"literal": ast.literal_eval(val)}


# NUMBERS
real_num = (
    Regex(r"[+-]?(\d+\.\d*|\.\d+)([eE][+-]?\d+)?").set_parser_name("float")
    / (lambda t: float(t[0]))
)


def parse_int(tokens):
    if "e" in tokens[0].lower():
        return int(float(tokens[0]))
    else:
        return int(tokens[0])


int_num = Regex(r"[+-]?\d+([eE]\+?\d+)?").set_parser_name("int") / parse_int
hex_num = Regex(r"0x[0-9a-fA-F]+").set_parser_name("hex") / (lambda t: {"hex": t[0][2:]})

# STRINGS
ansi_string = Regex(r"\'(\'\'|[^'])*\'") / to_string
mysql_doublequote_string = Regex(r'\"(\"\"|[^"])*\"') / to_string

# BASIC IDENTIFIERS
ansi_ident = Regex(r'\"(\"\"|[^"])*\"') / unquote
mysql_backtick_ident = Regex(r"\`(\`\`|[^`])*\`") / unquote
sqlserver_ident = Regex(r"\[(\]\]|[^\]])*\]") / unquote
