import logging
import os
import queue
from pprint import pprint, pformat

import psycopg2
from dotenv import load_dotenv
from mo_sql_parsing import parse, format

from preprocessing import preprocess_query_string, preprocess_query_tree

# did not change (might need to change .env file if making changes)
def import_config():
    load_dotenv()
    db_uname = os.getenv("DB_UNAME")
    db_pass = os.getenv("DB_PASS")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    return  db_uname, db_pass, db_host, db_port

# did not change
def open_db(db_name, db_uname, db_pass, db_host, db_port):
    conn = psycopg2.connect(database=db_name, user=db_uname, password=db_pass, host=db_host, port=db_port)
    conn.set_session(readonly=True, autocommit=True)
    return conn

# did not change
def init_conn(db_name):
    db_uname, db_pass, db_host, db_port = import_config()
    conn = open_db(db_name, db_uname, db_pass, db_host, db_port)
    return conn

def get_query_execution_plan(cursor, sql_query):
    # to execute the given sql operation
    cursor.execute(f"EXPLAIN (VERBOSE TRUE, FORMAT JSON) {sql_query}")
    result = cursor.fetchone()
    return result

# did not change much, added cost and index name
def transverse_plan(plan):
    logging.debug(f"current in {plan['Node Type']}")
    
    if plan['Node Type'] == 'Nested Loop':
        # to check if the length of plans is two
        assert len(plan['Plans']) == 2, "Length of plans is more than two."
        
        if 'Join Filter' in plan:
            yield {
                'Type': 'Join',
                'Subtype': 'Nested loop',
                'Filter': plan['Join Filter'],
                'Cost': plan['Total Cost'],
            }
        else: 
            yield {
                'Type': 'Join',
                'Subtype': 'Nested loop',
                'Filter': '',
                'LHS': plan['Plans'][0]['Output'],
                'RHS': plan['Plans'][1]['Output'],
                'Cost': plan['Total Cost'],
            }
        
        yield from transverse_plan(plan['Plans'][0])
        yield from transverse_plan(plan['Plans'][1])
    
    elif plan['Node Type'] == 'Hash Join':
        assert len(plan['Plans']) == 2, "Length of plans is more than two."

        yield {
            'Type': 'Join',
            'Subtype': 'Hash join',
            'Filter': plan['Hash Cond'],
            'Cost': plan['Total Cost'],
        }

        yield from transverse_plan(plan['Plans'][0])
        yield from transverse_plan(plan['Plans'][1])

    elif plan['Node Type'] == 'Merge Join':
        yield {
            'Type': 'Join',
            'Subtype': 'Merge join',
            'Filter': plan['Merge Cond'],
            'Cost': plan['Total Cost'],
        }

        for p in plan['Plans']:
            yield from transverse_plan(p)
    
    elif plan['Node Type'] == 'Seq Scan':
        yield {
            'Type': 'Scan',
            'Subtype': 'Sequence scan',
            'Name': plan['Relation Name'],
            'Alias': plan['Alias'],
            'Filter': plan.get('Filter', ''),
            'Cost': plan['Total Cost'],
        }
    
    elif plan['Node Type'] in ['Index Scan', 'Index Only Scan']:
        def filter():
            if 'Index Cond' in plan:
                yield plan['Index Cond']
            if 'Filter' in plan:
                yield plan['Filter']

        yield {
            'Type': 'Scan',
            'Subtype': 'Index scan',
            'Name': plan['Relation Name'],
            'Alias': plan['Alias'],
            'Filter': ' AND '.join(filter()),
            'Index' : plan['Index Name'],
            'Cost': plan['Total Cost'],
        }
    
    elif plan['Node Type'] == 'Bitmap Index Scan':
        yield {
            'Type': 'Scan',
            'Subtype': 'Bitmap index scan',
            'Name': plan['Index Name'],
            'Alias': '',
            'Filter': plan.get('Index Cond', ''),
            'Index' : plan['Index Name'],
            'Cost': plan['Total Cost'],
        }
    
    elif plan['Node Type'] == 'Bitmap Heap Scan':
        yield {
            'Type': 'Scan',
            'Subtype': 'Bitmap heap scan',
            'Name': plan['Relation Name'],
            'Alias': plan['Alias'],
            'Filter': plan.get('Filter', ''),
           # 'Index' : plan['Index Name'], 
           # heapscan doesnt have the key Index 
            'Cost': plan['Total Cost'],
        }
        for p in plan['Plans']:
            yield from transverse_plan(p)
    
    else:
        logging.warning(f"WARNING: Unimplemented Node Type {plan['Node Type']}")
        for p in plan['Plans']:
            yield from transverse_plan(p)

# added new function to explain why an algorithm is chosen (remove this later)
def explain(result):
    algo = result['Subtype']
    if algo == 'Nested loop':
        statement = "Nested loop is used because it is less costly when joining smaller tables / The join condition does not use the equality operator."
    elif algo == 'Hash join':
        statement = "Hash join is used because the join condition uses equality operator and both sides of the join are large."
    elif algo == 'Merge join':
        statement = "Merge join is used because both sides of the join are large and can be sorted on the join condition efficiently."
    elif algo == 'Sequence scan':
        statement = "Sequential scan is used since there is no index on the table"
    elif algo == 'Index scan' or algo == 'Index only scan':
        statement = f"This is used because index ({result['Index']}) exists."
    elif algo == 'Bitmap index scan':
        statement = f"This is used because both tables have indexes ({result['Index']})."
    elif algo == 'Bitmap heap scan':
        statement = f"This is used because both tables have indexes."
    else:
        statement = ''
    return statement

def format_ann(result: dict):
    # marker for annotation
    if result['Type'] == 'Join':
        return f"Perform {result['Subtype']} on {result['Filter']}, total cost is {result['Cost']}. {explain(result)}"
    
    #marker for annotation
    elif result['Type'] == 'Scan':
        return f"Filtered by {result['Subtype']} of {result['Name']}, total cost is {result['Cost']}. {explain(result)}"

#todo: add more comments and explanations
def parse_expr_node(query: dict, result: dict) -> bool:

    # input query and result for parsing
    outcome = True
    #check if query dictionary keys contain 'ann'
    if 'ann' in query.keys():
        outcome = False
        return outcome
    #dictionary of comparison operations in query dictionary keys
    comp_ops = {
        'gt': (' > ', ' < '),
        'lt': (' < ', ' > '),
        'eq': (' = ', ' = '),
        'neq': (' <> ', ' <> '),
        'gte': (' >= ', ' <= '),
        'lte': (' <= ', ' >= '),
        'like': (' ~~ ', ' ~~ '),
        'not_like': (' !~~ ', ' !~~ '),
    }
    op = list(query.keys())[0]
    # if theres 'and' or 'or', there are likely subqueries to parse
    if op == 'and' or op == 'or':
        outcome = False
        for subquery in query[op]:
            if type(subquery) is dict:
                # check for subquery and recursively parse expressions in the subquery
                outcome |= parse_expr_node(subquery, result)
            else:
                raise NotImplementedError(f'{subquery}')
        if outcome:
            query['expand'] = True
        return outcome
    #if no subqueries, check for comparison operations 
    elif op in comp_ops:
        new_arr = []
        annotated = False
        for subquery in query[op]:
            if type(subquery) is str:
                new_arr.append(subquery)
            elif type(subquery) in [int, float]:
                new_arr.append(str(subquery))
            elif type(subquery) is dict:
                if 'literal' in subquery:
                    new_arr.append(f"'{subquery['literal']}'")
                elif 'date' in subquery:
                    new_arr.append(f"'{subquery['date']['literal']}'")
                elif len(subquery.keys() & {'sub', 'add'}) > 0:
                    new_arr.append('$')
                else:
                    new_arr.append('$')
                    if find_query_node(subquery, result):
                        query['expand'] = True
                        annotated = True
            else:
                raise NotImplementedError(f'{subquery}')
        
        exp = (comp_ops[op][0].join(new_arr), comp_ops[op][1].join(reversed(new_arr)))
        if any(x in result['Filter'] for x in exp):
            query['ann'] = format_ann(result)
            return True
        else:
            return annotated

    elif op == 'between':
        outcome = False
        return outcome
    elif op == 'exists':
        if find_query_node(query[op], result):
            query['expand'] = True
            outcome = True
            return outcome
        outcome = False
        return outcome
    elif op == 'not':
        if parse_expr_node(query[op], result):
            query['expand'] = True
            outcome = True
            return outcome
        outcome = False
        return outcome
    elif op in ['in', 'nin']:
        if type(query[op][1]) is dict:
            if 'literal' in query[op][1]:
                pass
            else:
                if find_query_node(query[op][1], result):
                    query['expand'] = True
                    outcome = True
                    return outcome
        elif type(query[op][1]) is list:
            assert type(query[op][1][0]) in [str, int, float]
        outcome = False
        return outcome
    else:
        raise NotImplementedError(f'{op}')

# did not change much, added cost and explanation
def find_query_node(query: dict, result: dict) -> bool:
    # a joining operation
    if result['Type'] == 'Join': 
        # condition stated in where
        if 'where' in query:
            # did not specify condition
            if result['Filter'] == '':
                possible_conditions = []
                for condition in [f'{x} = {y}' for x in result['LHS'] for y in result['RHS']]:
                    result['Filter'] = condition
                    if parse_expr_node(query['where'], result):
                        possible_conditions.append(condition)
                assert len(possible_conditions) <= 1, "There are more than one possible conditions"
                if len(possible_conditions) == 1:
                    return True
            else:
                if parse_expr_node(query['where'], result):
                    return True
        
        if type(query['from']) is dict and type(query['from']['value']) is dict:
            if find_query_node(query['from']['value'], result):
                return True
        
        if type(query['from']) is list:
            for v in query['from']:
                if type(v) is dict and type(v['value']) is dict:
                    if find_query_node(v['value'], result):
                        return True
    
    elif result['Type'] == 'Scan':  
        annotated = False
        
        if type(query['from']) is str:
            if query['from'] == result['Name'] and query['from'] == result['Alias']:
                query['from'] = {
                    'value': query['from'],
                    # marker for annotation
                    'ann': f"{result['Subtype']} of {result['Name']}, total cost is {result['Cost']}. {explain(result)}"
                }
                annotated = True
        
        elif type(query['from']) is dict:
            if type(query['from']['value']) is dict:
                if find_query_node(query['from']['value'], result):
                    query['from']['expand'] = True
                    annotated = True
            elif type(query['from']['value']) is str and query['from']['value'] == result['Name'] and query['from'].get(
                    'name', '') == result['Alias']:
                annotated = True
                # marker for annotation
                query['from']['ann'] = f"{result['Subtype']} of {result['Name']} as {result['Alias']}, total cost is {result['Cost']}. {explain(result)}"
        
        elif type(query['from']) is list:
            for i, rel in enumerate(query['from']):
                if type(rel) is str:
                    if rel == result['Name'] and rel == result['Alias']:
                        query['from'][i] = {
                            'value': rel,
                            # marker for annotation
                            'ann': f"{result['Subtype']} on {result['Name']}, total cost is {result['Cost']}. {explain(result)}"
                        }
                        annotated = True
                        break
                else:
                    if type(rel['value']) is dict:
                        if find_query_node(rel['value'], result):
                            rel['expand'] = True
                            annotated = True
                        continue
                    assert type(rel['value']) is str
                    if rel['value'] == result['Name'] and rel.get('name', '') == result['Alias']:
                        # marker for annotation
                        rel['ann'] = f"{result['Subtype']} {result['Name']} as {result['Alias']} , total cost is {result['Cost']}. {explain(result)}"
                        annotated = True
                        break
        
        # filter exixts
        if result['Filter'] != '' and 'where' in query:
            parse_expr_node(query['where'], result)
        return annotated

    return False

def traverse_query(query: dict, plan: dict):
    #loop over each node in the input query
    for outcome in transverse_plan(plan):  
        find_query_node(query, outcome)

def process(conn, query):
    #process input by getting query execution plan and parsing query
    #outputs annotated query plan
    query = preprocess_query_string(query)
    current = conn.cursor()
    plan = get_query_execution_plan(current, query)
    parsed_query = parse(query)
    
    preprocess_query_tree(current, parsed_query)
    traverse_query(parsed_query, plan[0][0]['Plan'])
    
    result = []
    reparse_query(result, parsed_query)
    
    return [q['statement'] for q in result], [q['annotation'] for q in result]

def reparse_without_expand(statement_dict):
    #retrieve annotations and format the query 
    #outputs formatted query in an array
    result = []
    annotn = get_annotation(statement_dict)
    if 'ann' in statement_dict.keys():
        del statement_dict['ann']
    formatted_statement = format(statement_dict)
    result.append(format_query(formatted_statement, annotn))
    return result

def format_keyword_special(statement_dict):
    changed = format(statement_dict)
    print('changed: ', changed)
    return changed.split('""', 1)[0].strip()

def get_annotation(statement_dict):
    if 'ann' not in statement_dict.keys():
        return ''
    else:
        return statement_dict['ann']

def get_name(statement_dict):
    if 'name' not in statement_dict.keys():
        return None
    else:
        return statement_dict['name']

# keyword in function name can change to aggregate later
def find_keyword_operation(statement_dict: dict):
    aggregate = {'sum', 'count', 'min', 'max', 'avg', 'coalesce'}
    for operation in aggregate:
        if operation in statement_dict.keys():
            return operation
    return None

def find_arithmetic_operation(statement_dict: dict):
    arithmetic = {'mul', 'sub', 'add', 'div', 'mod'}
    for operation in arithmetic:
        if operation in statement_dict.keys():
            return operation
    return None

def find_conjunction_operation(statement_dict: dict):
    conjunction = {'and', 'or'}
    for operation in conjunction:
        if operation in statement_dict.keys():
            return operation
    return None

def find_comparison_operation(statement_dict: dict):
    comparison = {
        'gt', 'lt',
        'gte', 'lte',
        'in','nin'
        'like', 'not_like',
        'eq', 'neq',
    }


    for operation in comparison:
        if operation in statement_dict.keys():
            return operation
    return None

def find_datetime_operation(statement_dict: dict):
    datetime = {'time', 'timestamp', 'date', 'interval'}
    for operation in datetime:
        if operation in statement_dict.keys():
            return operation
    return None

def reparse_literal(value: any):
    if isinstance(value, str):
        return "'" + value + "'"
    elif isinstance(value, list):
        result = '('
        for i, v in enumerate(value):
            result += "'" + v + "'"
            if i < len(value) - 1:
                result += ', '
            else:
                result += ')'
        return result
    else:
        raise NotImplementedError(f"literal type - {value}")

#to do: make more edits
def reparse_arithmetic_operation(statement_dict: dict, symbol_op: str):
    temp_list = []

    if 'expand' not in statement_dict.keys():
        temp_list.extend(reparse_without_expand(statement_dict))
        return temp_list

    symbol_ops = {'mul': '*', 'sub': '-', 'add': '+', 'div': '/', 'mod': '%'}
    operands = statement_dict[symbol_op]

    if isinstance(operands, list):
        statement = ''
        for i, operand in enumerate(operands):
            if isinstance(operand, (int, float, str)):
                statement += str(operand)
            elif type(operand) == dict:
                arithmetic_operator = find_arithmetic_operation(operand)
                datetime_op = find_datetime_operation(operand)
                if arithmetic_operator is not None:
                    subq = reparse_arithmetic_operation(operand, arithmetic_operator)
                    if len(subq) > 1:
                        statement += '('
                        temp_list.append(format_query(statement))
                        temp_list.extend(subq)
                        temp_list.append(format_query(')'))
                    else:
                        statement += '(' + subq[0]['statement'] + ')'
                        temp_list.append(format_query(statement, subq[0]['annotation']))

                    statement = ''
                elif datetime_op is not None:
                    subq = reparse_datetime_operation(operand, datetime_op)
                    statement += '('
                    if len(subq) == 1:
                        statement += subq[0]['statement']
                        statement += ')'
                        temp_list.append(format_query(statement))
                    else:
                        temp_list.append(format_query(statement))
                        temp_list.extend(subq)
                        temp_list.append(format_query(')'))
                    statement = ''
                else:
                    subq = reparse_other_operations(operand)
                    statement += '(' + subq + ')'
                    temp_list.append(format_query(statement))
                    statement = ''

            if i < len(operands) - 1:
                if statement != '':
                    statement += ' '
                statement += symbol_ops[symbol_op] + ' '

        if statement != '':
            temp_list.append(format_query(statement))

    return temp_list


def reparse_keyword_operation(statement_dict: dict, op: str, comma: bool = False):
    temp_list = []

    if 'expand' not in statement_dict.keys():
        temp_list.extend(reparse_without_expand(statement_dict))
        return temp_list

    operand = statement_dict[op]

    if isinstance(operand, str):
        statement = op.upper() + ' (' + operand + ')'
        if comma:
            statement += ','
        temp_list.append(format_query(statement))
    elif type(operand) == dict:
        arithmetic_operator = find_arithmetic_operation(operand)
        temp_list.append(format_query(op.upper() + ' ('))

        if arithmetic_operator is not None:
            subq = reparse_arithmetic_operation(operand, arithmetic_operator)
            temp_list.extend(subq)
        else:
            subq = reparse_other_operations(operand)
            temp_list.append(format_query(subq))

        end_statement = ')'
        if comma:
            end_statement += ','
        temp_list.append(format_query(end_statement))

    return temp_list


def reparse_conjunction_operation(statement_dict: dict, conj_op: str):
    temp_list = []

    if 'expand' not in statement_dict.keys():
        temp_list.extend(reparse_without_expand(statement_dict))
        return temp_list

    operands = statement_dict[conj_op]
    assert type(operands) is list

    for i, operand in enumerate(operands):
        arithmetic_op = find_arithmetic_operation(operand)
        conjunction_op = find_conjunction_operation(operand)
        comparison_op = find_comparison_operation(operand)

        if arithmetic_op is not None:
            temp_list.append(format_query('('))
            subq = reparse_arithmetic_operation(operand, arithmetic_op)
            temp_list.extend(subq)
            temp_list.append(format_query(')'))
        elif conjunction_op is not None:
            temp_list.append(format_query('('))
            subq = reparse_conjunction_operation(operand, conjunction_op)
            temp_list.extend(subq)
            temp_list.append(format_query(')'))
        elif comparison_op is not None:
            subq = reparse_comparison_operation(operand, comparison_op)
            if len(subq) > 1:
                temp_list.append(format_query('('))
                temp_list.extend(subq)
                temp_list.append(format_query(')'))
            else:
                temp_list.extend(subq)
        elif 'exists' in operand.keys():
            temp_list.append(format_query('('))
            subq = reparse_exists_keyword(operand)
            temp_list.extend(subq)
            temp_list.append(format_query(')'))
        elif 'not' in operand.keys():
            temp_list.append(format_query('('))
            subq = reparse_not_operation(operand)
            temp_list.extend(subq)
            temp_list.append(format_query(')'))
        else:
            temp_list.append(format_query('('))
            subq = reparse_other_operations(operand)
            temp_list.append(format_query(subq))
            temp_list.append(format_query(')'))

        if i < len(operands) - 1:
            temp_list.append(format_query(conj_op.upper()))

    return temp_list


def reparse_not_operation(statement_dict: dict):
    temp_list = []

    if 'expand' not in statement_dict.keys():
        temp_list.extend(reparse_without_expand(statement_dict))
        return temp_list

    operand = statement_dict['not']

    arithmetic_op = find_arithmetic_operation(operand)
    conjunction_op = find_conjunction_operation(operand)
    comparison_op = find_comparison_operation(operand)

    temp_list.append(format_query('NOT ('))
    if arithmetic_op is not None:
        subq = reparse_arithmetic_operation(operand, arithmetic_op)
        temp_list.extend(subq)
    elif conjunction_op is not None:
        subq = reparse_conjunction_operation(operand, conjunction_op)
        temp_list.extend(subq)
    elif comparison_op is not None:
        subq = reparse_comparison_operation(operand, comparison_op)
        temp_list.extend(subq)
    elif 'exists' in operand.keys():
        subq = reparse_exists_keyword(operand)
        temp_list.extend(subq)
    else:
        subq = reparse_other_operations(operand)
        temp_list.append(format_query(subq))

    temp_list.append(format_query(')'))
    return temp_list


def reparse_comparison_operation(statement_dict: dict, comp_op: str):
    temp_list = []

    if 'expand' not in statement_dict.keys():
        temp_list.extend(reparse_without_expand(statement_dict))
        return temp_list

    comp_ops = {
        'gt': '>',
        'lt': '<',
        'eq': '=',
        'neq': '<>',
        'gte': '>=',
        'lte': '<=',
        'like': 'LIKE',
        'not_like': 'NOT LIKE',
        'in': 'IN',
        'nin': 'NOT IN'
    }

    annotation = get_annotation(statement_dict)
    operands = statement_dict[comp_op]

    if isinstance(operands, list):
        statement = ''
        for i, operand in enumerate(operands):
            if isinstance(operand, (int, float, str)):
                statement += str(operand)
            elif type(operand) is dict:
                arithmetic_op = find_arithmetic_operation(operand)
                datetime_op = find_datetime_operation(operand)

                if 'literal' in operand.keys():
                    statement += reparse_literal(operand['literal'])

                else:
                    statement += '('
                    if i > 0:
                        temp_list.append(format_query(statement, annotation))
                    else:
                        temp_list.append(format_query(statement))
                    statement = ''

                    if 'select' in operand.keys():
                        reparse_query(temp_list, operand)
                    elif arithmetic_op is not None:
                        subq = reparse_arithmetic_operation(operand, arithmetic_op)
                        temp_list.extend(subq)
                    elif datetime_op is not None:
                        subq = reparse_datetime_operation(operand, datetime_op)
                        temp_list.extend(subq)
                    else:
                        subq = reparse_other_operations(operand)
                        temp_list.append(format_query(subq))

                    temp_list.append(format_query(')'))
            elif isinstance(operand, list):
                if all(isinstance(x, int) for x in operand):
                    statement += '(' + ', '.join([str(op) for op in operand]) + ')'

            if i < len(operands) - 1:
                if statement != '':
                    statement += ' '
                statement += comp_ops[comp_op] + ' '

        if statement != '':
            temp_list.append(format_query(statement, annotation))

    return temp_list


def reparse_datetime_operation(statement_dict: dict, datetime_op: str):
    temp_list = []

    if 'expand' not in statement_dict.keys():
        temp_list.extend(reparse_without_expand(statement_dict))
        return temp_list

    operand = statement_dict[datetime_op]

    statement = datetime_op.upper() + " "
    if isinstance(operand, list):
        statement += "'" + ' '.join([str(op) for op in operand]) + "'"
    elif type(operand) == dict:
        if 'literal' in operand.keys():
            statement += reparse_literal(operand['literal'])

    temp_list.append(format_query(statement))
    return temp_list

#not edited 
def reparse_other_operations(statement_dict: dict):
    if 'expand' in statement_dict.keys():
        raise NotImplementedError(f"operation - {statement_dict}")
    else:
        return format(statement_dict)


def reparse_exists_keyword(statement_dict: dict):
    temp_list = []

    if 'expand' not in statement_dict.keys():
        temp_list.extend(reparse_without_expand(statement_dict))
        return temp_list

    operand = statement_dict['exists']

    if type(operand) == dict:
        temp_list.append(format_query('EXISTS ('))
        reparse_query(temp_list, operand)
        temp_list.append(format_query(')'))

    return temp_list




def reparse_between_keyword(statement_dict: dict):
    temp_list = []

    if 'expand' not in statement_dict.keys():
        temp_list.extend(reparse_without_expand(statement_dict))
        return temp_list

    operand = statement_dict['between']
    temp_list.append(format_query(operand[0] + ' BETWEEN '))

    for i, op in range(1, len(operand)):
        if isinstance(op, (int, float, str)):
            temp_list.append(format_query(op))
        elif type(op) is dict:
            arithmetic_op = find_arithmetic_operation(op)

            if arithmetic_op is not None:
                temp_list.append(format_query('('))
                subquery = reparse_arithmetic_operation(operand, arithmetic_op)
                temp_list.extend(subquery)
                temp_list.append(format_query(')'))
            elif 'literal' in op:
                temp_list.append(format_query(reparse_literal(op['literal'])))

        if i == 1:
            temp_list.append(format_query('AND'))

    return temp_list


def format_query(statement: str, annotation: str = ''):
    return {'statement': statement, 'annotation': annotation}


def reparse_from_keyword(formatted_query: list, identifier: any, last_identifier: bool = True):
    temp_list = []

    if type(identifier) == dict:
        name = get_name(identifier)
        annotation = get_annotation(identifier)
        end_statement = ''

        if isinstance(identifier['value'], str):
            end_statement = identifier['value']
        elif type(identifier['value']) == dict:
            temp_list.append(format_query('('))
            reparse_query(temp_list, identifier['value'])
            end_statement = ')'

        if name is not None:
            end_statement += ' AS ' + name

        if not last_identifier:
            end_statement += ','
        temp_list.append(format_query(end_statement, annotation))
    elif type(identifier) is str:
        statement = identifier

        if not last_identifier:
            statement += ','

        temp_list.append(format_query(statement))
    elif type(identifier) is list:
        for i, single_identifier in enumerate(identifier):
            reparse_from_keyword(temp_list, single_identifier, i == len(identifier) - 1)

    formatted_query.extend(temp_list)


def reparse_where_keyword(formatted_query: list, identifier: any):
    temp_list = []
    assert type(identifier) is dict

    conjunction_op = find_conjunction_operation(identifier)
    comparison_op = find_comparison_operation(identifier)

    if conjunction_op is not None:
        subquery = reparse_conjunction_operation(identifier, conjunction_op)
        temp_list.extend(subquery)
    elif comparison_op is not None:
        subquery = reparse_comparison_operation(identifier, comparison_op)
        temp_list.extend(subquery)
    elif 'exists' in identifier.keys():
        subquery = reparse_exists_keyword(identifier)
        temp_list.extend(subquery)
    elif 'not' in identifier.keys():
        subquery = reparse_not_operation(identifier)
        temp_list.extend(subquery)
    else:
        subquery = reparse_other_operations(identifier)
        temp_list.append(format_query(subquery))

    formatted_query.extend(temp_list)


def reparse_keyword_without_annotation(formatted_query: list, identifier: any):
    formatted_query.append(format_query(format_keyword_special(identifier)))


def reparse_query(formatted_query: list, statement_dict: dict):
    temp_list = []

    for keyword, identifier in statement_dict.items():
        if keyword.startswith('select'):
            appended_identifier = {keyword: identifier}
            if 'distinct_on' in statement_dict.keys():
                appended_identifier['distinct_on'] = statement_dict['distinct_on']
            temp_list.append(format_query(format(appended_identifier)))
        elif keyword == 'from':
            temp_list.append(format_query('FROM'))
            reparse_from_keyword(temp_list, identifier)
        elif keyword == 'where':
            temp_list.append(format_query('WHERE'))
            reparse_where_keyword(temp_list, identifier)
        elif keyword == 'having':
            temp_list.append(format_query('HAVING'))
            reparse_where_keyword(temp_list, identifier)
        elif keyword == 'groupby':
            appended_identifier = {'groupby': identifier, 'from': ''}
            reparse_keyword_without_annotation(temp_list, appended_identifier)
        elif keyword == 'orderby':
            appended_identifier = {'orderby': identifier, 'from': ''}
            reparse_keyword_without_annotation(temp_list, appended_identifier)
        elif keyword == 'limit':
            appended_identifier = {'limit': identifier, 'from': ''}
            reparse_keyword_without_annotation(temp_list, appended_identifier)

    formatted_query.extend(temp_list)


def annotate_query(parsed_query: dict):
    formatted_query = []
    reparse_query(formatted_query, parsed_query)
    return formatted_query

def dfs_get_node_types(plan):
    cur_plan = plan
    cur_node = cur_plan['Node Type']
    output = []
    stack = []
    stack.append(cur_plan)
    while(len(stack)):
        cur_plan = stack.pop()
        output.append(cur_plan['Node Type'])
        if 'Plans' in cur_plan:
            for item in cur_plan['Plans']:
                #print(item)
                stack.append(item)
    return output


def get_used_node_types(plan):
    '''
    check what node types have been used in the QEP generated
    Params: 
        QEP generated
    Output:
        node types used in QEP (list)
    '''
    child_plans = queue.Queue()
    parent_plans = queue.Queue()
    child_plans.put(plan)
    parent_plans.put(None)
    nodes_used = []
    
    while not child_plans.empty():
        curr_plan = child_plans.get()
        parentNode = parent_plans.get()

        curr_node = curr_plan['Node Type']
        nodes_used.append(curr_node)

        if 'Plans' in curr_plan:
            for item in curr_plan['Plans']:
                # add children plans to the child queue
                child_plans.put(item)
                parent_plans.put(curr_node)

    return nodes_used

# added method to generate aqp 
def generate_alternative_qep(cursor, sql_query, nodes_used):
    '''
    disable the node types that were used in QEP and generate an alternative qep when given the same query
    Params: 
        Cursor
        Input Query
        Nodes used in QEP (list)
    Output:
        AQP
    '''
    # remove the node types that were used in the previous query plan
    cond = None
    cond2 = None
    #check for scans first

    if 'Index Scan' in nodes_used:
        cond = "enable_indexscan"
    elif 'Index Only Scan' in nodes_used:
        cond = 'enable_indexonlyscan'
    elif 'Seq Scan' in nodes_used:
        cond = "enable_seqscan"
    elif 'Bitmap Index Scan' in nodes_used:
        cond = "enable_bitmapscan"
    elif 'Bitmap Heap Scan' in nodes_used:
        cond = "enable_bitmapscan"
  #  print('cond is: ', cond)
    
    # check for joins

    if 'Hash Join' in nodes_used:
        cond2 = "enable_hashjoin"
    elif 'Merge Join' in nodes_used:
        cond2 = "enable_mergejoin"
    elif 'Nested Loop' in nodes_used:
        cond2 = "enable_nestloop"
    
   # print('cond 2 is :', cond2)

    # disable conditions accordingly when generating aqp
    print('the following conditions are turned off:', cond, ' and ', cond2)
    if cond is not None and cond2 is not None:
        cursor.execute(f"set {cond} = 'off'; set {cond2} = 'off'; EXPLAIN (VERBOSE TRUE, FORMAT JSON) {sql_query}")
        result = cursor.fetchone()
        return result
    elif cond is not None and cond2 is None:
        cursor.execute(f"set {cond} = 'off'; EXPLAIN (VERBOSE TRUE, FORMAT JSON) {sql_query}")
        result = cursor.fetchone()
        return result
    elif cond is None and cond2 is not None:
        cursor.execute(f"set {cond2} = 'off'; EXPLAIN (VERBOSE TRUE, FORMAT JSON) {sql_query}")
        result = cursor.fetchone()
    else:
        cursor.execute(f"EXPLAIN (VERBOSE TRUE, FORMAT JSON) {sql_query}")
        result = cursor.fetchone()
        return result

def check_if_same(qep, aqp):
    '''
    compare QEP and AQP and retrieve the differences and reasons behind the differences
    Params:
        QEP (Plan)
        AQP (Plan)
    Output:
        1 if same
        0 if different
    '''
    #check if the qep and aqp are the same first
    nodes_used_by_qep = get_used_node_types(qep)
    nodes_used_by_aqp = get_used_node_types(aqp)
    if nodes_used_by_qep == nodes_used_by_aqp:
        return 1
    else:
        return 0
    
def compare_results(qep, aqp):
    '''
    compare annotated results of QEP AND AQP
    and include the differences between QEP AND AQP
    Params:
        QEP annotations
        AQP annotations
    Output:
        annotation for QEP, updated with comparisons (assuming that this will be the only explanations outputted on GUI)
    '''
    for i in range(len(qep)):
        if qep[i]['annotation'] == '':
            pass
        elif qep[i]['annotation'] == aqp[i]['annotation']:
            pass
        else:
            # use this to analyse the annotations and generate the differences
            #return updated annotation to be added to QEP result
            result = compare_annotations(qep[i]['annotation'], aqp[i]['annotation'])
            qep[i]['annotation'] = qep[i]['annotation'] + '\n' + result

    return qep

def compare_annotations(qep_ann, aqp_ann):
    '''
    this method is to extract the node type from the annotations
    Params:
        annotation line from QEP
        annotation line from AQP
    Output:
        node types from QEP and AQP, to be compared using generate_differences()
    '''
    annotations = [qep_ann, aqp_ann]
    node_types = []
    for i in annotations:
        if i.startswith('Filtered by'):
            idx1 = i.index('Filtered by')
            idx2 = i.index('of')
            res = ''
            for idx in range(idx1 + len('Filtered by') + 1, idx2-1):
                res = res+i[idx]
            node_types.append(res)
        elif i.startswith('Perform'):
            idx1 = i.index('Perform')
            idx2 = i.index('on')
            res = ''
            for idx in range(idx1 + len('Perform')+1, idx2-1):
                res = res+i[idx]
            node_types.append(res)
        else: 
            if 'of' in i:
                idx1 = i.index('of')
            elif 'on' in i:
                idx1 = i.index('on')
            res = ''
            for idx in range(0, idx1-1):
                res = res+i[idx]
            node_types.append(res)
    if node_types[0] == node_types[1]:
        output= 'the two node types are the same, but the cost is different due to the other node types.'
        return output
    # use this method to find the difference between two node types
    print('node types:', node_types)
    result = generate_differences(node_types[0], node_types[1])
    return result

def generate_differences(node1, node2):
    '''
    Compare two nodes from the qep and aqp and generate the reason for their differences
    node1 is from qep
    node2 is from aqp
    Output:
        reasons for differences between the node types (string)
    '''
    if node1 == 'Index scan' and node2 == 'Bitmap heap scan':
        diff = "Index scan is chosen over bitmap heap scan as index condition has high selectivity, which makes index scan more efficient and less costly."
    elif node1 == 'Index scan' and node2 == 'Bitmap index scan':
        diff = "Index scan is chosen over bitmap index scan as index condition has high selectivity, which makes index scan more efficient and less costly."
    elif node1 == "Index scan" and node2 == "Seq Scan":
        diff = "Index scan is chosen over sequential scan as it is able to access the tuples with the desired values directly, unlike sequential scans, which needs to check each tuples."
    elif node1 == "NL Join" and node2 == "Merge Join":
        diff = "Nested-loop join is chosen over merge join because one of the inputs is small and so less comparisons are needed to be done using nested loop join, resulting in lower cost."
    elif node1 == "NL Join" and node2 == "Hash Join":
        diff = "Nested-loop join is chosen over hash join because one of the inputs is small and so less comparisons are needed to be done using nested loop join, resulting in lower cost."
    elif node1 == "Hash Join" and node2 == "Merge Join":
        diff = "Hash join is chosen over merge join because the input relations are large and unsorted, hence using a hash join will be faster and less costly."
    elif node1 == "Hash Join" and node2 == "NL Join":
        diff = "Hash join is chosen over nested-loop join because the input relations are large and unsorted, hence using a hash join will be faster and less costly."
    elif node1 == "Merge Join" and node2 == "Hash Join":
        diff = "Merge join is chosen over hash join because the input relations are already sorted on the join attributes. Hence, each relation has to be scanned only once, making the join more efficient and less costly."
    elif node1 == "Merge Join" and node2 == "NL Join":
        diff = "Merge join is chosen over nested-loop join because the input relations are large and using nested-loop join will be inefficient and costly. Besides, the relations are already sorted on the join attributes. Hence, each relation has to be scanned only once, making the join more efficient and less costly."
    elif node1 == "Seq Scan" and node2 == "Index Scan":
        diff = "Sequential scan is chosen over index scan because the expected size of the output is large, hence using a sequential scan will be more efficient than using an index scan."
    elif node1 == "Bitmap Index Scan" and node2 == "Index Scan":
        diff = "Bitmap index scan is chosen over index scan as indexes are available and the expected size of the output is large. Bitmap index scan fetches all the tuple-pointers from the index in one go, while index scan fetches one tuple-pointer at a time from the index. Hence, using bitmap index scan is more efficient than using index scan."
    elif node1 == "Bitmap Heap Scan" and node2 == "Index Scan":
        diff = "Bitmap heap scan is chosen over index scan as indexes are available and the expected size of the output is large. Bitmap heap scan fetches all the tuple-pointers from the index in one go, while index scan fetches one tuple-pointer at a time from the index. Hence, using bitmap heap scan is more efficient than using index scan."
    elif node1 == "Bitmap Index Scan" and node2 == "Seq Scan":
        diff = "Bitmap index scan is chosen over sequential scan as indexes are available. Bitmap index scan fetches all the tuple-pointers from the index in one go and visits these desired tuples directly. Hence, it is more efficient than sequential scan which needs to check every tuples."
    elif node1 == "Bitmap Heap Scan" and node2 == "Seq Scan":
        diff = "Bitmap heap scan is chosen over sequential scan as indexes are available. Bitmap heap scan fetches all the tuple-pointers from the index in one go and visits these desired tuples directly. Hence, it is more efficient than sequential scan which needs to check every tuples."
    elif node1 == "Seq Scan" and node2 == "Bitmap Index Scan":
        diff = "Sequential scan is chosen over bitmap index scan because there is no index which can be utilized to perform bitmap index scan."
    elif node1 == "Seq Scan" and node2 == "Bitmap Heap Scan":
        diff = "Sequential scan is chosen over bitmap heap scan because there is no index which can be utilized to perform bitmap heap scan."
    else:
        diff = ''    
    return diff

    # TO DO : add more differences and comparisons (the cases i can think of are listed below)
    # case 1: index scan over seq scan (done)
    # case 2: seq scan over index scan (done)
    # case 3: index scan over bitmap scan (done)
    # case 4: seq scan over bitmap scans 
    # case 5: bitmap scans over seq scan (done)
    # case 6: merge join over NL join (done)
    # case 7: NL join over merge join (done)
    # case 8: merge join over hash join (done)
    # case 9: hash join over merge join (done)
    # case 10: NL join over hash join (done)
    # case 11: hash joing over NL join (done)
    # case 12 : bitmap over index (done)
    


def main():
   # logging.basicConfig(filename='log/debug.log', filemode='w', level=logging.DEBUG)
    conn = init_conn("TPC-H")
   # conn = init_conn('mydatabase')
    cur = conn.cursor()
   
    print('connected')

    queries = [
        # Test cases
         "SELECT * FROM nation, region WHERE nation.n_regionkey = region.r_regionkey and nation.n_regionkey = 0;",
        # "SELECT * FROM nation, region WHERE nation.n_regionkey < region.r_regionkey and nation.n_regionkey = 0;",
         "SELECT * FROM nation;",
        # 'select N_NATIONKey, "n_regionkey" from NATion;',
        # 'select N_NATIONKey from NATion;',
        #"SELECT * FROM nation as n1, nation as n2 WHERE n1.n_regionkey = n2.n_regionkey;",
        # "SELECT * FROM nation as n1, nation as n2 WHERE n1.n_regionkey < n2.n_regionkey;",
        # "SELECT * FROM nation as n1, nation as n2 WHERE n1.n_regionkey <> n2.n_regionkey;",
        # "SELECT * FROM nation as n WHERE 0 < n.n_regionkey  and n.n_regionkey < 3;",
        # "SELECT * FROM nation as n WHERE 0 < n.n_nationkey  and n.n_nationkey < 30;",
        # "SELECT n.n_nationkey FROM nation as n WHERE 0 < n.n_nationkey  and n.n_nationkey < 30;",
        # "SELECT * FROM customer as c, (SELECT * FROM nation as n where n.n_nationkey > 7 and n.n_nationkey < 15) as n, region as r WHERE n.n_regionkey = r.r_regionkey  and c.c_nationkey = n.n_nationkey;",
        # "SELECT * FROM customer as c, nation as n, region as r WHERE n.n_nationkey > 7 and n.n_nationkey < 15 and  n.n_regionkey = r.r_regionkey  and c.c_nationkey = n.n_nationkey;",
        # "SELECT * FROM customer as c, (SELECT * FROM nation as n where n.n_regionkey=0) as n, region as r WHERE n.n_regionkey = r.r_regionkey  and c.c_nationkey = n.n_nationkey;",
        # "SELECT * FROM customer as c, (SELECT * FROM nation as n where n.n_regionkey<5) as n, region as r WHERE n.n_regionkey = r.r_regionkey  and c.c_nationkey = n.n_nationkey;",
        # "SELECT  DISTINCT c.c_custkey FROM customer as c, (SELECT * FROM nation as n where n.n_regionkey=0) as n, region as r WHERE n.n_regionkey = r.r_regionkey  and c.c_nationkey = n.n_nationkey;",

        # # http://www.qdpma.com/tpch/TPCH100_Query_plans.html
#         """SELECT l.L_RETURNFLAG, l.L_LINESTAATUS, SUM(l.L_QUANTITY) AS SUM_QTY,
#  SUM(l.L_EXTENDEDPRICE) AS SUM_BASE_PRICE, SUM(l.L_EXTENDEDPRICE*(1-L_DISCOUNT)) AS SUM_DISC_PRICE,
#  SUM(l.L_EXTENDEDPRICE*(1-l.L_DISCOUNT)*(1+l.L_TAX)) AS SUM_CHARGE, AVG(l.L_QUANTITY) AS AVG_QTY,
#  AVG(l.L_EXTENDEDPRICE) AS AVG_PRICE, AVG(l.L_DISCOUNT) AS AVG_DISC, COUNT(*) AS COUNT_ORDER
# FROM LINEITEM AS l
# WHERE l.L_SHIPDATE <= date '1998-12-01' + interval '-90 day'
# GROUP BY l.L_RETURNFLAG, l.L_LINESTAATUS
# ORDER BY l.L_RETURNFLAG,l.L_LINESTAATUS""",
#         """SELECT S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT
# FROM PART, SUPPLIER, PARTSUPP, NATION, REGION
# WHERE P_PARTKEY = PS_PARTKEY AND S_SUPPKEY = PS_SUPPKEY AND P_SIZE = 15 AND
# P_TYPE LIKE '%%BRASS' AND S_NATIONKEY = N_NATIONKEY AND N_REGIONKEY = R_REGIONKEY AND
# R_NAME = 'EUROPE' AND
# PS_SUPPLYCOST = (SELECT MIN(PS_SUPPLYCOST) FROM PARTSUPP, SUPPLIER, NATION, REGION
#  WHERE P_PARTKEY = PS_PARTKEY AND S_SUPPKEY = PS_SUPPKEY
#  AND S_NATIONKEY = N_NATIONKEY AND N_REGIONKEY = R_REGIONKEY AND R_NAME = 'EUROPE')
# ORDER BY S_ACCTBAL DESC, N_NAME, S_NAME, P_PARTKEY
# LIMIT 100;""",
    ]

    for query in queries:
        print("<==============================================>")
        query = preprocess_query_string(query)  # assume all queries are case insensitive
     #   print('query: \n')
     #   print(query)
     #   print('\n')
     #   logging.debug(query)

        # getting the QEP
        plan = get_query_execution_plan(cur, query)
     #   print('plan:')
     #   print('\n')
     #   print(plan) 
        parsed_query = parse(query)
     #   print('parsed query:')
     #   print('\n')
     #   print(parsed_query)
        try:
            preprocess_query_tree(cur, parsed_query)
            traverse_query(parsed_query, plan[0][0]['Plan'])
            result = []
            reparse_query(result, parsed_query)

        except Exception as e:
            logging.error(e, exc_info=True)
            logging.debug(pformat(query))
            logging.debug(pformat(parsed_query))
            logging.debug(pformat(plan))
            raise e
        else:
        #    print('below is parsed query: \n')
        #    pprint(parsed_query, sort_dicts=False)
            print('\n -----Below is the generated Query Execution Plan----- \n')
            pprint(result)

        # getting AQP
        try:
            nodes_used = get_used_node_types(plan[0][0]['Plan'])
            print('\n -----QEP Operators Used-----  \n', nodes_used)
            parsed_query_aqp = parse(query)
            aqp = generate_alternative_qep(cur, query, nodes_used)
            preprocess_query_tree(cur, parsed_query_aqp)
            traverse_query(parsed_query_aqp, aqp[0][0]['Plan'])
            aqp_result = []
            reparse_query(aqp_result, parsed_query_aqp)
            aqp_nodes_used = get_used_node_types(aqp[0][0]['Plan'])
            
        except Exception as e:
            raise e
        else:
            if check_if_same(plan[0][0]['Plan'], aqp[0][0]['Plan']) == 1: 
                print("\n => There is no AQP available for this particular query ")
            else:
                print('\n -----Below is the generated Alternate Query Plan----- \n')
                print(aqp_result)
                print('\n -----AQP Operators Used:----- \n ', aqp_nodes_used)
                updated_results = compare_results(result, aqp_result)
                print('\n -----Updated annotations with comparisons for generated QEP vs AQP:----- \n')
                pprint(updated_results)
        print()

    cur.close()


if __name__ == '__main__':
    main()

# SELECT *
# FROM nation, 					  -> Seq Scan, Filter n_regionkey
#      region  					  -> Index Scan on n_regionkey = 0
# WHERE nation.n_regionkey = region.r_regionkey     -> Nested Loop
# AND
#       nation.n_regionkey = 0			  -> SeqScan, Filter n_regionkey = 0