import logging
import typing

# used in annotation
def preprocess_query(query):
    return ' '.join([word.lower() if word[0] != '"' and word[0] != "'" else word for word in query.split()])

# used in annotation
def preprocess_query_tree(cur, query_tree):
    rel_list = []
    col_rel_dict = {}
    collect_relation_list(query_tree, rel_list)
    logging.debug(f'rel_list={rel_list}')
    
    # obtain information about the column
    for relation in rel_list:
        cur.execute(f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{relation}'")
        col_list = cur.fetchall()
        for column in col_list:
            if column in col_rel_dict:
                col_rel_dict[column[0]].append(relation)
            else:
                col_rel_dict[column[0]] = [relation]
    logging.debug(f'col_rel_dict={col_rel_dict}')

    # rename when necessary
    rename_column(query_tree, col_rel_dict)

def collect_relation_list(query_tree, rel_list):
    # check if is string
    if type(query_tree['from']) is str:
        rel_list.append(query_tree['from'])
    # check in dict again
    elif type(query_tree['from']) is dict:
        if type(query_tree['from']['value']) is str:
            rel_list.append(query_tree['from']['value'])
        elif type(query_tree['from']['value']) is dict:
            collect_relation_list(query_tree['from']['value'], rel_list)
        else:
            raise NotImplementedError(f"{query_tree['from']['value']}")

    # check if is list
    elif type(query_tree['from']) is list:
        for relation in query_tree['from']:
            if type(relation) is str:
                rel_list.append(relation)
            # check in dict again
            elif type(relation) is dict:
                if type(relation['value']) is str:
                    rel_list.append(relation['value'])
                elif type(relation['value']) is dict:
                    collect_relation_list(relation['value'], rel_list)
                else:
                    raise NotImplementedError(f"{relation['value']}")

# rename query when necessary
def rename_column(query_tree: typing.Union[dict, list], col_rel_dict: dict):
    if type(query_tree) is dict:
        for key, val in query_tree.items():
            if key in ['literal', 'interval']:
                continue
            if type(val) is str:
                if '.' not in val and val in col_rel_dict and len(col_rel_dict[val]) == 1:
                    query_tree[key] = f'{col_rel_dict[val][0]}.{val}'
            elif type(val) not in [int, float]:
                rename_column(val, col_rel_dict)
    elif type(query_tree) is list:
        for i, v in enumerate(query_tree):
            if type(v) is str:
                if '.' not in v and v in col_rel_dict and len(col_rel_dict[v]) == 1:
                    query_tree[i] = f'{col_rel_dict[v][0]}.{v}'
            elif type(v) not in [int, float]:
                rename_column(v, col_rel_dict)
    else:
        raise NotImplementedError(f"{query_tree}")