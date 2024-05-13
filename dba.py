#!/usr/bin/env python
# -*- coding: utf-8 -*-


import functools

from prettyconf import config


def connection_params_from_db_url(conn_string):
    assert '://' in conn_string
    user = password = host = port = name = None
    schema, rest = conn_string.split('://', 1)
    if '@' in rest:
        user_and_password, host_name_and_port = rest.split('@', 1)
        assert ':' in user_and_password
        user, password = user_and_password.split(':', 1)
    else:
        host_name_and_port = rest
    if '/' in host_name_and_port:
        if host_name_and_port.startswith('/'):
            name = host_name_and_port[1:]
        else:
            host_port, name = host_name_and_port.split('/', 1)
            if ':' in host_port:
                host, port = host_port.split(':')
                port = int(port)
            else:
                host = host_port
    else:
        name = host_name_and_port
    return {
        'schema': schema,
        'user': user,
        'password': password,
        'host': host,
        'port': port,
        'name': name,
    }


def get_oracle_connection(db_name, user, password):
    import cx_Oracle
    connection_string = f'{user}/{password}@{db_name}'
    db_connection = cx_Oracle.connect(
        connection_string,
        encoding="UTF-8",
        nencoding="UTF-8",
    )
    db_connection.autocommit = True
    return db_connection


def get_database_connection(dsn):
    db_connection_url = config(dsn)
    connection_parameters = connection_params_from_db_url(db_connection_url)
    schema = connection_parameters['schema']
    match schema:
        case 'oracle':
            db_name = connection_parameters['name']
            user = connection_parameters['user']
            password = connection_parameters['password']
            return get_oracle_connection(db_name, user, password)
        case _:
            raise ValueError("Imposible conectarme a bases de datos de tipo {_}")


def execute(dbc, sql, *args):
    sql = str(sql)
    parameters = list(args)
    result = None
    with dbc.cursor() as cur:
        result = cur.execute(sql, parameters)
    return result


def get_row(dbc, sql, *args, cast=None):
    sql = str(sql)
    field_names = []
    parameters = list(args)
    with dbc.cursor() as cur:
        cur.execute(sql, parameters)
        field_names = [desc[0].lower() for desc in cur.description]
        row = cur.fetchone()
        if row:
            row = dict(zip(field_names, row))
            if cast:
                row = cast(row)
            return row
    return {}


def get_rows(dbc, sql, *args, cast=None):
    sql = str(sql)
    field_names = []
    parameters = list(args)
    with dbc.cursor() as cur:
        cur.execute(sql, parameters)

        field_names = [desc[0].lower() for desc in cur.description]
        rows = cur.fetchall()
        if rows:
            rows = [dict(zip(field_names, row)) for row in rows]
            if cast:
                rows = [cast(row) for row in rows]
            return rows
    return []


def get_scalar(dbc, sql, cast=None, default=None):
    """Obtener un único valor desde la base de datos.
    """
    row = get_row(dbc, sql, cast=cast)
    result = list(row.values())[0] if row else default
    return result


def es_oracle(conn):
    cursor = None
    try:
        cursor = conn.cursor()
        return 'oracle' in cursor.db.__module__
    finally:
        if cursor:
            cursor.close()


def next_val(sequence_name, conn="default"):
    '''Obtener el siguiente número de una sequencia Oracle.

      * El primer parámetro es el nombre de la sequencia a utilizar.
      * El segundo parámetro es la conexión de la base de datos a
        utilizar. Es opcional, si no se especifica, se usará
        la conexión por defecto.

    Devuelve sl siguiente número de la secuencia, e incrementa
    el contador de forma atómica.
    '''

    db_conn = get_database_connection(conn)
    if es_oracle(db_conn):
        sql = "SELECT {}.NextVal FROM Dual".format(sequence_name)
    else:  # Será postgres
        sql = "SELECT nextval('{}')".format(sequence_name)
    cur = db_conn.cursor()
    try:
        cur.execute(sql)
        row = cur.fetchone()
        return row[0]
    finally:
        cur.close()


def listify(value) -> list:
    """Convertir a lista.

    Si se le pasa una lista, tupla o diccionario, devuelve una lista.
    Si se le pasa otra cosa, devuelve una lista con ese
    valor como único elemento.
    """
    if isinstance(value, (tuple, list)):
        return list(value)
    elif isinstance(value, dict):
        return [(k, value[k]) for k in value]
    return [value]



def create_locator(table, instance) -> list:
    conditions = []
    for field_name in listify(table._primary_key):
        value = getattr(instance, field_name)
        conditions.append(f'{field_name} = {value}')
    return conditions


def create_conditions(table, instance) -> str:
    pk_fields = listify(table._primary_key)
    conditions = create_locator(instance, pk_fields)
    first_condition = conditions.pop(0)
    sql = [
        f'SELECT {fields}',
        f'  FROM {table._table_name}',
        f' WHERE {first_condition}',
    ]
    if conditions:
        # It there are more conditions (Composed primary key)
        for condition in conditions:
            sql.append('  AND {condition}')
    return '\n'.join(sql)


def create_select(table, instance, fields='*') -> str:
    conditions = create_locator(table, instance)
    first_condition = conditions.pop(0)
    sql = [
        f'SELECT {fields}',
        f'  FROM {table._table_name}',
        f' WHERE {first_condition}',
    ]
    if conditions:
        # It there are more conditions (Composed primary key)
        for condition in conditions:
            sql.append('  AND {condition}')
    return '\n'.join(sql)


create_exists = functools.partial(create_select, fields="Count(*)")
