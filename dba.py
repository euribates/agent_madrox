#!/usr/bin/env python
# -*- coding: utf-8 -*-


import functools

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


