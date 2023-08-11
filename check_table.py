#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse

from clitools import ERROR, OK, green
import dml
import dba


def check_table_size(db_source, db_target, table_name):
    sql = dml.Select('Count(*)').From(table_name)
    return dba.get_scalar(db_source, sql) == dba.get_scalar(db_target, sql)


def check_table_number(db_source, db_target, table_name, field_name):
    sql = dml.Select(
        f'SUM({field_name}) as suma,'
        f' AVG({field_name}) as media,'
        f' MAX({field_name}) as maximo,'
        f' MIN({field_name}) as minimo'
        ).From(table_name)
    source_row = dba.get_row(db_source, sql)
    target_row = dba.get_row(db_target, sql)
    if source_row['suma'] != target_row['suma']:
        return f'No coincide la suma del campo {table_name}'
    if source_row['media'] != target_row['media']:
        return f'No coincide la media del campo {table_name}'
    if source_row['maximo'] != target_row['maximo']:
        return f'No coincide el maximo del campo {table_name}'
    if source_row['minimo'] != target_row['minimo']:
        return f'No coincide el minimo del campo {table_name}'
    return ''


def get_options():
    parser = argparse.ArgumentParser(
        prog='check_table',
        description='Verifica integridad de una tabla entre oracle y oracle_dev',
        epilog='Usar./check_table.py [<table_space>.])<table_name>',
        )
    parser.add_argument('table_name')
    parser.add_argument('-n', '--number', nargs='*')
    return parser.parse_args()


def main():
    options = get_options()
    db_source = dba.get_database_connection('DB_SOURCE')
    db_target = dba.get_database_connection('DB_TARGET')
    print(f'Comprobando {green(options.table_name)}', end=" : ")
    if options.number:
        for field_name in options.number:
            err = check_table_number(db_source, db_target, options.table_name, field_name)
            print (err or f'{field_name} {OK}', end=' ')
    print(OK if check_table_size(db_source, db_target, options.table_name) else ERROR)


if __name__ == "__main__":
    main()
