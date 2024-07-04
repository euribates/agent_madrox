#!/usr/bin/env python3.12
# -*- coding: utf-8 -*-

import sys
import argparse
from typing import Final

from rich.console import Console
import dml
import dba

OK: Final[str] = '[green]✓[/green]'
ERROR: Final[str] = '[red]✖[/red]'


def red(msg):
    """Devuelve el texto en color rojo (Para terminal/consola ANSI)."""
    return f"[red]{msg}[/red]"


def check_table_size(db_source, db_target, table_name):
    sql = dml.Select('Count(*)').From(table_name)
    rows_in_source = dba.get_scalar(db_source, sql)
    rows_in_target = dba.get_scalar(db_target, sql)
    if rows_in_source != rows_in_target:
        return (
            f'No son del mismo tamaño: En Origen {rows_in_source},'
            f' en destino {rows_in_target}'
            )
    return ''

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
        return f'No coincide la suma del campo {field_name}'
    if source_row['media'] != target_row['media']:
        return f'No coincide la media del campo {field_name}'
    if source_row['maximo'] != target_row['maximo']:
        return f'No coincide el maximo del campo {field_name}'
    if source_row['minimo'] != target_row['minimo']:
        return f'No coincide el minimo del campo {field_name}'
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
    console = Console()
    console.print(f'Comprobando [green]{options.table_name}[/green]', end=" : ")
    err = check_table_size(db_source, db_target, options.table_name)
    if err:
        console.print(ERROR)
        console.print(red(err))
        return -1
    console.print(f'size {OK}', end=' ')
    if options.number:
        for field_name in options.number:
            console.print(field_name, end=' ')
            err = check_table_number(db_source, db_target, options.table_name, field_name)
            if err:
                console.print(ERROR)
                console.print(red(err))
                return -1
            console.print(OK, end=' ')
        console.print()
    return 0


if __name__ == "__main__":
    status = main()
    sys.exit(status)
