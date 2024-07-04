#!/usr/bin/env python3.12
# -*- coding: utf-8 -*-

import sys
import argparse
from typing import Final

from rich.console import Console
import models
import dml
import dba

OK: Final[str] = '[green]✓[/green]'
ERROR: Final[str] = '[red]✖[/red]'


def red(msg):
    """Devuelve el texto en color rojo (Para terminal/consola ANSI)."""
    return f"[red]{msg}[/red]"


def get_primary_keys(db_source, db_target, table_name):
    _model = getattr(models, table_name)
    primary_key = _model._primary_key
    table_name = _model._table_name
    sql = dml.Select(primary_key).From(table_name)
    keys_in_source = set([row[primary_key] for row in dba.get_rows(db_source, sql)])
    keys_in_target = set([row[primary_key] for row in dba.get_rows(db_target, sql)])
    return keys_in_source, keys_in_target


def get_options():
    parser = argparse.ArgumentParser(
        prog='show_diff',
        description='Muestra claves primarias diferentes entre destino y objetivo',
        epilog='Usar ./show_diff.py [<table_space>.])<table_name>',
        )
    parser.add_argument('table_name', nargs='+')
    parser.add_argument('--verbose', action='store_true')
    return parser.parse_args()


def main():
    options = get_options()
    db_source = dba.get_database_connection('DB_SOURCE')
    db_target = dba.get_database_connection('DB_TARGET')
    console = Console()
    for name in options.table_name:
        console.print(f'Comprobando [green]{name}[/green]', end=" : ")
        keys_in_source, keys_in_target = get_primary_keys(db_source, db_target, name)
        num_in_source = len(keys_in_source)
        num_in_target = len(keys_in_target)
        a_insertar = a_borrar = set()
        if options.verbose:
            console.print(f'[En origen: {num_in_source}]', end=' ')
            console.print(f'[En destino: {num_in_target}]', end=' ')
        if num_in_target < num_in_source:
            a_insertar = keys_in_source - keys_in_target
            console.print(f'Faltan [red][bold]{len(a_insertar)}[/] en destino')
            if options.verbose and a_insertar:
                for pk in a_insertar:
                    console.print(pk)
        elif num_in_target > num_in_source:
            a_borrar = keys_in_target - keys_in_source
            console.print(f'Sobran [red][bold]{len(a_borrar)}[/] en destino')
            if options.verbose and a_borrar:
                for pk in a_borrar:
                    console.print(pk)
        else:
            console.print(OK)


if __name__ == "__main__":
    status = main()
    sys.exit(status)
