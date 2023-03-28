#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys

import typer

import dba
from models import Tarea, Jornada, Nota
from models import Usuario
from clitools import ERROR, OK

app = typer.Typer()


@app.command()
def jornadas(num_days: int=7):
    db_source = dba.get_database_connection('DB_SOURCE')
    db_target = dba.get_database_connection('DB_TARGET')

    print(f"Actualizando jornadas desde hace {num_days} días")
    for id_jornada in Jornada.since(db_source, num_days):
        success, message = Jornada.migrar(db_source, db_target, id_jornada)
        if success and message == 'Skipped':
            continue
        print(f"- Migrando Jornada {id_jornada}", end=" ")
        print(f"{OK} {message}" if success else f"{ERROR} {message}")


@app.command()
def tareas(num_days: int=7):
    db_source = dba.get_database_connection('DB_SOURCE')
    db_target = dba.get_database_connection('DB_TARGET')
    print(f"Actualizando tareas.tarea desde hace {num_days} días")
    for id_tarea in Tarea.since(db_source, num_days):
        success, message = Tarea.migrar(db_source, db_target, id_tarea)
        if success and message == 'Skipped':
            continue
        print(f"- Migrando tarea {id_tarea}", end=" ")
        print(f"{OK} {message}" if success else f"{ERROR} {message}")


@app.command()
def usuarios(num_days: int=7):
    db_source = dba.get_database_connection('DB_SOURCE')
    db_target = dba.get_database_connection('DB_TARGET')
    print(f"Actualizando usuarios desde hace {num_days} días")
    for id_usuario in Usuario.since(db_source, num_days):
        success, message = Usuario.migrar(db_source, db_target, id_usuario)
        if success and message == 'Skipped':
            continue
        print(f"- Migrando usuario {id_usuario}", end=" ")
        print(f"{OK} {message}" if success else f"{ERROR} {message}")


@app.command()
def all(num_days: int=7):
    jornadas(num_days)
    tareas(num_days)
    usuarios(num_days)


if __name__ == "__main__":
    app()
