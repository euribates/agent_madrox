#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import typer
import logging

import dba
from models import Tarea, Jornada, Nota, Sesion, SesionDatos
from models import Usuario
from models import Noticia
from clitools import ERROR, OK

app = typer.Typer()


@app.command()
def sesion_datos(id_sesion):
    db_source = dba.get_database_connection('DB_SOURCE')
    db_target = dba.get_database_connection('DB_TARGET')

    print(f"Migrando sesión_datos {id_sesion}", end=" ")
    for success, message in SesionDatos.migrar(db_source, db_target, id_sesion):
        print(f"{OK} {message}" if success else f"{ERROR} {message}")


@app.command()
def sesion(id_sesion):
    db_source = dba.get_database_connection('DB_SOURCE')
    db_target = dba.get_database_connection('DB_TARGET')

    print(f"Migrando sesión {id_sesion}", end=" ")
    for success, message in Sesion.migrar(db_source, db_target, id_sesion):
        print(f"{OK} {message}" if success else f"{ERROR} {message}")


@app.command()
def jornada(id_jornada: int):
    db_source = dba.get_database_connection('DB_SOURCE')
    db_target = dba.get_database_connection('DB_TARGET')

    print(f"Migrando jornada {id_jornada}", end=" ")
    for success, message in Jornada.migrar(db_source, db_target, id_jornada):
        print(f"{OK} {message}" if success else f"{ERROR} {message}")


@app.command()
def jornadas(num_days: int = 7):
    db_source = dba.get_database_connection('DB_SOURCE')
    db_target = dba.get_database_connection('DB_TARGET')

    print(f"Actualizando jornadas desde hace {num_days} días")
    for id_jornada in Jornada.since(db_source, num_days):
        for success, message in Jornada.migrar(db_source, db_target, id_jornada):
            if success and message == 'Skipped':
                continue
            print(f"- Migrando Jornada {id_jornada}", end=" ")
            print(f"{OK} {message}" if success else f"{ERROR} {message}")


@app.command()
def tarea(id_tarea: int):
    db_source = dba.get_database_connection('DB_SOURCE')
    db_target = dba.get_database_connection('DB_TARGET')

    print(f"Migrando tarea {id_tarea}", end=" ")
    for success, message in Tarea.migrar(db_source, db_target, id_tarea):
        print(f"{OK} {message}" if success else f"{ERROR} {message}")


@app.command()
def tareas(num_days: int = 7):
    db_source = dba.get_database_connection('DB_SOURCE')
    db_target = dba.get_database_connection('DB_TARGET')
    print(f"Actualizando tareas.tarea desde hace {num_days} días")
    for id_tarea in Tarea.since(db_source, num_days):
        for success, message in Tarea.migrar(db_source, db_target, id_tarea):
            if success and message == 'Skipped':
                continue
            print(f"- Migrando tarea {id_tarea}", end=" ")
            print(f"{OK} {message}" if success else f"{ERROR} {message}")


@app.command()
def usuarios(num_days: int = 7):
    db_source = dba.get_database_connection('DB_SOURCE')
    db_target = dba.get_database_connection('DB_TARGET')
    print(f"Actualizando usuarios desde hace {num_days} días")
    for id_usuario in Usuario.since(db_source, num_days):
        for success, message in Usuario.migrar(db_source, db_target, id_usuario):
            if success and message == 'Skipped':
                continue
            print(f"- Migrando usuario {id_usuario}", end=" ")
            print(f"{OK} {message}" if success else f"{ERROR} {message}")


@app.command()
def noticias(num_days: int = 7):
    db_source = dba.get_database_connection('DB_SOURCE')
    db_target = dba.get_database_connection('DB_TARGET')
    print(f"Actualizando noticias desde hace {num_days} días")
    for id_noticia in Noticia.since(db_source, num_days):
        for success, message in Noticia.migrar(db_source, db_target, id_noticia):
            if success and message == 'Skipped':
                continue
            print(f"- Migrando noticia {id_noticia}", end=" ")
            print(f"{OK} {message}" if success else f"{ERROR} {message}")


@app.command()
def noticia(id_noticia: int):
    db_source = dba.get_database_connection('DB_SOURCE')
    db_target = dba.get_database_connection('DB_TARGET')

    print(f"Migrando noticia {id_noticia}", end=" ")
    for success, message in Noticia.migrar(db_source, db_target, id_noticia):
        print(f"{OK} {message}" if success else f"{ERROR} {message}")


@app.command()
def all(num_days: int = 7):
    jornadas(num_days)
    tareas(num_days)
    usuarios(num_days)
    noticias(num_days)


def has_equal_num_of_rows(db_source, db_target, table, query=''):
    sql = f'SELECT count(*)\n  FROM {table}'
    if query:
        sql = f'{sql}\n WHERE {query}'
    num_source = dba.get_scalar(db_source, sql)
    num_target = dba.get_scalar(db_target, sql)
    if num_source == num_target:
        return True, ''
    else:
        return False, f'Hay {num_source} registros en origen pero {num_target} en destino'


def check_actas(db_source, db_target, eleccion):
    table = 'Elecciones.Actas'
    query = f'eleccion = {eleccion}'
    print(f'- {table} {eleccion}', end=' ')
    is_ok, error_message = has_equal_num_of_rows(db_source, db_target, table, query)
    print(f'{OK}' if is_ok else f'{ERROR} {error_message}')


@app.command()
def check():
    db_source = dba.get_database_connection('DB_SOURCE')
    db_target = dba.get_database_connection('DB_TARGET')
    elecciones = [2003, 2007, 2011, 2015, 2019, 2023]
    print('Checking elecciones...')
    for eleccion in elecciones:
        check_actas(db_source, db_target, eleccion)



if __name__ == "__main__":
    logging.basicConfig(filename='madrox.log', level='DEBUG')
    logger = logging.getLogger('models')
    logger.setLevel(logging.DEBUG)
    app()
