#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys

import dba
from models import Tarea, Jornada, Nota
from models import Usuario
from clitools import ERROR, OK


def main():
    argc = len(sys.argv)
    num_dias = int(sys.argv[1]) if argc > 1 else 1
    db_source = dba.get_database_connection('DB_SOURCE')
    db_target = dba.get_database_connection('DB_TARGET')

    print(f"Actualizando jornadas desde hace {num_dias} días")
    for id_jornada in Jornada.since(db_source, num_dias):
        print(f"- Migrando Jornada {id_jornada}", end=" ")
        success, message = Jornada.migrar(db_source, db_target, id_jornada)
        print(f"{OK} {message}" if success else f"{ERROR} {message}")

    # print(f"Actualizando tareas.nota desde hace {num_dias} días")
    # dba.execute(db_target, 'alter trigger tareas.ON_UPDATE_NOTA disable')
    # dba.execute(db_target, 'alter trigger tareas.ON_INSERT_NOTA disable')
    # for id_nota in Nota.since(db_source, num_dias):
        # print(f"- Migrando nota {id_nota}", end=" ")
        # success, message = Nota.migrar(db_source, db_target, id_nota)
        # print(f"{OK} {message}" if success else f"{ERROR} {message}")
    # dba.execute(db_target, 'alter trigger tareas.ON_UPDATE_NOTA enable')
    # dba.execute(db_target, 'alter trigger tareas.ON_INSERT_NOTA enable')

    print(f"Actualizando tareas.tarea desde hace {num_dias} días")
    for id_tarea in Tarea.since(db_source, num_dias):
        print(f"- Migrando tarea {id_tarea}", end=" ")
        success, message = Tarea.migrar(db_source, db_target, id_tarea)
        print(f"{OK} {message}" if success else f"{ERROR} {message}")


    print(f"Actualizando usuarios desde hace {num_dias} días")
    for id_usuario in Usuario.since(db_source, num_dias):
        print(f"- Migrando usuario {id_usuario}", end=" ")
        success, message = Usuario.migrar(db_source, db_target, id_usuario)
        print(f"{OK} {message}" if success else f"{ERROR} {message}")



if __name__ == "__main__":
    main()
