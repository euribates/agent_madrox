#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import sys
from datetime import date, datetime, timedelta

from pony.orm import Database, db_session, select, set_sql_debug

from clitools import ERROR, OK, SKIP, green, red, yellow
import dba
from models import open_source_database, open_target_database
from models import Tarea


def migrar_sala(db_source, db_target, id_sala: int) -> int:
    with db_session():
        source_item = db_source.Sala.get(id_sala=id_sala)
        if not source_item:
            return False, f"Unable to load Sala {id_sala}"
        target_item = db_target.Sala.get(id_sala=id_sala)
        if not target_item:  # Insert
            values = source_item.to_dict(exclude="id_sala")
            new_sesion = db_target.Sala(id_sala=id_sala, **values)
            new_sesion.flush()
            logging.info(f"- Inserte Sala {OK}")
            return True, "Inserted"
        new_values = source_item.to_dict(exclude="id_sala")
        old_values = target_item.to_dict(exclude="id_sala")
        if new_values != old_values:  # Update
            target_item.set(**new_values)
            target_item.flush()
            logging.info(f"- Update Sala {OK}")
            return True, "Updated"
        logging.info(f"- Skipped Sala {SKIP}")
        return True, "Skipped"


def migrar_organo(db_source, db_target, id_organo: str) -> int:
    with db_session():
        source_item = db_source.Organo.get(id_organo=id_organo)
        if not source_item:
            return False, f"Unable to load Organo {id_organo}"
        target_item = db_target.Organo.get(id_organo=id_organo)
        if not target_item:  # Insert
            values = source_item.to_dict(exclude="id_organo")
            new_sesion = db_target.Organo(id_organo=id_organo, **values)
            new_sesion.flush()
            logging.info(f"- Insert Organo {OK}")
            return True, "Inserted"
        new_values = source_item.to_dict(exclude="id_organo")
        old_values = target_item.to_dict(exclude="id_organo")
        if new_values != old_values:  # Update
            target_item.set(**new_values)
            target_item.flush()
            logging.info(f"- Update Organo {OK}")
            return True, "Updated"
        logging.info(f"- Skipped Organo {SKIP}")
        return True, "Skipped"


def migrar_sesion(db_source, db_target, id_sesion: str) -> int:
    with db_session():
        source_item = db_source.Sesion.get(id_sesion=id_sesion)
        if not source_item:
            return False, f"Unable to load Sesion_Datos {id_sesion}"
        if source_item.id_organo:
            migrar_organo(db_source, db_target, source_item.id_organo)
        target_item = db_target.Sesion.get(id_sesion=id_sesion)
        if not target_item:  # Insert
            values = source_item.to_dict(exclude="id_sesion")
            new_sesion = db_target.Sesion(id_sesion=id_sesion, **values)
            new_sesion.flush()
            logging.info(f"- Insert Sesion {OK}")
            return True, "Inserted"
        new_values = source_item.to_dict(exclude="id_sesion")
        old_values = target_item.to_dict(exclude="id_sesion")
        if new_values != old_values:  # Update
            target_item.set(**new_values)
            target_item.flush()
            logging.info(f"- Update Sesion {OK}")
            return True, "Updated"
        logging.info(f"- Skipped Sesion {SKIP}")
        return True, "Skipped"


def migrar_sesion_datos(db_source, db_target, id_sesion: str) -> int:
    with db_session():
        source_item = db_source.SesionDatos.get(id_sesion=id_sesion)
        if not source_item:
            return False, f"Unable to load Sesion_Datos {id_sesion}"
        target_item = db_target.SesionDatos.get(id_sesion=id_sesion)
        if not target_item:  # Insert
            values = source_item.to_dict(exclude="id_sesion")
            new_sesion = db_target.SesionDatos(id_sesion=id_sesion, **values)
            new_sesion.flush()
            logging.info(f"- Insert SesionDatos {OK}")
            return True, "Inserted"
        new_values = source_item.to_dict(exclude="id_sesion")
        old_values = target_item.to_dict(exclude="id_sesion")
        if new_values != old_values:  # Update
            target_item.set(**new_values)
            target_item.flush()
            logging.info(f"- Update SesionDatos {OK}")
            return True, "Updated"
        logging.info(f"- Skipped SesionDatos {SKIP}")
        return True, "Skipped"


def migrar_asuntos(db_source, db_target, id_sesion: str) -> int:
    with db_session():
        source_item = db_source.SesionDatos.get(id_sesion=id_sesion)
        if not source_item:
            return False, f"Unable to load Asuntos {id_sesion}"
        target_item = db_target.SesionDatos.get(id_sesion=id_sesion)
        if not target_item:  # Insert
            values = source_item.to_dict(exclude="id_sesion")
            new_sesion = db_target.SesionDatos(id_sesion=id_sesion, **values)
            new_sesion.flush()
            logging.info(f"- Insert SesionDatos {OK}")
            return True, "Inserted"
        new_values = source_item.to_dict(exclude="id_sesion")
        old_values = target_item.to_dict(exclude="id_sesion")
        if new_values != old_values:  # Update
            target_item.set(**new_values)
            target_item.flush()
            logging.info(f"- Update SesionDatos {OK}")
            return True, "Updated"
        logging.info(f"- Skipped SesionDatos {SKIP}")
        return True, "Skipped"


def migrar_jornada(db_source, db_target, id_jornada):
    with db_session():
        source_item = db_source.Jornada.get(id_jornada=id_jornada)
        if not source_item:
            return False, f"Unable to load Jornada {id_jornada}"
        if source_item.id_sala:
            migrar_sala(db_source, db_target, source_item.id_sala)
        if source_item.id_sesion:
            migrar_sesion(db_source, db_target, source_item.id_sesion)
            migrar_sesion_datos(db_source, db_target, source_item.id_sesion)
            migrar_asuntos(db_source, db_target, source_item.id_sesion)
        # Dependecies migrated
        target_item = db_target.Jornada.get(id_jornada=id_jornada)
        if not target_item:  # Insert
            values = source_item.to_dict(exclude="id_jornada")
            new_sesion = db_target.Jornada(id_jornada=id_jornada, **values)
            new_sesion.flush()
            logging.info(f"- Insert Jornada {OK}")
            return True, 'Inserted'
        new_values = source_item.to_dict(exclude="id_jornada")
        old_values = target_item.to_dict(exclude="id_jornada")
        if new_values != old_values:  # Update
            target_item.set(**new_values)
            target_item.flush()
            logging.info(f"- Update Jornada {OK}")
            return True, 'Updated'
    logging.info(f"- Skipped Jornada {SKIP}")
    return True, 'Skipped'


def disable_triggers(db_handler):
    with db_session():
        db_handler.execute('alter trigger agora.CREAR_JORNADAS disable')


def enable_triggers(db_handler):
    with db_session():
        db_handler.execute('alter trigger agora.CREAR_JORNADAS enable')


def migrar_tarea(db_source, db_target, id_tarea):
    print(f"Migrando tarea {id_tarea}")
    source_item = Tarea.load_instance(db_source, id_tarea)
    if not source_item:
        return False, f"Unable to load Tarea {id_tarea}"
    # Falta Dependecies migrated
    target_item = Tarea.load_instance(db_target, id_tarea)
    if not target_item:  # Insert
        values = Tarea.to_dict(source_item)
        print(Tarea.insert(db_target, values))
        logging.info(f"- Insert tarea {OK}")
        return True, 'Inserted'
    new_values = Tarea.to_dict(source_item, exclude={"id_jornada"})
    old_values = Tarea.to_dict(target_item, exclude={"id_jornada"})
    if new_values != old_values:  # Update
        diff_values = {}
        for name in old_values:
            if new_values[name] != old_values[name]:
                diff_values[name] = new_values[name]
        print(target_item.update(db_target, id_tarea, diff_values))
        logging.info(f"- Update Jornada {OK}")
        return True, 'Updated'
    logging.info(f"- Skipped tarea {SKIP}")
    return True, 'Skipped'


def main():
    argc = len(sys.argv)
    num_dias = int(sys.argv[1]) if argc > 1 else 1
    fecha = date.today() - timedelta(days=num_dias)
    # db_source = open_source_database()
    # db_target = open_target_database()
    # print(f"Actualizando jornadas desde {fecha} (hace {num_dias} dias)")
    # disable_triggers(db_target)
    # try:
        # with db_session():
            # for jornada in select(_ for _ in db_source.Jornada if _.fecha >= fecha):
                # print(f"- {jornada.id_jornada}: {jornada.descripcion}", end=" ")
                # success, message = migrar_jornada(db_source, db_target, jornada.id_jornada)
                # print(f"{OK} {message}" if success else f"{ERROR} {message}")
    # finally:
        # enable_triggers(db_target)
    # Using dba
    db_source = dba.get_database_connection('DB_SOURCE')
    db_target = dba.get_database_connection('DB_TARGET')
    print(f"Actualizando tareas desde {fecha} (hace {num_dias} días)")
    print("Obtener tareas")

    query = 'f_creacion > :1 OR f_ultima_act > :1'
    sql = f"Select {Tarea._primary_key} as pk from {Tarea._table_name} WHERE {query}"
    print(sql, fecha)
    ids_tareas = dba.get_rows(db_source, sql, fecha, cast=lambda row: row['pk'])
    print(len(ids_tareas))
    for id_tarea in ids_tareas:
        migrar_tarea(db_source, db_target, id_tarea)




if __name__ == "__main__":
    main()
