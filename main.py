#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import date, datetime
from contextlib import contextmanager

from pony.orm import Database, set_sql_debug, db_session, select

from models import open_source_database, open_target_database


def red(msg):
    """Devuelve el texto en color rojo (Para terminal/consola ANSI)."""
    return f"\u001b[31m{msg}\u001b[0m"


def green(msg):
    """Devuelve el texto en color verde (Para terminal/consola ANSI).
    """
    return f"\u001b[32m{msg}\u001b[0m"


def yellow(msg):
    """Devuelve el texto en color amarillo (Para terminal/consola ANSI).
    """
    return f"\u001b[33m{msg}\u001b[0m"


ERROR = red("[✖]")
OK = green("[✓]")
SKIPPED = yellow("[⤼]")


def migrar_sala(db_source, db_target, id_sala: int) -> int:
    if id_sala:
        source_item = db_source.Sala.get(id_sala=id_sala)
        if source_item:
            target_item = db_target.Sala.get(id_sala=id_sala)
            if not target_item:  # Insert
                values = source_item.to_dict(exclude="id_sala")
                new_sesion = db_target.Sala(id_sala=id_sala, **values)
                new_sesion.flush()
                print(f"- Update Sala {OK}")
                return 1
            new_values = source_item.to_dict(exclude="id_sala")
            old_values = target_item.to_dict(exclude="id_sala")
            if new_values != old_values:  # Update
                target_item.set(**new_values)
                target_item.flush()
                print(f"- Insert Sala {OK}")
                return 1
    print(f"- Skipped Sala {SKIPPED}")
    return 0


def migrar_organo(db_source, db_target, id_organo: str) -> int:
    if id_organo:
        source_item = db_source.Organo.get(id_organo=id_organo)
        if source_item:
            target_item = db_target.Organo.get(id_organo=id_organo)
            if not target_item:  # Insert
                values = source_item.to_dict(exclude="id_organo")
                new_sesion = db_target.Organo(id_organo=id_organo, **values)
                new_sesion.flush()
                print(f"- Update Organo {OK}")
                return 1
            new_values = source_item.to_dict(exclude="id_organo")
            old_values = target_item.to_dict(exclude="id_organo")
            if new_values != old_values:  # Update
                target_item.set(**new_values)
                target_item.flush()
                print(f"- Insert Organo {OK}")
                return 1
    print(f"- Skipped Organo {SKIPPED}")
    return 0


def migrar_sesion(db_source, db_target, id_sesion: str) -> int:
    if id_sesion:
        source_item = db_source.Sesion.get(id_sesion=id_sesion)
        if source_item:
            if source_item.id_organo:
                migrar_organo(db_source, db_target, source_item.id_organo)
            target_item = db_target.Sesion.get(id_sesion=id_sesion)
            if not target_item:  # Insert
                values = source_item.to_dict(exclude="id_sesion")
                new_sesion = db_target.Sesion(id_sesion=id_sesion, **values)
                new_sesion.flush()
                print(f"- Update Sesion {OK}")
                return 1
            new_values = source_item.to_dict(exclude="id_sesion")
            old_values = target_item.to_dict(exclude="id_sesion")
            if new_values != old_values:  # Update
                target_item.set(**new_values)
                target_item.flush()
                print(f"- Insert Sesion {OK}")
                return 1
    print(f"- Skipped Sesion {SKIPPED}")
    return 0


def migrar_sesion_datos(db_source, db_target, id_sesion: str) -> int:
    if id_sesion:
        source_item = db_source.SesionDatos.get(id_sesion=id_sesion)
        if source_item:
            target_item = db_target.SesionDatos.get(id_sesion=id_sesion)
            if not target_item:  # Insert
                values = source_item.to_dict(exclude="id_sesion")
                new_sesion = db_target.SesionDatos(id_sesion=id_sesion, **values)
                new_sesion.flush()
                print(f"- Update SesionDatos {OK}")
                return 1
            new_values = source_item.to_dict(exclude="id_sesion")
            old_values = target_item.to_dict(exclude="id_sesion")
            if new_values != old_values:  # Update
                target_item.set(**new_values)
                target_item.flush()
                print(f"- Insert SesionDatos {OK}")
                return 1
    print(f"- Skipped SesionDatos {SKIPPED}")
    return 0


def migrar_jornada(db_source, db_target, id_jornada):
    if id_jornada:
        source_item = db_source.Jornada.get(id_jornada=id_jornada)
        if source_item.id_sala:
            migrar_sala(db_source, db_target, source_item.id_sala)
        if source_item.id_organo:
            migrar_organo(db_source, db_target, source_item.id_organo)
        if source_item.id_sesion:
            migrar_sesion(db_source, db_target, source_item.id_sesion)
            migrar_sesion_datos(db_source, db_target, source_item.id_sesion)
        if source_item:
            target_item = db_target.Jornada.get(id_jornada=id_jornada)
            if not target_item:  # Insert
                values = source_item.to_dict(exclude="id_jornada")
                new_sesion = db_target.Jornada(id_jornada=id_jornada, **values)
                new_sesion.flush()
                print(f"- Update Jornada {OK}")
                return 1
            new_values = source_item.to_dict(exclude="id_jornada")
            old_values = target_item.to_dict(exclude="id_jornada")
            if new_values != old_values:  # Update
                target_item.set(**new_values)
                target_item.flush()
                print(f"- Insert Jornada {OK}")
                return 1
    print(f"- Skipped Jornada {SKIPPED}")
    return 0


@contextmanager
def frame_decoration(titulo, *args, **kwargs):
    l = len(titulo)
    print(f"--[ {titulo} ]{'-'* (66 - l)}")
    try:
        yield
    finally:
        print(f"{'-'*72}")


def main():
    hoy = date.today()
    db_source = open_source_database()
    db_target = open_target_database()
    with db_session():
        for jornada in select(_ for _ in db_source.Jornada if _.fecha >= hoy):
            with frame_decoration(f"Jornada {jornada.id_jornada}"):
                print(f"- {jornada.descripcion}")
                migrar_jornada(db_source, db_target, jornada.id_jornada)


if __name__ == "__main__":
    main()
