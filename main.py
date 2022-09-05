#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import date, datetime

from pony.orm import Database, set_sql_debug, db_session, select

from models import SesionDatos, Jornada



hoy = date.today()
db = Database()
print(db)

with db_session():
    for j in select(_ for _ in Jornada if _.fecha >= hoy)[:]:
        print(j.id_jornada, j.id_sesion, j.id_organo, j.id_sala)

