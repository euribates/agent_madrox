#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import date, datetime

from pony.orm import Database, set_sql_debug, db_session, select

from models import open_source_database, open_target_database



hoy = date.today()
db = Database()
print(db)

db_source = open_source_database()
db_target = open_target_database()
with db_session():
    for j in select(_ for _ in db_source.Jornada if _.fecha >= hoy):
        id_sala = j.sala.id_sala
        sala = db_target.Sala[id_sala]
        if j.sesion:
            sesion = db_target.Sesion.get(id_sesion=j.sesion.id_sesion)
            print(j, sesion, sesion is None)
        # print(j.id_jornada, j.descripcion) # , sala == j.sala)
        # print(
            # all([
                # sala.id_sala == j.sala.id_sala,
                # sala.descripcion == j.sala.descripcion,
                # sala.url == j.sala.url,
                # sala.rutaimagen == j.sala.rutaimagen,
            # ])
        # )
        # print(j.sesion)
