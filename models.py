#!/usr/bin/env python3

from datetime import date, datetime

from pony.orm import Database, PrimaryKey, Required, Optional, set_sql_debug

db = Database()
db.bind(
    provider='postgres',
    host='localhost',
    user='parcanweb',
    password='parcanweb',
    database='parcanweb',
)
set_sql_debug(True)


class SesionDatos(db.Entity):
    _table_ = ("agora", "sesion_datos")
    id_sesion = PrimaryKey(int)
    convocada = Required(str)
    f_convocada = Required(date)
    f_desconvocada = Optional(date)
    id_usuario = Optional(int)
    confirmada = Required(
        str,
        max_len=1,
        default='N',
        py_check=lambda val: val in {'S', 'N'},
    )


class Jornada(db.Entity):
    _table_ = ("agora", "jornada")

    id_jornada = PrimaryKey(int)
    n_grabaciones = Required(int, default=0)
    id_sesion = Required(str)
    id_organo = Required(str)
    fecha = Required(date)
    hora = Required(str, max_len=5)
    descripcion = Required(str, max_len=512)
    texto = Optional(str, max_len=4000, default='')
    id_sala = Optional(int)
    canal = Required(int, default=0)
    hora_fin = Optional(str, max_len=5)
    id_sesion_seneca = Optional(int)
    id_sesion_seneca_fx = Optional(int)
    estado = Required(str, max_len=5, default='READY')
    secreta = Required(str, max_len=1, default='S')
    hora_reanudacion = Optional(str, max_len=5)


db.generate_mapping()
