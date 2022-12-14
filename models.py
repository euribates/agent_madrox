#!/usr/bin/env python3

from datetime import date, datetime
from dataclasses import dataclass


from pony.orm import (
    Database,
    Optional,
    PrimaryKey,
    Required,
    set_sql_debug,
)

import settings

# set_sql_debug(settings.DEBUG)


@dataclass
class TableIsla:
    _table_name = "AGORA.ISLA"
    _primary_key = 'id_isla'
    _required = [
        'id_isla',
        'descripcion',
        ]
    _depends_on = []

    id_isla: int
    descripcion: str
    ts_mod: str
    migrable: str


@dataclass
class TableOrgano:
    _table_name = "AGORA.ORGANO"
    _primary_key = 'id_organo'
    _required = [
        'legislatura',
        'nombre',
        'constitucion',
        'tipo',
        'tipo_comision',
        'codorg',
        ]
    _depends_on = [
        TableIsla,
    ]
    id_organo: str
    legislatura: int
    alias: str
    nombre: str
    direccion:str
    constitucion:datetime.date
    disolucion:datetime.date
    cp:str
    fax:str
    id_isla:int
    municipio:str
    telefono:str
    tipo:int
    id_usuario:str
    ts_mod:str
    tipo_comision:int
    codorg:int
    id_sala_habitual:int
    id_tramite_organo:str
    migrable:str
    url:str
    descripcion:str
    id_letrado:str
    nombre_completo:str




def define_entities(db):

    class Organo(db.Entity):
        _table_ = ("AGORA", "ORGANO")

        id_organo = PrimaryKey(str, max_len=6)
        legislatura = Required(int)
        alias = Required(str, max_len=32)
        nombre = Required(str, max_len=250)
        direccion = Optional(str, max_len=250)
        constitucion = Required(date)
        disolucion = Optional(date)
        cp = Optional(str, max_len=5)
        fax = Optional(str, max_len=14),
        id_isla = Optional(int),
        municipio = Optional(str, max_len=62),
        telefono = Optional(str, max_len=14),
        tipo = Required(int)
        id_usuario = Optional(str, max_len=8),
        ts_mod = Required(str, max_len=14),
        tipo_comision = Optional(int)
        codorg = Required(int)
        id_sala_habitual = Optional(int)
        id_tramite_organo = Optional(str, max_len=6),
        migrable = Required(str, max_len=1),
        url = Optional(str, max_len=320),
        descripcion = Optional(str, max_len=4000),
        id_letrado = Optional(str, max_len=5),
        nombre_completo = Optional(str, max_len=500)


    class Sesion(db.Entity):
        _table_ = ("AGORA", "SESION")

        id_sesion = PrimaryKey(str, max_len=11)
        id_organo = Required(str, max_len=6)
        fecha = Required(date)
        hora = Required(str, max_len=5)
        legislatura = Required(int)
        n_sesion = Required(int)
        tipo_sesion = Required(str, max_len=1)
        descripcion = Optional(str, max_len=512)
        id_sala = Required(int, default=0)
        n_asistentes = Required(int, default=0),
        prensa = Required(str, max_len=1)
        ts_mod = Required(str, max_len=14)
        id_usuario = Required(str, max_len=8)
        n_dias = Required(int, default=1)
        migrable = Required(str, max_len=1)


    class SesionDatos(db.Entity):
        _table_ = ("AGORA", "SESION_DATOS")

        id_sesion = PrimaryKey(str, max_len=11)
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


    class Asunto(db.Entity):
        _table_ = ("AGORA", "ASUNTO")

        id_asunto = PrimaryKey(str, max_len=14)
        id_sesion = Required(str, max_len=11)
        legislatura = Required(int)
        n_orden_asunto = Required(int)
        n_orden_tramite = Required(int)
        id_iniciativa = Required(str, max_len=13)
        descripcion = Required(str, max_len=110)
        n_sesion = Required(int)
        punto = Required(int)
        subpunto = Optional(int)
        extracto = Optional(str, max_len=4000)
        id_tramite = Optional(str, max_len=16)
        codorg = Required(int)
        pendiente = Required(
            str,
            max_len=1,
            default='N',
            py_check=lambda val: val in {'S', 'N'},
        )
        id_usuario = Required(str, max_len=8)
        ts_mod = Required(str, max_len=14)
        migrable = Required(
            str,
            max_len=1,
            default='N',
            py_check=lambda val: val in {'S', 'N'},
        )
        extracto2 = Optional(str, max_len=4000)


    class Sala(db.Entity):
        _table_ = ("AGORA", "SALA")

        id_sala = PrimaryKey(int)
        descripcion = Required(str, max_len=512)
        url = Optional(str, max_len=255)
        rutaimagen = Optional(str, max_len=255)

        def __str__(self):
            if self.descripcion.lower().startswith('sala '):
                return f"{self.descripcion} [{self.id_sala}]"
            else:
                return f"Sala {self.descripcion} [{self.id_sala}]"


    class Jornada(db.Entity):
        _table_ = ("AGORA", "JORNADA")

        id_jornada = PrimaryKey(int)
        n_grabaciones = Required(int, default=0)
        id_sesion = Optional(str, max_len=11)
        id_organo = Optional(str, max_len=6)
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
    return db


def open_source_database():
    db = Database()
    db.bind(
        provider='oracle',
        dsn='oracle',
        user='agora',
        password='agora',
    )
    define_entities(db)
    return db


def open_target_database():
    db = Database()
    db.bind(
        provider='oracle',
        dsn='oracle_dev',
        user='agora',
        password='agora',
    )
    define_entities(db)
    return db

