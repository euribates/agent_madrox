#!/usr/bin/env python3

from datetime import date as Date
from datetime import datetime as DateTime
from datetime import timedelta as TimeDelta
import dataclasses
import logging

import dba


logger = logging.getLogger(__name__)


def as_tsmod(dt):
    return f'{dt.year:04d}{dt.month:04d}{dt.day:02d}000000'


def as_list(values: list) -> str:
    return ', '.join(values)


class Model:

    @classmethod
    def to_dict(cls, item, exclude=None):
        exclude = exclude or set([])
        return {
            f.name: getattr(item, f.name)
            for f in dataclasses.fields(cls)
            if f.name not in exclude
            }

    @classmethod
    def from_dict(cls, dict_data):
        _fields = set(cls.field_names())
        _keys = list(dict_data.keys())
        for name in _keys:
            if name not in _fields:
                dict_data.pop(name)
        return cls(**dict_data)

    @classmethod
    def load_instances(cls, db, pk):
        query = f'{cls._primary_key} = :1'
        sql = f'Select * from {cls._table_name} WHERE {query}'
        return dba.get_rows(db, sql, pk, cast=cls.from_dict)

    @classmethod
    def field_names(cls):
        return [_.name for _ in dataclasses.fields(cls)]

    @classmethod
    def insert(cls, dbc, dict_data):
        _names = cls.field_names()
        _values = [dict_data[name] for name in _names]
        _placeholders = [f':{i+1}' for i in range(len(_names))]
        sql = '\n'.join([
            f"Insert into {cls._table_name} (",
            f"    {as_list(_names)}",
            ") values (",
            f"    {as_list(_placeholders)}"
            ")",
            ])
        logger.debug(str(sql))
        logger.debug(repr(_values))
        return dba.execute(dbc, sql, _values)

    @classmethod
    def update(cls, dbc, pk, new_values):
        query = f'{cls._primary_key} = :pk'
        lines = [f'Update {cls._table_name}']
        sep = ' set '
        _values = []
        for i, name in enumerate(new_values, start=1):
            lines.append(sep)
            lines.append(f'{name} = :{i}')
            _values.append(new_values[name])
            sep = ', '
        lines.append(' where ')
        lines.append(query)
        _values.append(pk)
        assert isinstance(lines, list)
        sql = '\n'.join(lines)
        assert isinstance(sql, str)
        logger.info(f'sql: {sql} valuesL {_values}')
        return dba.execute(dbc, sql, _values)

    @classmethod
    def migrar(cls, db_source, db_target, primary_key):
        logger.info(f'Migrar {cls.__name__} :: {primary_key}')
        source_items = cls.load_instances(db_source, primary_key)
        if not source_items:
            return False, f"Unable to load {cls.__name__} [{cls._table_name}] {primary_key}"
        for source_item in source_items:
            logger.info(f'Migrar Dependecias de {cls.__name__}')
            for field_name in cls._depends_on:
                if hasattr(source_item, field_name):
                    Model = cls._depends_on[field_name]
                    foreign_key = getattr(source_item, field_name)
                    for _,_ in Model.migrar(db_source, db_target, foreign_key):
                        pass

            logger.info(f'Migrar registro {cls.__name__} {primary_key}')
            targets = list(cls.load_instances(db_target, primary_key))
            if targets:
                for target_item in targets:
                    logger.info(f' - target_item {target_item}. Maybe needs update?')
                    exclude = {cls._primary_key}
                    new_values = cls.to_dict(source_item, exclude=exclude)
                    old_values = cls.to_dict(target_item, exclude=exclude)
                    if new_values != old_values:  # Update
                        logger.info(' - Yes, update.')
                        diff_values = {}
                        for name in old_values:
                            logger.info(f'{name} { new_values[name]} <-> {old_values[name]}')
                            if new_values[name] != old_values[name]:
                                diff_values[name] = new_values[name]
                        logger.info(f'diff_values: {diff_values!r}')
                        cls.update(db_target, primary_key, diff_values)
                        logger.info(f"- Update {cls.__name__} {primary_key}")
                        yield True, 'Updated'
                        continue
                    logging.info(f"- No, Skipped {cls.__name__} {primary_key}")
                    yield True, 'Skipped'
            else:  # Insert
                values = cls.to_dict(source_item)
                cls.insert(db_target, values)
                logger.info(f"- Insert {cls.__name__} {primary_key}")
                yield True, 'Inserted'


@dataclasses.dataclass
class Proyecto(Model):
    _table_name = "Tareas.Proyecto"
    _primary_key = 'id_proyecto'
    _depends_on = {}

    id_proyecto: int
    nombre: str
    descripcion: str
    f_creacion: DateTime
    f_cierre: DateTime
    status: str

    @classmethod
    def since(cls, dbc, num_days):
        fecha = Date.today() - TimeDelta(days=num_days)
        query = 'f_creacion > :1 OR f_cierre > :1'
        sql = f"Select {cls._primary_key} as pk from {cls._table_name} WHERE {query}"
        return dba.get_rows(dbc, sql, fecha, cast=lambda row: row['pk'])


@dataclasses.dataclass
class Tarea(Model):
    _table_name = "Tareas.Tarea"
    _primary_key = 'id_tarea'
    _depends_on = {
        'id_proyecto': Proyecto,
    }

    id_tarea: int
    titulo: str
    descripcion: str
    prioridad: str
    estado: str
    progreso: int
    f_creacion: Date
    f_ultima_act: Date
    id_usr_solicitante: int
    id_usr_creador: int
    id_usr_asignado: int
    id_servicio: int
    f_visualizacion: Date
    f_cierre: Date
    estimacion: str
    id_proyecto: int

    @classmethod
    def since(cls, dbc, num_days):
        fecha = Date.today() - TimeDelta(days=num_days)
        query = 'f_ultima_act > :1'
        sql = f"Select {cls._primary_key} as pk from {cls._table_name} WHERE {query}"
        return dba.get_rows(dbc, sql, fecha, cast=lambda row: row['pk'])


@dataclasses.dataclass
class Nota(Model):
    _table_name = "Tareas.Nota"
    _primary_key = 'id_nota'
    _depends_on = {
        Tarea,
    }

    id_nota: int
    id_tarea: int
    numero: int
    autor: int
    texto: str
    notificar: str
    fecha: Date
    f_creacion: Date

    @classmethod
    def since(cls, dbc, num_days):
        fecha = Date.today() - TimeDelta(days=num_days)
        query = 'f_creacion > :1 OR fecha > :1'
        sql = f"Select {cls._primary_key} as pk from {cls._table_name} WHERE {query}"
        return dba.get_rows(dbc, sql, fecha, cast=lambda row: row['pk'])


@dataclasses.dataclass
class Isla(Model):
    _table_name = "AGORA.Isla"
    _primary_key = 'id_isla'
    _depends_on = {}

    id_isla: int
    descripcion: str
    ts_mod: str
    migrable: str

    @classmethod
    def since(cls, dbc, num_days):
        fecha = Date.today() - TimeDelta(days=num_days)
        ts_mod = as_tsmod(fecha)
        query = 'ts_mod > :1'
        sql = f"Select {cls._primary_key} as pk from {cls._table_name} WHERE {query}"
        return dba.get_rows(dbc, sql, ts_mod, cast=lambda row: row['pk'])


@dataclasses.dataclass
class Sala(Model):
    _table_name = "AGORA.Sala"
    _primary_key = 'id_sala'
    _depends_on = {}

    id_sala: str
    descripcion: str
    url: str
    rutaimagen: str

    @classmethod
    def since(cls, dbc, num_days):
        sql = f"Select {cls._primary_key} as pk from {cls._table_name}"
        return dba.get_rows(dbc, sql, ts_mod, cast=lambda row: row['pk'])


@dataclasses.dataclass
class Organo(Model):
    _table_name = "AGORA.ORGANO"
    _primary_key = 'id_organo'
    _depends_on = {
        'id_isla': Isla,
    }

    id_organo: str
    legislatura: int
    alias: str
    nombre: str
    direccion: str
    constitucion: Date
    disolucion: Date
    cp: str
    fax: str
    id_isla: int
    municipio: str
    telefono: str
    tipo: int
    id_usuario: str
    ts_mod: str
    tipo_comision: int
    codorg: int
    id_sala_habitual: int
    id_tramite_organo: str
    migrable: str
    url: str
    descripcion: str
    id_letrado: str
    nombre_completo: str

    @classmethod
    def since(cls, dbc, num_days):
        fec = Date.today() - TimeDelta(days=num_days)
        ts_mod = f'{fec.year:04d}{fec.month:02d}{fec.day:02d}000000'
        query = 'ts_mod > :1'
        sql = f"Select {cls._primary_key} as pk from {cls._table_name} WHERE {query}"
        return dba.get_rows(dbc, sql, ts_mod, cast=lambda row: row['pk'])


@dataclasses.dataclass
class Jornada(Model):
    _table_name = "AGORA.Jornada"
    _primary_key = 'id_jornada'
    _depends_on = {
        'id_sala': Sala,
    }

    id_jornada: int
    n_grabaciones: int
    id_sesion: str
    id_organo: str
    fecha: Date
    hora: str
    descripcion: str
    texto: str
    id_sala: int
    canal: int
    hora_fin: str
    id_sesion_seneca: int
    id_sesion_seneca_fx: int
    estado: str
    secreta: str
    hora_reanudacion: str
    emision_para_prensa: str

    @classmethod
    def since(cls, dbc, num_days):
        fecha = Date.today() - TimeDelta(days=num_days)
        query = 'fecha > :1'
        sql = f"Select {cls._primary_key} as pk from {cls._table_name} WHERE {query}"
        return dba.get_rows(dbc, sql, fecha, cast=lambda row: row['pk'])


@dataclasses.dataclass
class SesionDatos(Model):
    _table_name = "AGORA.Sesion_Datos"
    _primary_key = 'id_sesion'
    _depends_on = {}

    id_sesion: str
    convocada: str
    f_convocada: Date
    f_desconvocada: Date
    id_usuario: int
    confirmada: str


@dataclasses.dataclass
class Sesion(Model):
    _table_name = "AGORA.Sesion"
    _primary_key = 'id_sesion'
    _depends_on = {
        'id_organo': Organo,
        'id_sesion': SesionDatos,
        }

    id_sesion: str
    id_organo: str
    fecha: Date
    hora: str
    legislatura: int
    n_sesion: int
    tipo_sesion: str
    descripcion: str
    id_sala: int
    n_asistentes: int
    prensa: str
    ts_mod: str
    id_usuario: str
    n_dias: int
    migrable: str


@dataclasses.dataclass
class Asunto(Model):
    _table_name = "AGORA.Asunto"
    _primary_key = 'id_asunto'
    _depends_on = {
        'id_sesion': SesionDatos,
    }

    id_asunto: str
    id_sesion: str
    legislatura: int
    n_orden_asunto: int
    n_orden_tramite: int
    id_iniciativa: str
    descripcion: str
    n_sesion: int
    punto: int
    subpunto: int
    extracto: str
    id_tramite: str
    codorg: int
    pendiente: str
    id_usuario: str
    ts_mod: str
    migrable: str
    extracto2: str


@dataclasses.dataclass
class Aplicacion(Model):
    _table_name = "Comun.Aplicacion"
    _primary_key = 'id_aplicacion'
    _depends_on = {}

    id_aplicacion: int
    nombre: str
    url: str
    descripcion: str
    alta: DateTime
    icono: str
    codigo: str


@dataclasses.dataclass
class Acceso(Model):
    _table_name = "Comun.Acceso"
    _primary_key = 'id_acceso'
    _depends_on = {
        'id_aplicacion': Aplicacion,
        }

    id_acceso: int
    id_aplicacion: int
    id_usuario: int
    orden: int
    alta: DateTime


@dataclasses.dataclass
class Usuario(Model):
    _table_name = "Comun.Usuario"
    _primary_key = 'id_usuario'
    _depends_on = {
        'id_usuario': Acceso,
        }

    id_usuario: int
    id_servicio: int
    login: str
    nombre: str
    apellido1: str
    apellido2: str
    clase: str
    id_portafirma: int
    id_agora: str
    email: str
    f_alta: Date
    f_baja: Date
    f_mod: Date
    grupo_nt: str
    telefono_contacto: str
    nif: str
    pwd_shadow: str
    movil: str
    notificar_reuniones: str
    notificar_bop: str
    notificar_ds: str
    notificar_dsc: str
    notificar_dsdp: str
    seudonimo: str
    twitter: str
    id_sap: int
    id_mhp: str
    extra: str
    pwd_inicial: str

    @classmethod
    def since(cls, dbc, num_days):
        fecha = Date.today() - TimeDelta(days=num_days)
        query = 'f_mod > :1 OR f_alta > :1'
        sql = f"Select {cls._primary_key} as pk from {cls._table_name} WHERE {query}"
        return dba.get_rows(dbc, sql, fecha, cast=lambda row: row['pk'])


@dataclasses.dataclass
class Noticia(Model):

    _table_name = "Noticias.Noticia"
    _primary_key = 'id_noticia'
    _depends_on = {}

    id_noticia: int
    titulo: str
    url: str
    entradilla: str
    id_servicio: int
    texto: str
    f_alta: Date
    f_baja: Date
    f_pub: Date
    prioritaria: str
    uuid: str
    destacada: str
    streaming_url: str

    @classmethod
    def since(cls, dbc, num_days):
        fecha = Date.today() - TimeDelta(days=num_days)
        query = 'f_alta >= :1'
        sql = f"Select {cls._primary_key} as pk from {cls._table_name} WHERE {query}"
        return [row['pk'] for row in dba.get_rows(dbc, sql, fecha)]


@dataclasses.dataclass
class NoticiaChunk(Model):

    _table_name = "Noticias.Noticia_Chunk"
    _primary_key = ('uuid', 'ordering')
    _depends_on = {'uuid': Noticia}

    uuid: str
    ordering: int
    text: str

