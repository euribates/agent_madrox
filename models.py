#!/usr/bin/env python3.12

from datetime import date as Date
from datetime import datetime as DateTime
from datetime import timedelta as TimeDelta
from typing import Callable
import dataclasses
import logging

from settings import DEFAULT_SINCE_DAYS
import dba

logger = logging.getLogger(__name__)


def as_ts_mod(dt):
    return f'{dt.year:04d}{dt.month:04d}{dt.day:02d}000000'


def as_list(values: list) -> str:
    return ', '.join(values)


def loop(sequence, start=0):
    sequence = list(sequence)
    if sequence:
        last_pos = len(sequence) - 1
        for index, value in enumerate(sequence):
            is_first = bool(index == 0)
            is_last = bool(index == last_pos)
            yield (index+start, is_first, is_last, value)


@dataclasses.dataclass
class PrimaryKey:
    name: str
    cast: Callable


@dataclasses.dataclass
class Model:

    @classmethod
    def _field_names(cls):
        return [_.name for _ in dataclasses.fields(cls)]

    def _to_dict(self, exclude=None):
        exclude = exclude or set([])
        return {
            name: getattr(self, name)
            for name in self._field_names()
            if name not in exclude
            }

    @classmethod
    def _from_dict(cls, dict_data):
        _fields = set(cls._field_names())
        _keys = list(dict_data.keys())
        for name in _keys:
            if name not in _fields:
                dict_data.pop(name)
        return cls(**dict_data)

    @classmethod
    def _locator(cls, pk):
        table_name = cls.Meta.table_name
        pk_field = cls.Meta.primary_key.name
        return f'{table_name}.{pk_field} = {pk!r}'

    @classmethod
    def _load_instance(cls, db, pk):
        table_name = cls.Meta.table_name
        names = as_list(cls._field_names())
        query = f'{cls.Meta.primary_key.name} = :1'
        sql = f'SELECT {names} FROM {table_name} WHERE {query}'
        return dba.get_row(db, sql, pk, cast=cls._from_dict)

    @classmethod
    def _load_instances(cls, db, field_name, value):
        table_name = cls.Meta.table_name
        names = as_list(cls._field_names())
        query = f'{field_name} = :1'
        sql = f'SELECT {names} FROM {table_name} WHERE {query}'
        return dba.get_rows(db, sql, value, cast=cls._from_dict)

    @classmethod
    def _insert(cls, dbc, data: dict):
        names = cls._field_names()
        values = [data[name] for name in names]
        placeholders = [f':{i+1}' for i in range(len(names))]
        sql = (
            f'INSERT INTO {cls.Meta.table_name} ({as_list(names)})'
            f'     VALUES ({as_list(placeholders)})'
            )
        return dba.execute(dbc, sql, *values)

    @classmethod
    def _update(cls, dbc, pk, new_values):
        lines = [f'UPDATE {cls.Meta.table_name}']
        values = []
        for idx, is_first, is_last, name in loop(new_values, start=1):
            before = '  SET' if is_first else '     '
            after = '' if is_last else ','
            lines.append(f'{before} {name} = :{idx}{after}')
            values.append(new_values[name])
        lines.append(f' WHERE {cls.Meta.primary_key.name} = :{idx+1}')
        values.append(pk)
        sql = '\n'.join(lines)
        return dba.execute(dbc, sql, *values)

    @classmethod
    def _keys_since(cls, source, query, num_days=DEFAULT_SINCE_DAYS, cast=None):
        fecha = Date.today() - TimeDelta(days=num_days)
        if cast:
            fecha = cast(fecha)
        sql = (
            f'SELECT {cls.Meta.primary_key.name } as pk'
            f'  FROM {cls.Meta.table_name}'
            f' WHERE {query}'
            )
        return dba.get_rows(
            source, sql, fecha,
            cast=lambda row: row['pk'],
            )

    @classmethod
    def _is_migrable(cls):
        return hasattr(cls, '_since') and callable(cls._since)


class Catalog:

    def __init__(self):
        self.kernel = {}

    def register(self, model):
        name = model.__name__.lower()
        self.kernel[name] = model
        for field_name, _model in model.Meta.depends_on.items():
            assert issubclass(_model, Model)
            assert isinstance(field_name, str)
            assert field_name in model._field_names(), (
                f'Campo {field_name} no definido en {_model!r}.'
                f' Los posibles valores son: {as_list(_model._field_names())}.'
                )
        for _model in model.Meta.master_of:
            assert issubclass(_model, Model)
        return model

    def __getitem__(self, key):
        return self.kernel[key.lower()]

    def keys(self):
        yield from self.kernel.keys()

    def items(self):
        yield from self.kernel.items()


catalog = Catalog()


@catalog.register
@dataclasses.dataclass
class Legislatura(Model):

    class Meta:
        table_name = "Agora.Legislatura"
        primary_key = PrimaryKey('legislatura', int)
        depends_on = {}
        master_of = set([])

    legislatura: int
    descripcion: str
    f_elecciones: Date
    f_inicio: Date
    f_final: Date
    anio: str

    @classmethod
    def _since(cls, dbc, num_days=DEFAULT_SINCE_DAYS):
        return cls._keys_since(
            source=dbc,
            query='f_inicio >= :1',
            num_days=num_days,
            )


@catalog.register
@dataclasses.dataclass
class Usuario(Model):

    class Meta:

        table_name = "Comun.Usuario"
        primary_key = PrimaryKey('id_usuario', int)
        depends_on = {}
        master_of = set([])

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
    def _since(cls, dbc, num_days=DEFAULT_SINCE_DAYS):
        return cls._keys_since(
            source=dbc,
            query='f_mod > :1 OR f_alta > :1',
            num_days=num_days,
            )


@catalog.register
@dataclasses.dataclass
class Proyecto(Model):

    class Meta:
        table_name = "Tareas.Proyecto"
        primary_key = PrimaryKey('id_proyecto', int)
        depends_on = {}
        master_of = {}

    id_proyecto: int
    nombre: str
    descripcion: str
    f_creacion: DateTime
    f_cierre: DateTime
    status: str

    @classmethod
    def _since(cls, dbc, num_days=DEFAULT_SINCE_DAYS):
        return cls._keys_since(
            source=dbc,
            query='f_creacion > :1 OR f_cierre > :1',
            num_days=num_days,
            )


@catalog.register
@dataclasses.dataclass
class Nota(Model):

    class Meta:
        table_name = "Tareas.Nota"
        primary_key = PrimaryKey('id_nota', int)
        depends_on = {}
        master_of = set([])

    id_nota: int
    id_tarea: int
    numero: int
    autor: int
    texto: str
    notificar: str
    f_modificacion: Date
    f_creacion: Date

    @classmethod
    def _since(cls, dbc, num_days=DEFAULT_SINCE_DAYS):
        return cls._keys_since(
            source=dbc,
            query='f_creacion > :1 OR f_modificacion > :1',
            num_days=num_days,
            )


@catalog.register
@dataclasses.dataclass
class Tarea(Model):

    class Meta:
        table_name = "Tareas.Tarea"
        primary_key = PrimaryKey('id_tarea', int)
        depends_on = {
            'id_proyecto': Proyecto,
            'id_usr_solicitante': Usuario,
            }
        master_of = {Nota}

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
    def _since(cls, dbc, num_days=DEFAULT_SINCE_DAYS):
        return cls._keys_since(
            source=dbc,
            query='f_ultima_act >= :1',
            num_days=num_days,
            )

@catalog.register
@dataclasses.dataclass
class Isla(Model):

    class Meta:
        table_name = "AGORA.Isla"
        primary_key = PrimaryKey('id_isla', int)
        depends_on = {}
        master_of = set([])

    id_isla: int
    descripcion: str
    ts_mod: str
    migrable: str

    @classmethod
    def _since(cls, dbc, num_days=DEFAULT_SINCE_DAYS):
        return cls._keys_since(
            source=dbc,
            query='ts_mod >= :1',
            num_days=num_days,
            cast=as_ts_mod,
            )


@catalog.register
@dataclasses.dataclass
class Sala(Model):

    class Meta:
        table_name = "AGORA.Sala"
        primary_key = PrimaryKey('id_sala', int)
        depends_on = {}
        master_of = set([])

    id_sala: int
    descripcion: str
    slug: str
    f_baja: Date
    rutaimagen: str
    web: str
    updated_at: DateTime

    @classmethod
    def _since(cls, dbc, num_days=DEFAULT_SINCE_DAYS):
        fecha = DateTime.now() - TimeDelta(days=num_days)
        sql = (
            f'Select {cls.Meta.primary_key.name} as pk'
            f'  from {cls.Meta.table_name}'
            '  WHERE updated_at > :1'
            )
        return dba.get_rows(
            dbc, sql, fecha,
            cast=lambda d: cls.Meta.primary_key.cast(d['pk']),
            )


@catalog.register
@dataclasses.dataclass
class Organo(Model):

    class Meta:
        table_name = "AGORA.ORGANO"
        primary_key = PrimaryKey('id_organo', str)
        depends_on = {
            'id_isla': Isla,
            'legislatura': Legislatura,
            }
        master_of = set([])

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
    def _since(cls, dbc, num_days=DEFAULT_SINCE_DAYS):
        return cls._keys_since(
            source=dbc,
            query='ts_mod >= :1',
            num_days=num_days,
            cast=as_ts_mod,
            )



@catalog.register
@dataclasses.dataclass
class SesionDatos(Model):

    class Meta:
        table_name = "AGORA.Sesion_Datos"
        primary_key = PrimaryKey('id_sesion', str)
        depends_on = {}
        master_of = set([])

    id_sesion: str
    convocada: str
    f_convocada: Date
    f_desconvocada: Date
    id_usuario: int
    confirmada: str

    @classmethod
    def _since(cls, dbc, num_days=DEFAULT_SINCE_DAYS):
        return cls._keys_since(
            source=dbc,
            query='f_convocada >= :1 OR f_desconvocada >= :1',
            num_days=num_days,
            )


@catalog.register
@dataclasses.dataclass
class Asunto(Model):

    class Meta:
        table_name = "AGORA.Asunto"
        primary_key = PrimaryKey('id_asunto', str)
        depends_on = {
            'legislatura': Legislatura,
            }
        master_of = set([])

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



@catalog.register
@dataclasses.dataclass
class Sesion(Model):
    class Meta:
        table_name = "AGORA.Sesion"
        primary_key = PrimaryKey('id_sesion', str)
        depends_on = {
            'id_organo': Organo,
            }
        master_of = set([SesionDatos, Asunto])

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

    @classmethod
    def _since(cls, dbc, num_days=DEFAULT_SINCE_DAYS):
        return cls._keys_since(
            source=dbc,
            query='fecha >= :1',
            num_days=num_days,
            )



@catalog.register
@dataclasses.dataclass
class Jornada(Model):

    class Meta:
        table_name = "AGORA.Jornada"
        primary_key = PrimaryKey('id_jornada', int)
        depends_on = {
            'id_sala': Sala,
            'id_organo': Organo,
            'id_sesion': Sesion,
            }
        master_of = set([])

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
    def _since(cls, dbc, num_days=DEFAULT_SINCE_DAYS):
        return cls._keys_since(
            source=dbc,
            query='fecha > :1',
            num_days=num_days,
            )


@catalog.register
@dataclasses.dataclass
class Acceso(Model):

    class Meta:
        table_name = "Comun.Acceso"
        primary_key = PrimaryKey('id_acceso', int)
        depends_on = {
            'id_usuario': Usuario,
            }
        master_of = set([])

    id_acceso: int
    id_aplicacion: int
    id_usuario: int
    alta: DateTime

    @classmethod
    def _since(cls, dbc, num_days=DEFAULT_SINCE_DAYS):
        return cls._keys_since(
            source=dbc,
            query='alta > :1',
            num_days=num_days,
            )


@catalog.register
@dataclasses.dataclass
class Aplicacion(Model):

    class Meta:
        table_name = "Comun.Aplicacion"
        primary_key = PrimaryKey('id_aplicacion', int)
        depends_on = {}
        master_of = set([Acceso])

    id_aplicacion: int
    nombre: str
    url: str
    descripcion: str
    alta: DateTime
    icono: str
    codigo: str

    @classmethod
    def _since(cls, dbc, num_days=DEFAULT_SINCE_DAYS):
        return cls._keys_since(
            source=dbc,
            query='alta > :1',
            num_days=num_days,
            )


@catalog.register
@dataclasses.dataclass
class Parrafo(Model):

    class Meta:
        table_name = "Noticias.Parrafo"
        primary_key = PrimaryKey('id_parrafo', int)
        depends_on = {}
        master_of = set([])

    id_parrafo: int
    id_noticia: int
    orden: int
    texto: str



@catalog.register
@dataclasses.dataclass
class Noticia(Model):

    class Meta:
        table_name = "Noticias.Noticia"
        primary_key = PrimaryKey('id_noticia', int)
        depends_on = {}
        master_of = set([Parrafo])

    id_noticia: int
    titulo: str
    url: str
    entradilla: str
    texto: str
    f_alta: Date
    f_baja: Date
    f_pub: Date
    prioritaria: str
    uuid: str
    destacada: str
    streaming_url: str

    @classmethod
    def _since(cls, dbc, num_days):
        return cls._keys_since(
            source=dbc,
            query='f_alta >= :1',
            num_days=num_days,
            )


# Publicaciones


@catalog.register
@dataclasses.dataclass
class BOP(Model):

    class Meta:
        table_name = "Agora.BOP"
        primary_key = PrimaryKey('id_bop', int)
        depends_on = {
            'legislatura': Legislatura,
            }
        master_of = set([])

    id_bop: int
    n_bop: int
    f_publicacion: Date
    pdf_ruta: str
    legislatura: int
    publicar: str
    f_notifica: Date
    num_fasciculos: int
    n_paginas: int
    pdf_filesize: int
    ts_mod: str

    @classmethod
    def _since(cls, dbc, num_days):
        return cls._keys_since(
            source=dbc,
            query='f_publicacion >= :1',
            num_days=num_days,
            )


# Diarios de sesiones del PArlamento

@dataclasses.dataclass
class DS_Sumario(Model):

    class Meta:
        table_name = "Agora.DS_SUMARIO"
        primary_key = PrimaryKey('id_ds', int)
        depends_on = {}
        master_of = set([])

    id_ds: int
    orden: int
    pagina: int
    texto: str
    id_iniciativa: str
    aplazada: str


@catalog.register
@dataclasses.dataclass
class DS(Model):

    class Meta:
        table_name = "Agora.DS"
        primary_key = PrimaryKey('id_ds', int)
        depends_on = {
            'legislatura': Legislatura,
            }
        master_of = set([DS_Sumario])

    id_ds: int
    n_ds: int
    legislatura: int
    f_publicacion: Date
    publicar: str
    f_notifica: Date
    comentario: str
    n_paginas: int
    id_sesion: str
    inicio_sesion: str
    fin_sesion: str
    pdf_filesize: int
    pdf_ruta: str
    ts_mod: str

    @classmethod
    def _since(cls, dbc, num_days):
        return cls._keys_since(
            source=dbc,
            query='f_publicacion >= :1',
            num_days=num_days,
            )


