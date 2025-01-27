#!/usr/bin/env python3

import datetime
import decimal
import re

import arrow
#  Expresiones regulares para parsear las partes de las sentencias

pat_table_as_alias = re.compile(r"(.+)\s+as\s+(\.+)", flags=re.I)


# Funcionaes auxiliares


def forloop(sequence, start=0):
    sequence = list(sequence)
    if sequence:
        last_pos = len(sequence) - 1
        for index, value in enumerate(sequence):
            is_first = bool(index == 0)
            is_last = bool(index == last_pos)
            yield (index+start, is_first, is_last, value)


def sql_safe_string(s):
    if isinstance(s, bytes):
        s = s.rstrip()
        s = s.replace(b"\r\n", b"\n")
        # replace cp1252 LEFT/RIGHT DOUBLE QUOTATION MARK
        s = s.replace(b"\x93", b'"')
        s = s.replace(b"\x94", b'"')
        s = s.replace(b"'", b"''")
    elif isinstance(s, str):
        s = s.rstrip()
        s = s.replace("\r\n", "\n")
        s = s.replace("'", "''")
    return s


def sep_comma(items):
    return ", ".join(items)


class Field:
    """
    DESCRIPCIÓN
    ===========

    Clase base para la descripción de los campos de una tabla.

    Las responsabilidades de cada objeto L{Field} son las siguientes:

        - Contener el valor asignado al campo, o C{None}
          si en la base de datos está a C{NULL} (Atributo C{Value}).

        - Ser capaz de representar dicho valor de una forma apta para
          sentencias C{SQL} (Método L{__str__} y métodos derivados
          L{as_sql}.)

        - Ser capaz de interpretar una representación textual del valor
          (método L{parse}), y reconstruir el valor a partir de la misma.
          Los tipos derivados de C{Field}, como L{FieldInteger} o
          L{FieldDate} normalmente también sobreescriben
          este método.
    """

    def __init__(self, value=None):
        """Constructor.

        Normalmente no se usa el constructor a pelo, sino el método
        factoría L{new_field}.

        @param value: Valor del campo. A partir del valor se interpreta
                    el tipo de dato.
        """
        self.value = value

    def parse(self, s):
        """
        Interpreta una string intentando obtener el valor
        correspondiente. La implementación por defecto sólo
        es válida para el tipo string, las demas clases derivadas
        tendran que sobreescribirla.

        @param s: string que representa el valor
        @attention: Nomrmalmente, se sobreescribe en las clases derivadas.
        """
        self.value = str(s)

    def __str__(self):
        """
        Representación textual.

        Devuelve una representación textual apta para una operación SQL.
        Depende del método virtual L{as_sql}, por lo que todas las
        clases derivadas deben sobreescribir L{as_sql}.

        @return: Representación textual apta para una operación SQL.
        @rtype: string
        @see: L{as_sql}.
        """
        if self.value is None:
            return "NULL"
        else:
            return self.as_sql()

    def as_sql(self):
        """
        Representación textual apta para una operación SQL.

        Ejemplo:

            >>> import pytest
            >>> f = Field(23)
            >>> with pytest.raises(NotImplementedError):
            ...     f.as_sql()

        @return: Representación textual apta para una operación SQL.
        @rtype:  string
        @see: L{__str__}
        @attention: Método virtual. Debe ser implementeado por
                    B{todas} las chases derivadas. Si se intenta
                    invocar directamente desde un objeto tipo
                    L{Field} puro, elevará una excepción

        @raise NotImplementedError: Si se intenta invocar desde
                                    la clase base.
        """
        raise NotImplementedError(
            "La clase Field es una clase base.\n"
            "Se ha intentado invocar as_sql, un método abstracto."
        )


class Null(Field):

    def as_sql(self):
        return "NULL"

    def parse(self, s):
        return None


class FieldInteger(Field):
    def as_sql(self):
        return "{:d}".format(self.value)

    def parse(self, s):
        self.value = int(s)


def as_integer(value):
    return FieldInteger(value).as_sql() if value is not None else 'NULL'


class FieldDecimal(Field):
    def as_sql(self):
        return str(self.value)

    def parse(self, s):
        self.value = decimal.Decimal(s)


def as_decimal(value):
    return FieldDecimal(value).as_sql() if value is not None else 'NULL'


class FieldFloat(Field):
    def as_sql(self):
        return "{:f}".format(self.value)

    def parse(self, f):
        self.value = float(f)


def as_float(value):
    return FieldFloat(value).as_sql() if value is not None else 'NULL'


class FieldString(Field):

    def as_sql(self):
        """
        Representación textual apta para una operación SQL.

        Ejemplos:

            >>> assert FieldString('PEPE').as_sql() == "'PEPE'"
            >>> assert FieldString(None).as_sql() == 'NULL'

        @return: Representación textual apta para una operación SQL.
        @rtype:  string
        @see: L{Field.__str__}
        """
        if self.value is None:
            return 'NULL'
        if len(self.value) > 0:
            return "'%s'" % sql_safe_string(self.value)
        else:
            return "''"

    def parse(self, s):
        self.value = str(s)


def as_string(value):
    return FieldString(value).as_sql() if value is not None else 'NULL'


class FieldDate(Field):

    def as_sql(self):
        f = self.value
        return f"TO_DATE('{f.year:04d}-{f.month:02d}-{f.day:02d}', 'YYYY-MM-DD')"

    def parse(self, s):
        self.value = arrow.get(s)


class FieldTimestamp(Field):
    def as_sql(self):
        f = self.value
        return (
            "TO_DATE("
            f"'{f.year:04d}-{f.month:02d}-{f.day:02d}"
            f" {f.hour:02d}:{f.minute:02d}:{f.second:02d}"
            "', 'YYYY-MM-DD HH24:MI:SS')"
            )

    def parse(self, s):
        self.value = arrow.get(s)


class FieldBoolean(Field):
    def as_sql(self):
        return "1" if self.value else "0"

    def parse(self, s):
        self.value = bool(s)


def as_boolean(value):
    return FieldBoolean(value).as_sql() if value is not None else 'NULL'


def new_field(value):
    """
    Factoría para la construcción de nuevos campos.

    Se utiliza esta función para crear un nuevo objeto, derivado
    del tipo L{Field}, adecuado al tipo de dato que se pasa como parámetro.
    por ejemplo, si se pasa un entero, la función retornará un
    objeto de tipo L{FIELD_INTEGER}.

    Ejemplo de uso:

        >>> f = new_field(23)
        >>> assert f.value == 23
        >>> f.parse('243')
        >>> assert f.value == 243

    @param value: Valor a partir del cual se creará el objeto L{Field}.
    @see: L{Field}
    """
    if value is None:
        return Null()
    tipo = type(value)
    if tipo is int:
        return FieldInteger(value)
    elif tipo is float:
        return FieldFloat(value)
    elif isinstance(value, decimal.Decimal):
        return FieldDecimal(value)
    elif tipo is str or tipo is bytes:
        return FieldString(value)
    elif tipo == datetime.date:
        return FieldDate(value)
    elif tipo in (datetime.datetime, arrow.Arrow):
        return FieldTimestamp(value)
    elif tipo == bool:
        return FieldBoolean(value)
    else:
        raise ValueError("No entiendo los datos de tipo {}".format(repr(tipo)))


def predicate_isnull(fn, v):
    if v:
        return "%s IS NULL" % fn
    else:
        return "%s IS NOT NULL" % fn


def predicate_isnotnull(fn, v):
    if v:
        return "%s IS NOT NULL" % fn
    else:
        return "%s IS NULL" % fn


def predicate_between(fn, v):
    (minimo, maximo) = v
    return "%s BETWEEN %s AND %s" % (fn, new_field(minimo), new_field(maximo))


_Mapa_Operadores = {
    "gt": lambda fn, v: "%s > %s" % (fn, new_field(v)),
    "gte": lambda fn, v: "%s >= %s" % (fn, new_field(v)),
    "lt": lambda fn, v: "%s < %s" % (fn, new_field(v)),
    "lte": lambda fn, v: "%s <= %s" % (fn, new_field(v)),
    "eq": lambda fn, v: "%s = %s" % (fn, new_field(v)),
    "noteq": lambda fn, v: "%s <> %s" % (fn, new_field(v)),
    "contains": lambda fn, v: "{} LIKE '%%{}%%'".format(fn, sql_safe_string(v)),
    "icontains": lambda fn, v: "UPPER({}) LIKE UPPER('%%{}%%')".format(
        fn, sql_safe_string(v)
    ),
    "startswith": lambda fn, v: "{} LIKE '{}%%'".format(fn, sql_safe_string(v)),
    "istartswith": lambda fn, v: "UPPER({}) LIKE UPPER('{}%%')".format(
        fn, sql_safe_string(v)
    ),
    "endswith": lambda fn, v: "{} LIKE '%%{}'".format(fn, sql_safe_string(v)),
    "notendswith": lambda fn, v: "{} NOT LIKE '%%{}'".format(fn, sql_safe_string(v)),
    "iendsswith": lambda fn, v: "UPPER({}) LIKE UPPER('%%{}')".format(
        fn, sql_safe_string(v)
    ),
    "isnull": predicate_isnull,
    "isnotnull": predicate_isnotnull,
    "year": lambda fn, v: "extract(year from %s) = %s" % (fn, new_field(v)),
    "month": lambda fn, v: "extract(month from %s) = %s" % (fn, new_field(v)),
    "between": predicate_between,
}


def get_predicate(field__op, value):
    if "__" in field__op:
        (field_name, op_name) = field__op.split("__", 1)
    else:
        field_name = field__op
        op_name = "eq"
    functor = _Mapa_Operadores.get(op_name)
    return functor(field_name, value)


class Select:
    """
    DESCRIPCIÓN
    ===========

    El objetivo de esta clase es poder escribir
    sentencias Select SQL, de forma sencilla. Permite ir componiendo la
    sentencia mediante distintas llamadas.

    Es especialmente apropiada para ser usada cuando tenemos un número
    variable de condiciones de búsqueda, ya que podemos ir añadido
    las condiciones mediante llamadas al método L{And}, sin preocuparnos
    de llamar antes al Where

    Ejemplo de Uso:

        >>> print(
        ...     Select('Codigo, Descripcion')
        ...      .From('Isla')
        ...     .Where('Codigo Between 2 and 5')
        ... )
        SELECT Codigo, Descripcion
          FROM Isla
         WHERE Codigo Between 2 and 5
    """

    def __init__(self, fields=""):
        """Constructor

        Constructor de la clase Select.

        @param fields: Los nombres de los campos que se quieren obtener
        @type fields: string
        """
        if not fields:
            self._fields = []
        elif "," in fields:
            self._fields = [s.strip() for s in fields.split(",")]
        else:
            self._fields = [fields]
        self._from = []
        self._joins = []
        self._where = []
        self._group_by = None
        self._order_by = None
        self._limit = None

    def add_field(self, field):
        if "," in field:
            self._fields.extend([s.strip() for s in field.split(",")])
        else:
            self._fields.append(field)
        return self

    def _get_tabla_and_alias(self, tabla):
        m = pat_table_as_alias.match(tabla)
        if m:
            tabla, alias = m.groups()
        else:
            alias = tabla
        return (tabla, alias)

    def From(self, tabla):
        (tabla, alias) = self._get_tabla_and_alias(tabla)
        self._from.append((tabla, alias))
        return self

    def LeftJoin(self, tabla, condicion):
        """Añadir un left join a la consulta.

        Ejemplo de uso:

            >>> print(
            ...     Select('U.Nombre, U.Apellido1, U.Apellido2, S.Nombre')
            ...       .From('Comun.Usuario U')
            ...       .LeftJoin('Comun.Servicio S', 'U.Id_Servicio = S.Id_Servicio')
            ... )
            SELECT U.Nombre, U.Apellido1, U.Apellido2, S.Nombre
              FROM Comun.Usuario U
              LEFT JOIN Comun.Servicio S ON U.Id_Servicio = S.Id_Servicio

        @see: L{Join}
        @param tabla: Nombres de la tabla con la que se va a hacer el join
        @type tabla: string
        @param condicion:Condición de seleccion del Join
        @type condicion: string
        """
        (tabla, alias) = self._get_tabla_and_alias(tabla)
        self._joins.append(("LEFT JOIN", tabla, alias, condicion))
        return self

    def Join(self, tabla, condicion):
        """Añadir un inner join a la consulta.

        Ejemplo de uso:

            >>> print(
            ...     Select('U.Nombre, U.Apellido1, U.Apellido2, S.Nombre')
            ...      .From('Comun.Usuario U')
            ...      .Join('Comun.Servicio S', 'U.Id_Servicio = S.Id_Servicio')
            ... )
            SELECT U.Nombre, U.Apellido1, U.Apellido2, S.Nombre
              FROM Comun.Usuario U
              JOIN Comun.Servicio S ON U.Id_Servicio = S.Id_Servicio

        @see: L{LeftJoin}
        @param tabla: Nombres de la tabla con la que se va a hacer el join
        @type tabla: string
        @param condicion:Condición de seleccion del Join
        @type condicion: string
        """
        (tabla, alias) = self._get_tabla_and_alias(tabla)
        self._joins.append(("JOIN", tabla, alias, condicion))
        return self

    def Filter(self, **kwargs):
        kwargs.pop("tron", False)
        for k in kwargs:
            self.And(get_predicate(k, kwargs[k]))
        return self

    def Where(self, condicion):
        """Añadir una condición WHERE a la consulta.

        Ejemplo de Uso:

            >>> print(
            ...     Select('Id_Servicio, Nombre')
            ...      .From('Comun.Servicio')
            ...     .Where('Id_Servicio = 25')
            ... )
            SELECT Id_Servicio, Nombre
              FROM Comun.Servicio
             WHERE Id_Servicio = 25

        @see: L{And}, L{Or}
        @param condicion:Condición de seleccion
        @type condicion: string
        """
        self._where.append(condicion)
        return self

    def And(self, condicion):
        """Añadir una condición con el conector AND a la consulta.

        Ejemplo de Uso:

            >>> print(
            ...     Select('Id_Servicio, Nombre')
            ...     .From('Comun.Servicio')
            ...     .Where('Id_Servicio >= 20')
            ...     .And('Id_Servicio <= 25')
            ... )
            SELECT Id_Servicio, Nombre
              FROM Comun.Servicio
             WHERE Id_Servicio >= 20
               AND Id_Servicio <= 25

        @param condicion: Condición de seleccion
        @type condicion: string
        @see: L{Where}, L{Or}
        """
        if self._where:
            self._where.append("AND")
        self._where.append(condicion)
        return self

    def Or(self, condition):
        self._where.append("OR")
        self._where.append(condition)
        return self

    def GroupBy(self, agrupacion):
        self._group_by = agrupacion
        return self

    def OrderBy(self, ordenacion):
        self._order_by = ordenacion
        return self

    def __str__(self):
        """Retorna la sentencia SQL en forma de string.

        Retorna la sentencia SQL conpuesta con las funciones L{From},
        L{Join}, L{LeftJoin}, L{Where}, L{And}, L{Or} y L{OrderBy}. Vease
        cualquiera de estas funciones para ver un ejemplo de __str__.

        @return: La sentencia SQL construida.
        @rtype: string
        """
        if self._fields:
            buff = ["SELECT {}".format(sep_comma(self._fields))]
        else:
            buff = ["SELECT *"]
        list_of_tables = [
            "{} AS {}".format(table, alias) if table != alias else table
            for table, alias in self._from
        ]
        buff.append("  FROM {}".format(sep_comma(list_of_tables)))
        # Joins
        for join_type, table, alias, cond in self._joins:
            buff.append(
                "  {} {} ON {}".format(
                    join_type,
                    "{} AS {}".format(table, alias) if table != alias else table,
                    cond,
                )
            )
        if self._where:
            first_cond = self._where.pop(0)
            buff.append(" WHERE {}".format(first_cond))
            while self._where:
                op_ = self._where.pop(0)
                cond = self._where.pop(0)
                buff.append("   {} {}".format(op_, cond))
        if self._group_by:
            buff.append(" GROUP BY %s" % self._group_by)
        if self._order_by:
            buff.append(" ORDER BY %s" % self._order_by)
        if self._limit:
            buff.append(") WHERE ROWNUM <= %d" % self._Limit)
        return "\n".join(buff)


# --[ Insert ]---------------------------------------------------------


class Insert:
    """
    El objetivo de esta clase es poder escribir
    sentencias Insert SQL, de forma sencilla. Permite ir componiendo la
    sentencia mediante distintas llamadas.
    """

    def __init__(self, tabla):
        """Constructor"""
        self.tabla = tabla
        self._fields = []
        self._values = {}

    def Set(self, nombre, valor):
        """Agregar asignaciones de valores."""
        nombre = nombre.upper()
        if nombre not in self._fields:
            self._fields.append(nombre)
        if str(valor) == '%s':
            self._values[nombre] = '%s'
        else:
            self._values[nombre] = new_field(valor)
        return self

    def get(self, nombre):
        nombre = nombre.upper()
        if nombre in self._fields:
            return self._values[nombre].value
        else:
            raise KeyError('Field not found')

    def SetLiteral(self, nombre, valor):
        nombre = nombre.upper()
        if nombre not in self._fields:
            self._fields.append(nombre)
        self._values[nombre] = str(valor)
        return self

    set_literal = SetLiteral

    def join_and_format(self, values=[], sep=', '):
        if values:
            return sep.join([str(x) for x in values])
        return ''

    def __str__(self):
        """Retorna la sentencia INSERT en forma de string.

        @return: La sentencia SQL construida.
        @rtype: string
        """
        buff = [
            f'INSERT INTO {self.tabla} '
            f'({self.join_and_format(self._fields)})'
            ]
        values = [self._values[k] for k in self._fields]
        buff.append(' VALUES (%s)' % self.join_and_format(values))
        return '\n'.join(buff)


# --[ Update ]---------------------------------------------------------


class Update:
    """
    El objetivo de esta clase es poder escribir
    sentencias Update SQL de forma sencilla. Permite ir
    componiendo la sentencia mediante distintas llamadas.

        Ejemplo de Uso:

        >>> from Libreria.Base.Database import Update
        >>> sql = Update('Isla')
        >>> sql = sql.Set('Descripcion', 'San Borondon')
        >>> sql = sql.Where('Isla = 9')
        >>> print(sql)
        UPDATE Isla
           SET Descripcion = 'San Borondon'
         WHERE Isla = 9

    """

    def __init__(self, tabla):
        """Constructor"""
        self.tabla = tabla
        self._fields = []
        self._values = {}
        self._where = []

    def Set(self, nombre, valor):
        """Agregar asignaciones de valores."""
        nombre = nombre.upper()
        if nombre not in self._fields:
            self._fields.append(nombre)
        self._values[nombre] = new_field(valor)
        return self

    def SetLiteral(self, nombre, valor):
        nombre = nombre.upper()
        if nombre not in self._fields:
            self._fields.append(nombre)
        self._values[nombre] = str(valor)
        return self

    def SetExpr(self, nombre, expr):
        nombre = nombre.upper()
        if nombre not in self._fields:
            self._fields.append(nombre)
        self._values[nombre] = expr
        return self

    def Where(self, condicion):
        """Añadir una condición WHERE a la consulta.

        @param condicion:Condición de seleccion
        @type condicion: string
        """
        if condicion not in self._where:
            self._where.append(condicion)
        return self

    def And(self, condicion):
        """Añadir una condición con el conector AND a la consulta.

        @param condicion: Condición de seleccion
        @type condicion: string
        @see: L{Where}, L{Or}
        """
        self._where.append(condicion)
        return self

    def __str__(self):
        """Retorna la sentencia UPDATE en forma de string.

        @return: La sentencia SQL construida.
        @rtype: string
        """
        if not self._where:
            raise ValueError(
                'No se acepta una sentencia Update'
                ' que afecte a toda la tabla, tiene que'
                ' incluir al menos una condicion'
                )
        if not self._values:
            raise ValueError(
                'No se ha especificado una clausula'
                ' Update Válida. Falta los valores'
                ' de los campos'
                )
        buff = [f'UPDATE {self.tabla}']
        for (index, is_first, is_last, nombre) in forloop(self._fields):
            value = self._values[nombre]
            sep = '' if is_last else ','
            prefix = '   SET' if is_first else '      '
            buff.append(f'{prefix} {nombre} = {value}{sep}')
        first, *rest = self._where
        buff.append(f' WHERE {first}')
        for cond in rest:
            buff.append('   AND {cond}')
        return '\n'.join(buff)
