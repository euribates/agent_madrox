#!/usr/bin/env python
# -*- coding: utf-8 -*-


import sys


def has_ansi_marks(s):
    return '\u001b' in s


def red(msg):
    """Devuelve el texto en color rojo (Para terminal/consola ANSI)."""
    return f"\u001b[31m{msg}\u001b[0m"


def green(msg):
    """Devuelve el texto en color verde (Para terminal/consola ANSI)."""
    return f"\u001b[32m{msg}\u001b[0m"


def yellow(msg):
    """Devuelve el texto en color amarillo (Para terminal/consola ANSI)."""
    return f"\u001b[33m{msg}\u001b[0m"


ERROR = red("[✖]")

OK = green("[✓]")

WAITING = yellow("[⧗]")

FIXED = yellow("[⚒]")

SKIP = yellow("[⤼]")


class Tabula:
    """Imprimir tablas de forma sencilla.

    Ejemplo de uso:

    >>> with Tabula(a=-5, b=18, c=-12) as tab:
    ...     tab(1, 'Primera linea', 31234.23)
    ...     tab(2, 'Segunda linea', 31234.23)
        a b                             c
    ----- ------------------ ------------
        1 Primera linea          31234.23
        2 Segunda linea          31234.23
    ----- ------------------ ------------

    Si no queremos usar la salida estandar, podemos pasar en el constructor
    usando el parametro `_stdout` cualquier objeto que tenga un método `write`
    """

    def __init__(self, **kwargs):
        self.stream = kwargs.pop('_stdout', sys.stdout)
        self.cols = kwargs.copy()
        self.formaters = {
            name: f'{{{name}:>{-size}}}' if self.cols[name] < 0 else f'{{{name}:{size}}}'
            for name, size in self.cols.items()
        }

    def headers(self):
        buff = []
        for name in self.formaters:
            fmt = self.formaters[name]
            buff.append(fmt.format(**{name: name}))
        return ' '.join(buff)

    def separator(self):
        buff = []
        for name in self.cols:
            size = abs(self.cols[name])
            buff.append('-' * size)
        return ' '.join(buff)

    def row(self, *args):
        assert len(args) == len(self.cols)
        buff = []
        for name, value in zip(self.formaters, args):
            fmt = self.formaters[name]
            value = str(value)
            if not has_ansi_marks(value):
                size = abs(self.cols[name])
                value = value[0:size]
            buff.append(fmt.format(**{name: str(value)}))
        return ' '.join(buff)

    def new_line(self, line):
        self.stream.write(f'{line}\n')

    def __call__(self, *args):
        self.new_line(self.row(*args))

    def __enter__(self):
        self.new_line(self.headers())
        self.new_line(self.separator())
        return self

    def __exit__(self, ex_type, ex_value, ex_traceback):
        self.new_line(self.separator())
