#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dataclasses import dataclass

import pytest

import dba


# --[ listify function ]-----------------------------------------------


def test_listify_tupla():
    assert dba.listify((1, 2, 'b')) == [1, 2, 'b']


def test_listify_list():
    assert dba.listify([2, 3, 'foo']) == [2, 3, 'foo']


def test_listify_dictionary():
    assert dba.listify({'uno': 1, 'dos': 2}) == [
        ('uno', 1),
        ('dos', 2),
    ]


def test_listify_boolean():
    assert dba.listify(True) == [True]
    assert dba.listify(False) == [False]


def test_listify_integer():
    assert dba.listify(564) == [564]


def test_listify_float():
    assert dba.listify(603.23) == [603.23]


def test_listify_string():
    assert dba.listify('hello, world') == ['hello, world']


@dataclass
class Isla:
    _table_name = 'Agora.Isla'
    _primary_key = 'id_isla'
    _required = []
    _depends_on = []

    id_isla: int
    nombre: str


@pytest.fixture
def isla_tf():
    return Isla(id_isla=1, nombre='Tenerife')

# --[ create_locator ]-------------------------------------------------


def test_create_locator(isla_tf):
    assert dba.create_locator(Isla, isla_tf) == [
        'id_isla = 1',
        ]


# --[ create_select function ]-----------------------------------------


def test_create_select(isla_tf):
    expected = (
        "SELECT *\n"
        "  FROM Agora.Isla\n"
        " WHERE id_isla = 1"
        )
    assert dba.create_select(Isla, isla_tf) == expected


# --[ create_exists function ]-----------------------------------------


def test_exists_exists(isla_tf):
    expected = (
        "SELECT Count(*)\n"
        "  FROM Agora.Isla\n"
        " WHERE id_isla = 1"
        )
    assert dba.create_exists(Isla, isla_tf) == expected




if __name__ == "__main__":
    pytest.main()
