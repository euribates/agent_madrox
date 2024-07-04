#!/usr/bin/env python3.12

from abc import ABC


class Result(ABC):
    """Representa el resultado de una función o método.

    No se debe usar directamente esta clase, sino sus clases dervadas
    `Success` o `Failure`.
    """

    def __init__(self, success, outcome):
        self._success = success
        self._outcome = outcome

    def __bool__(self):
        return self._success

    def is_success(self):
        return self._success

    def is_failure(self):
        return not self._success


class Success(Result):
    """Representación de un resultado valido. El valor del
    resultado esta en el atributo `value`.
    """

    def __init__(self, value=True):
        super().__init__(True, value)

    @property
    def value(self):
        return self._outcome

    def __repr__(self):
        return 'Success({self.value!r})'

    def __bool__(self):
        return True

    @property
    def error_message(self):
        raise ValueError(
            'No se puede acceder a la propiedad error_message'
            ' en una instancia de Success.'
            )


class Failure(Result):

    def __init__(self, error_message):
        super().__init__(False, error_message)

    @property
    def error_message(self):
        return self._outcome

    def __repr__(self):
        return 'Failure({self.error_message!r})'

    def __bool__(self):
        return False

    @property
    def value(self):
        raise ValueError(
            'No se puede acceder a la propiedad value'
            ' en una instancia de Failure.'
            )
