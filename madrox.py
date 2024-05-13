#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import argparse
from rich.progress import Progress
from rich.console import Console

from models import catalog
from results import Success, Failure
from settings import DEFAULT_SINCE_DAYS
import dba


OK = "[green]✓[/green]"
ERROR = "[red]✖[/red]"


class Handler:

    def __init__(self):
        self.console = Console()
        logging.basicConfig(filename='madrox.log', level='DEBUG')
        self.log = logging.getLogger('madrox')
        self.log.setLevel(logging.DEBUG)
        self.db_source = dba.get_database_connection('DB_SOURCE')
        self.db_target = dba.get_database_connection('DB_TARGET')

    def print(self, *args, **kwargs):
        self.console.print(*args, **kwargs)

    def out(self, message, **kwargs):
        level = int(kwargs.pop('level', 0))
        match message:
            case Success(value=val):
                self.log.warning(f'Success: {val}')
                if self.is_verbose:
                    indent = ('  ' * level) + '[green]▶[/]'
                    self.console.print(f'{indent} [bold][yellow]{val}[/] {OK}', **kwargs)
            case Failure(error_message=msg):
                self.log.error(f'Error: {msg}')
                if self.is_verbose:
                    indent = ('  ' * level) + '[red]▶[/]'
                    self.console.print(f'{indent} [bold]{msg}[/] {ERROR}', **kwargs)
            case _:
                self.log.info(str(message))
                if self.is_verbose:
                    indent = ('  ' * level) + '[white]▶[/]'
                    self.console.print(f'{indent} {message}', **kwargs)
        return message

    def has_equal_num_of_rows(self, model, query=''):
        table = model.Meta.tabe_name
        if query:
            sql = f'SELECT count(*) FROM {table} WHERE {query}'
        else:
            sql = f'SELECT count(*) FROM {table}'
        num_source = dba.get_scalar(self.db_source, sql)
        num_target = dba.get_scalar(self.db_target, sql)
        return Success() if num_source == num_target else Failure(
            f'Hay {num_source} registros en origen'
            f' pero {num_target} en destino.'
            )

    def _do_update(self, model, source, target):
        primary_key = getattr(source, model.Meta.primary_key.name)
        exclude = set([model.Meta.primary_key.name])
        new_values = model._to_dict(source, exclude=exclude)
        old_values = model._to_dict(target, exclude=exclude)
        if new_values != old_values:  # Update needed
            diff_values = {}
            for name in old_values:
                old_value = old_values[name]
                new_value = new_values[name]
                if new_value != old_value:
                    self.out(f'{name} {new_value} != {old_value}')
                    diff_values[name] = new_values[name]
            model._update(self.db_target, primary_key, diff_values)
            return Success('Ya existe. Actualizado')
        return Success('Sin cambios')

    def _do_insert(self, model, instance):
        values = model._to_dict(instance)
        model._insert(self.db_target, values)
        return Success('No existe. Insertado')

    def migrar_modelo(self, model, primary_key, level=0):
        subject = model._locator(primary_key)
        self.out(f'Migrando [bold yellow]{subject}[/]', level=level)
        instance = model._load_instance(self.db_source, primary_key)
        if not instance:
            return self.out(Failure(f"No puedo cargar {subject} en origen"))

        # Dependencias previas
        for field_name, submodel in model.Meta.depends_on.items():
            f_key = submodel.Meta.primary_key
            self.out(
                f'Depende de {field_name} = {submodel!r}.{f_key}',
                level=level,
                )
            value = getattr(instance, field_name)
            if value is not None:
                self.migrar_modelo(submodel, value, level=level+1)
        
        # Instancia actual
        self.out(f'Migrando instancia actual {model.__name__}[{primary_key}]')
        target = model._load_instance(self.db_target, primary_key)
        if target:  # Ya existe. Update
            self.out(self._do_update(model, instance, target), level=level)
        else:  # Insert
            self.out(self._do_insert(model, instance), level=level)
        
        # modelos subordinados
        for submodel in model.Meta.master_of:
            submodel_name = submodel.__name__
            self.out(f'Migrando Entidad subordinada {submodel_name}')
            masons = submodel._load_instances(
                self.db_source,
                model.Meta.primary_key.name,
                value=primary_key,
                )
            for mason in masons:
                mason_pk = getattr(mason, submodel.Meta.primary_key.name)
                self.migrar_modelo(submodel, mason_pk, level=level+1)
        return Success()

    def get_parser(self):
        parser = argparse.ArgumentParser(
            prog='madrox',
            description='Migrar entre bases de datos',
            )
        subparsers = parser.add_subparsers(help='Comandos displonibles')
        # ls
        ls_parser = subparsers.add_parser(
            'ls',
            help='Listar modelos en el catálogo',
            )
        ls_parser.add_argument('model', nargs='+')
        ls_parser.set_defaults(func=self.cmd_ls)

        # graph
        graph_parser = subparsers.add_parser(
            'graph',
            help='Mostrar relaciones entre modelos en el catálogo',
            )
        graph_parser.add_argument('model', nargs='+')
        graph_parser.set_defaults(func=self.cmd_graph)

        # migrate
        migrate_parser = subparsers.add_parser(
            'migrate',
            help='migrar los modelos del catalog',
            )
        migrate_parser.add_argument(
            'model',
            nargs='+',
            )
        migrate_parser.add_argument(
            '--num-days',
            type=int,
            help='Número de días a migrar',
            default=DEFAULT_SINCE_DAYS,
            )
        group = migrate_parser.add_mutually_exclusive_group()
        group.add_argument('-v', '--verbose', action='store_true')
        group.add_argument('-m', '--muted', action='store_true')
        migrate_parser.set_defaults(func=self.cmd_migrate)
        return parser

    def run(self):
        parser = self.get_parser()
        options = parser.parse_args()
        self.is_verbose = options.verbose
        self.is_muted = options.muted
        return options.func(options)

    def cmd_ls(self, options):
        models = options.model
        if len(models) == 1 and models[0] == 'all':
            models = list(catalog.keys())
        for model_name in models:
            self.print(model_name)
        return 0

    def cmd_migrate(self, options):
        self.num_days = options.num_days
        models = options.model
        if len(models) == 1 and models[0] == 'all':
            models = list(catalog.keys())
        self.out(
            'Migrando registros creados o modificados'
            f' en los ultimos {self.num_days} días.'
            )
        tasks = {}
        with Progress() as progress:
            for model_name in models:
                model = catalog[model_name]
                if not model._is_migrable():
                    continue
                primary_keys = list(model._since(self.db_source, num_days=self.num_days))
                total = len(primary_keys)
                tasks[model_name] = progress.add_task(
                    description=f'{model_name} 0/{total}',
                    total=len(primary_keys),
                    )

                for counter, pk in enumerate(primary_keys, start=1):
                    self.migrar_modelo(model, pk, level=0)
                    progress.update(
                        tasks[model_name],
                        description=f'{model_name} {counter}/{total}',
                        advance=1,
                        )
        return 0

    def cmd_graph(self, options):
        models = options.model
        if len(models) == 1 and models[0] == 'all':
            models = list(catalog.keys())
        for model_name in models:
            model = catalog[model_name]
            self.print(f'{model_name}')
            for _field_name, _submodel in model.Meta.depends_on.items():
                name = _submodel.__name__
                self.print(f' -> Depende de {_field_name} -> {name}')
            for _submodel in model.Meta.master_of:
                name = _submodel.__name__
                key = model.Meta.primary_key.name
                self.print(f' <- Master de {name}.{key}')
        return 0

# @app.command()
# def check():
    # db_source = dba.get_database_connection('DB_SOURCE')
    # db_target = dba.get_database_connection('DB_TARGET')
    # elecciones = [2003, 2007, 2011, 2015, 2019, 2023]
    # print('Checking elecciones...')
    # for eleccion in elecciones:
        # check_actas(db_source, db_target, eleccion)

if __name__ == "__main__":
    handler = Handler()
    handler.run()
