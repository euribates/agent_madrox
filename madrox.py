#!/usr/bin/env python3.12
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
                indent = ('  ' * level) + '[green]▶[/]'
                self.print(f'{indent} [bold][yellow]{val}[/] {OK}', **kwargs)
            case Failure(error_message=msg):
                self.log.error(f'Error: {msg}')
                indent = ('  ' * level) + '[red]▶[/]'
                self.print(f'{indent} [bold]{msg}[/] {ERROR}', **kwargs)
            case _:
                self.log.info(str(message))
                indent = ('  ' * level) + '[white]▶[/]'
                self.print(f'{indent} {message}', **kwargs)
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
        primary_key = getattr(source, model.Meta.primary_key)
        exclude = set([model.Meta.primary_key])
        new_values = model._to_dict(source, exclude=exclude)
        old_values = model._to_dict(target, exclude=exclude)
        if new_values != old_values:  # Update needed
            diff_values = {}
            for name in old_values:
                old_value = old_values[name]
                new_value = new_values[name]
                if new_value != old_value:
                    if self.is_verbose:
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
        subject = f'{model.Meta.table_name}[{primary_key!r}]'
        if self.options.verbose:
            self.out(f'Migrando [bold yellow]{subject}[/]', level=level)
        instance = model._load_instance(self.db_source, primary_key)
        if not instance:
            return self.out(Failure(f"No puedo cargar {subject} en origen"))

        # Dependencias previas
        for field_name, submodel in model.Meta.depends_on.items():
            f_key = submodel.Meta.primary_key
            if self.options.verbose:
                self.out(
                    f'Depende de {field_name} = {submodel!r}.{f_key}',
                    level=level,
                    )
            value = getattr(instance, field_name)
            if value is not None:
                self.migrar_modelo(submodel, value, level=level+1)

        # Instancia actual
        if self.is_verbose:
            self.out(
                f'Migrando instancia actual {model.__name__}[{primary_key}]',
                level=level,
                )
        no_existe = instance.not_exists(self.db_target)
        if no_existe:  # Insert
            result = self._do_insert(model, instance)
            if self.is_verbose:
                self.out(result, level=level)
        else:
            target = model._load_instance(self.db_target, primary_key)
            if not target:
                target = model._load_from_natural_keys(self.db_target, instance)
            if target:
                result = self._do_update(model, instance, target)
            else:
                result = self._do_insert(model, instance)
            if self.is_verbose:
                self.out(result, level=level)

        # modelos subordinados
        for submodel in model.Meta.master_of:
            self.out(f'Entidad dependiente {submodel}', level=level+1)
            if self.options.verbose:
                self.out(f'Veamos las entidades dependientes {submodel}', level=level+1)
            masons = submodel._load_instances(
                self.db_source,
                model.Meta.primary_key,
                value=primary_key,
                )
            for mason in masons:
                mason_pk = getattr(mason, submodel.Meta.primary_key)
                self.migrar_modelo(submodel, mason_pk, level=level+1)
        return Success()

    def get_parser(self):
        parser = argparse.ArgumentParser(
            prog='madrox',
            description='Migrar entre bases de datos',
            )
        group = parser.add_mutually_exclusive_group()
        group.add_argument('-v', '--verbose', action='store_true')
        group.add_argument('-m', '--muted', action='store_true')
        subparsers = parser.add_subparsers(help='Comandos displonibles')
        # ls
        ls_parser = subparsers.add_parser(
            'ls',
            help='Listar modelos en el catálogo',
            )
        ls_parser.add_argument('model', nargs='*')
        ls_parser.set_defaults(func=self.cmd_ls)

        # graph
        graph_parser = subparsers.add_parser(
            'graph',
            help='Mostrar relaciones entre modelos en el catálogo',
            )
        graph_parser.add_argument('model', nargs='+')
        graph_parser.set_defaults(func=self.cmd_graph)

        # duplicate (Migrate just one instance)
        duplicate_parser = subparsers.add_parser(
            'duplicate',
            help='migrar una instancia de un modelo, dada su clave primaria',
            )
        duplicate_parser.add_argument('model')
        duplicate_parser.add_argument('pk')
        duplicate_parser.set_defaults(func=self.cmd_duplicate)

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
        if len(models) == 0 or models[0] == 'all':
            models = list(catalog.keys())
        for model_name in models:
            self.print(model_name)
        return 0

    def cmd_duplicate(self, options):
        self.options = options
        model_name = options.model
        pk = options.pk
        model = catalog[model_name]
        if not model._is_migrable():
            self.die('El modelo indicado no es migrable')
        if self.options.verbose:
            self.out(f'Migrando registro {pk} de {model}')
        self.migrar_modelo(model, pk, level=0)

    def cmd_migrate(self, options):
        self.options = options
        models = options.model
        if len(models) == 1 and models[0] == 'all':
            models = list(catalog.keys())
        if self.is_verbose:
            self.out(
                'Migrando registros creados o modificados'
                f' en los ultimos {options.num_days} días.',
                )
        tasks = {}
        with Progress() as progress:
            for model_name in models:
                model = catalog[model_name]
                if not model._is_migrable():
                    continue
                primary_keys = list(model._since(self.db_source, num_days=options.num_days))
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
                key = model.Meta.primary_key
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
