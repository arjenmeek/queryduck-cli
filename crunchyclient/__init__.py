import json
import yaml

from crunchylib.connection import Connection
from crunchylib.repository import StatementRepository
from crunchylib.schema import Schema, SchemaProcessor
from crunchylib.types import Statement

from .resource import ResourceProcessor
from .storage import StorageProcessor


class CrunchyCLIClient(object):
    """Main class for the CrunchyVicar client application."""

    def __init__(self, config):
        """Make the config available and initialize the API wrapper."""
        self.config = config
        self._connection = Connection(self.config['api']['url'])
        self._statements = None
        self._bindings = None

    def get_statement_repository(self):
        if self._statements is None:
            self._statements = StatementRepository(self._connection)
        return self._statements

    def get_bindings(self):
        if self._bindings is None:
            schemas = []
            for filename in self.config['schema_files']:
                with open(filename, 'r') as f:
                    schemas.append(json.load(f))
            repo = self.get_statement_repository()
            self._bindings = repo.bindings_from_schemas(schemas)
        return self._bindings

    def run(self, *params):
        """Perform the action requested by the user"""
        method = getattr(self, 'action_{}'.format(params[0]))
        return method(*params[1:])

    def get_rp(self):
        rp = ResourceProcessor(self)
        return rp

    def get_sp(self):
        sp = StorageProcessor(self)
        return sp

    def action_update_volume(self, volume_reference):
        sp = self.get_sp()
        return sp.update_volume(volume_reference)

    def action_process_volume(self, volume_reference):
        sp = self.get_sp()
        return sp.process_volume(volume_reference)

    def action_file_info(self, *paths):
        sp = self.get_sp()
        return sp.file_info(paths)

    def action_file_process(self, *paths):
        sp = self.get_sp()
        return sp.file_process(paths)

    def action_file_edit(self, *paths):
        sp = self.get_sp()
        return sp.file_edit(paths)

    def action_file_options(self, path, *options):
        sp = self.get_sp()
        return sp.file_options(path, *options)

    def action_write(self, filename, *references):
        rp = ResourceProcessor(self)
        return rp.write(filename, *references)

    def action_output(self, *references):
        rp = ResourceProcessor(self)
        return rp.output(*references)

    def action_read(self, filename):
        rp = ResourceProcessor(self)
        return rp.read(filename)

    def action_query(self, querystr):
        if querystr == '-':
            q = yaml.load(sys.stdin, Loader=yaml.SafeLoader)
        else:
            q = yaml.load(querystr, Loader=yaml.SafeLoader)
        rp = ResourceProcessor(self)
        docs = rp.query(q)
        print(yaml.dump_all(docs, sort_keys=False), end='')

    def action_file_query(self, querystr):
        if querystr == '-':
            q = yaml.load(sys.stdin, Loader=yaml.SafeLoader)
        else:
            q = yaml.load(querystr, Loader=yaml.SafeLoader)
        sp = StorageProcessor(self)
        paths = sp.file_query(q)
        [print(p) for p in paths]

    def action_set(self, *params):
        rp = ResourceProcessor(self)
        return rp.set(*params)

    def action_edit(self, *references):
        rp = self.get_rp()
        return rp.edit(*references)

    def action_export(self, filename):
        rp = self.get_rp()
        with open(filename, 'w') as f:
            for q in rp.export_statements():
                f.write(json.dumps(q) + '\n')

    def action_import(self, filename):
        rp = self.get_rp()
        with open(filename, 'r') as f:
            quads = []
            for line in f:
                quads.append(json.loads(line))
        rp.import_statements(quads)

    def action_process_schema_template(self, template_file, output_file):
        with open(template_file, 'r') as f:
            tpl = yaml.load(f, Loader=yaml.SafeLoader)
        rp = self.get_rp()
        result = rp.process_schema_template(tpl)
        with open(output_file, 'w') as f:
            json.dump(result, f)

    def _parse_identifier(self, value):
        if type(value) != str:
            v = value
        elif self.schema and value.startswith('.'):
            v = self.schema[value[1:]]
        elif self.schema and value.startswith('/'):
            parts = value[1:].split('/')
            filters = [s.type==s.Resource]
            for type_ in parts[:-1]:
                if type_ == 'Resource':
                    continue
                filters.append(s.type==s[type_])
            filters.append(s.label==parts[-1])
            statements = self.master.statements.query(*filters)
            return statements[0] if len(statements) else None
        elif value.startswith('file:'):
            sp = self.master.get_sp()
            v = sp.get_blob_by_path(value[5:])
        elif ':' in value:
            v = self.master.statements.sts.unique_deserialize(value)
        else:
            v = value
        return v

    def action_fill_prototype(self, input_filename, output_filename):
        with open(input_filename, 'r') as f:
            input_schema = json.load(f)
        schema_processor = SchemaProcessor()
        output_schema = schema_processor.fill_prototype(input_schema)
        with open(output_filename, 'w') as f:
            json.dump(output_schema, f)

    def action_import_schema(self, input_filename):
        with open(input_filename, 'r') as f:
            input_schema = json.load(f)
        schema_processor = SchemaProcessor()
        statements = schema_processor.statements_from_schema(input_schema)
        repo = self.get_statement_repository()
        repo.raw_create(statements)

    def action_process_files(self, *paths):
        sp = self.get_sp()
        return sp.process_files(paths)
