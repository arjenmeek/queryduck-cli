import json
import yaml

from crunchylib.api import CrunchyAPI
from crunchylib.repository import StatementRepository
from crunchylib.schema import Schema
from crunchylib.types import Statement

from .resource import ResourceProcessor
from .storage import StorageProcessor


class CrunchyCLIClient(object):
    """Main class for the CrunchyVicar client application."""

    schema_keys = [
        'Resource',
        'type',
        'label',
        'ComputerFile',
        'content',
        'file_size',
        'VideoFile',
    ]

    def __init__(self, config):
        """Make the config available and initialize the API wrapper."""
        self.config = config
        self.api = CrunchyAPI(self.config['api']['url'])
        self.statements = StatementRepository(self.api)
        self.schema = None

    def get_schema(self):
        root = Statement(uuid_=self.config['schema']['root_uuid'])
        schema = {}
        slist = self.statements.query(query={root: {"ne": None}})
        for s in slist:
            res = self.statements.get_statement_attribute(s, root)
            if type(res[0]) == str:
                schema[res[0]] = s
        self.schema = Schema(schema)
        return self.schema

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
