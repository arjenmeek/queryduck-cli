import yaml

from crunchylib.api import CrunchyAPI
from crunchylib.repository import StatementRepository
from crunchylib.schema import Schema

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
    ]

    def __init__(self, config):
        """Make the config available and initialize the API wrapper."""
        self.config = config
        self.api = CrunchyAPI(self.config['api']['url'])
        self.statements = StatementRepository(self.api)
        schema_keys = list(set(self.schema_keys) | set(self.config['schema']['keys']))
        self.schema = Schema(self.statements.load_schema(
            self.config['schema']['root_uuid'],
            schema_keys))

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

    def action_file_info(self, *paths):
        sp = self.get_sp()
        return sp.file_info(paths)

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
