import argparse
import json
import os
import pathlib
import yaml

from functools import partial

from queryduck.main import QueryDuck
from queryduck.schema import SchemaProcessor
from queryduck.types import Statement, Inverted, serialize
from queryduck.serialization import parse_identifier, make_identifier
from queryduck.storage import VolumeFileAnalyzer
from queryduck.utility import transform_doc, value_to_doc

from .resource import ResourceProcessor
from .storage import StorageProcessor


class QueryDuckCLI(object):
    """Main class for the QueryDuck client application."""

    def __init__(self, config):
        """Make the config available and initialize the API wrapper."""
        self.parser = self._create_parser()
        self.config = config
        self.qd = QueryDuck(
            self.config['connection']['url'],
            self.config['connection']['username'],
            self.config['connection']['password'],
            self.config['extra_schema_files'],
        )
        self.repo = self.qd.get_repo()
        self.bindings = self.qd.get_bindings()

    def _create_parser(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('-t', '--target', default='statement')
        parser.add_argument('-o', '--output', default='show')
        parser.add_argument('command')
        parser.add_argument('options', nargs='*')
        return parser

    def run(self, *params):
        """Perform the action requested by the user"""
        args = self.parser.parse_args(params)
        if args.command == 'query':
            self.action_query(
                args.options[0],
                target=args.target,
                output=args.output)

    def _process_query_string(self, query_string):
        if query_string == '-':
            q = yaml.load(sys.stdin, Loader=yaml.SafeLoader)
        else:
            q = yaml.load(query_string, Loader=yaml.SafeLoader)
        parser = partial(parse_identifier, self.repo, self.bindings)
        query = transform_doc(q, parser)
        return query

    def _result_to_yaml(self, result):
        docs = [value_to_doc(result, self.bindings, st) for st in result.values]
        print(yaml.dump_all(docs, sort_keys=False), end='')

    def _show_result(self, result):
        b = self.qd.get_bindings()
        for v in result.values:
            print(result.object_for(v, b.label))
            blob = result.object_for(v, b.fileContent)
            if blob in result.files:
                print(result.files[blob])

    def action_query(self, querystr, target, output):
        query = self._process_query_string(querystr)
        result = self.repo.query(query, target=target)
        if output == 'show':
            self._result_to_yaml(result)

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

    def action_bquery(self, querystr):
        if querystr == '-':
            q = yaml.load(sys.stdin, Loader=yaml.SafeLoader)
        else:
            q = yaml.load(querystr, Loader=yaml.SafeLoader)
        rp = ResourceProcessor(self)
        docs = rp.query(q, target='blob')
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

    def action_fill_prototype(self, input_filename, output_filename=None):
        if output_filename is None:
            output_filename = input_filename
        with open(input_filename, 'r') as f:
            input_schema = json.load(f)
        schema_processor = SchemaProcessor()
        output_schema = schema_processor.fill_prototype(input_schema)
        with open(output_filename, 'w') as f:
            f.write('{}\n'.format(json.dumps(output_schema, indent=4)))

    def action_import_schema(self, input_filename):
        bindings = self.qd.get_bindings()
        with open(input_filename, 'r') as f:
            input_schema = json.load(f)
        schema_processor = SchemaProcessor()
        statements = schema_processor.statements_from_schema(bindings, input_schema)
        repo = self.qd.get_repo()
        repo.raw_create(statements)

    def action_process_files(self, *paths):
        sp = self.get_sp()
        return sp.process_files(paths)
