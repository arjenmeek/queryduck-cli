import argparse
import json
import os
import pathlib
import sys

import yaml

from functools import partial

from queryduck.main import QueryDuck
from queryduck.schema import SchemaProcessor
from queryduck.types import Statement, Inverted, serialize
from queryduck.serialization import parse_identifier, make_identifier
from queryduck.storage import VolumeFileAnalyzer
from queryduck.transaction import Transaction
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
        elif args.command == 'analyze_file':
            self.action_analyze_file(
                args.options[0],
                output=args.output)
        elif args.command == 'set_file':
            self.action_set_file(
                args.options[0],
                options=args.options[1:])
        elif args.command == 'import_schema':
            self.action_import_schema(
                args.options[0])
        else:
            print("Unknown command:", args.command)

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

    def _show_files(self, result):
        b = self.qd.get_bindings()
        for v in result.values:
            blob = result.object_for(v, self.bindings.fileContent)
            if not blob in result.files:
                continue
            for f in result.files[blob]:
                filepath = self._get_file_path(f)
                if filepath:
                    sys.stdout.buffer.write(filepath + b'\n')
                    break

    def _get_file_path(self, file_):
        for volume_reference, volume_options in self.config['volumes'].items():
            if volume_reference == file_.volume:
                return volume_options['path'].encode() + b'/' + file_.path
        return None

    def action_query(self, querystr, target, output):
        query = self._process_query_string(querystr)
        result = self.repo.query(query, target=target)
        if output == 'show':
            self._result_to_yaml(result)
        elif output == 'filepath':
            self._show_files(result)

    def action_analyze_file(self, filepath, output):
        vfa = VolumeFileAnalyzer(self.config['volumes'])

        f = vfa.analyze(pathlib.Path(filepath))
        result = self.repo.query({self.bindings.fileContent: f})
        if output == 'show':
            self._result_to_yaml(result)
        elif output == 'filepath':
            self._show_files(result)

    def action_set_file(self, filepath, options):
        vfa = VolumeFileAnalyzer(self.config['volumes'])

        f = vfa.analyze(pathlib.Path(filepath))
        result = self.repo.query({self.bindings.fileContent: f})
        if len(result.values) != 1:
            print("Need exactly one file!")
            return

        main = result.values[0]
        transaction = Transaction()
        for opt in options:
            pred_str, obj_str = opt.split('=')
            if pred_str.startswith('+'):
                subj = last
                pred_str = pred_str[1:]
            else:
                subj = main
                last = None

            pred = parse_identifier(self.repo, self.bindings, pred_str)
            obj = parse_identifier(self.repo, self.bindings, obj_str)
            st = transaction.add(subj, pred, obj)
            if last is None:
                last = st

        transaction.show()
        self.repo.submit(transaction)

    def action_import_schema(self, input_filename):
        with open(input_filename, 'r') as f:
            input_schema = json.load(f)
        self.repo.import_schema(input_schema, self.bindings)

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

    def action_process_files(self, *paths):
        sp = self.get_sp()
        return sp.process_files(paths)
