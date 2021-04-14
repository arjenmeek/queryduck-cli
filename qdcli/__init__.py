import argparse
import base64
import json
import os
import pathlib
import sys

import yaml

from datetime import datetime as dt
from functools import partial

from queryduck.context import Context
from queryduck.main import QueryDuck
from queryduck.query import Main, ObjectFor, FetchEntity, AfterTuple, QDQuery, request_params_to_query
from queryduck.schema import SchemaProcessor
from queryduck.serialization import serialize, parse_identifier, make_identifier
from queryduck.storage import VolumeFileAnalyzer, VolumeProcessor, ApiFileIterator
from queryduck.transaction import Transaction
from queryduck.types import Statement, Blob
from queryduck.utility import transform_doc, DocProcessor, safe_bytes, safe_string

from .utility import (
    call_text_editor,
    FileAnalyzer,
)


class QueryDuckCLI(object):
    """Main class for the QueryDuck client application."""

    def __init__(self, config):
        """Make the config available and initialize the API wrapper."""
        self.parser = self._create_parser()
        self.config = config
        self.qd = QueryDuck(
            self.config["connection"]["url"],
            self.config["connection"]["username"],
            self.config["connection"]["password"],
            self.config["extra_schema_files"],
        )
        self.repo = self.qd.get_repo()
        self.bindings = self.qd.get_bindings()

    def _create_parser(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("-t", "--target", default="statement")
        parser.add_argument("-o", "--output", default="show")
        parser.add_argument("command")
        parser.add_argument("options", nargs="*")
        return parser

    def run(self, *params):
        """Perform the action requested by the user"""
        args = self.parser.parse_args(params)
        if args.command == "query":
            self.action_query(target=args.target, output=args.output, param_strs=args.options)
        elif args.command == "show":
            self.action_show_resources(args.options[0])
        elif args.command == "export":
            self.action_export(args.options[0])
        elif args.command == "import":
            self.action_import(args.options[0])
        elif args.command == "edit":
            self.action_edit_resources(args.options[0])
        elif args.command == "analyze_files":
            self.action_analyze_files(args.options, output=args.output)
        elif args.command == "set_file":
            self.action_set_file(args.options)
        elif args.command == "import_schema":
            self.action_import_schema(args.options[0])
        elif args.command == "update_volume":
            self.action_update_volume(args.options[0])
        elif args.command == "process_blobs":
            self.action_process_blobs()
        elif args.command == "list_blobs":
            self.action_list_blobs()
        elif args.command == "process_volume":
            self.action_process_volume(args.options[0])
        elif args.command == "test":
            self.action_test()
        else:
            print("Unknown command:", args.command)

    def _process_query_string(self, query_string):
        if query_string == "-":
            q = yaml.load(sys.stdin, Loader=yaml.SafeLoader)
        else:
            q = yaml.load(query_string, Loader=yaml.SafeLoader)
        parser = partial(parse_identifier, self.repo, self.bindings)
        query = transform_doc(q, parser)
        return query

    def _results_to_yaml(self, results, coll):
        doctf = DocProcessor(coll, self.bindings)
        docs = [doctf.value_to_doc(s) for s in results]
        print(yaml.dump_all(docs, sort_keys=False), end="")

    def _show_results(self, results, coll):
        b = self.qd.get_bindings()
        for v in results:
            print(coll.object_for(v, b.label))
            blob = coll.object_for(v, b.fileContent)
            print(coll.get_files(blob))

    def _show_files(self, results, coll):
        b = self.qd.get_bindings()
        for v in results:
            files = coll.get_files(v)
            if len(files):
                blob = v
            else:
                blob = coll.object_for(v, self.bindings.fileContent)
            for f in coll.get_files(blob):
                filepath = self._get_file_path(f)
                if filepath:
                    sys.stdout.buffer.write(bytes(filepath) + b"\n")
                    break

    def _get_file_path(self, file_):
        for volume_reference, volume_options in self.config["volumes"].items():
            if volume_reference == file_.volume:
                p = pathlib.Path(volume_options["path"])
                return p / pathlib.Path(os.fsdecode(file_.path))
        return None

    def action_query(self, target, output, param_strs):
        repo = self.qd.get_repo()
        bindings = self.qd.get_bindings()
        def deserializer(string):
            if string.startswith("@") and string[1:] in bindings:
                return bindings[string[1:]]
            else:
                return repo.unique_deserialize(string)

        params = [p.split("=", 1) for p in param_strs]
        query = request_params_to_query(params, target, deserializer)
        results, coll = self.repo.execute(query)
        if output == "show":
            self._results_to_yaml(results, coll)
        elif output == "filepath":
            self._show_files(results, coll)

    def action_export(self, filename):
        repo = self.qd.get_repo()
        with open(filename, "w") as f:
            after = None
            while True:
                statements = repo.export_statements(after=after)
                if len(statements):
                    after = statements[-1][0]
                else:
                    break
                for q in statements:
                    f.write(json.dumps(q) + "\n")

    def action_import(self, filename):
        repo = self.qd.get_repo()
        with open(filename, "r") as f:
            quads = []
            for line in f:
                quads.append(json.loads(line))
                if len(quads) >= 10000:
                    repo.import_statements(quads)
                    quads = []
            if len(quads) > 0:
                repo.import_statements(quads)

    def action_analyze_files(self, filepaths, output):
        vfa = VolumeFileAnalyzer(self.config["volumes"])

        files = [vfa.analyze(pathlib.Path(f)) for f in filepaths]
        m = Main(Statement)
        fileContent = m.object_for(self.bindings.fileContent)
        allpred = m.object_for()
        q = QDQuery(Statement).add(
            fileContent,
            fileContent.in_list(files),
            allpred,
            allpred.fetch(),
        )

        results, coll = self.repo.execute(q)
        if output == "show":
            self._results_to_yaml(results, coll)
        elif output == "filepath":
            self._show_files(results, coll)

    def action_set_file(self, params):
        if not "//" in params:
            print("Specify options, followed by '//', followed by files")
            sys.exit(1)
        options = []
        files = []
        in_files = False
        for p in params:
            if p == "//":
                in_files = True
            elif in_files:
                files.append(p)
            else:
                options.append(p)
        print("OPTS", options)
        print("FILES", files)
        now = dt.now()
        vfa = VolumeFileAnalyzer(self.config["volumes"])
        context = self.qd.get_context()

        for filepath in files:

            f = vfa.analyze(pathlib.Path(filepath))
            m = Main(Statement)
            fileContent = m.object_for(self.bindings.fileContent)
            allpred = m.object_for()
            q = QDQuery(Statement).add(
                fileContent,
                fileContent==f,
                allpred,
                allpred.fetch(),
            )

            results, coll = self.repo.execute(q)
            if len(results) != 1:
                print(f"Need exactly 1 file, got {len(results)}")
                return

            main = results[0]
            for opt in options:
                pred_str, obj_str = opt.split("=")
                if pred_str.startswith("+"):
                    subj = last
                    pred_str = pred_str[1:]
                else:
                    subj = main
                    last = None

                pred = context.parse_identifier(pred_str)

                if obj_str == "now":
                    obj = now
                else:
                    obj = context.parse_identifier(obj_str)
                st = context.transaction.add(subj, pred, obj)
                if last is None:
                    last = st

        context.transaction.show()
        #self.repo.submit(transaction)

    def action_import_schema(self, input_filename):
        with open(input_filename, "r") as f:
            input_schema = json.load(f)
        self.repo.import_schema(input_schema, self.bindings)

    def action_update_volume(self, volume_reference):
        vcfg = self.config["volumes"][volume_reference]
        vp = VolumeProcessor(
            self.qd.conn,
            volume_reference,
            vcfg["path"],
            vcfg["exclude"] if "exclude" in vcfg else None,
        )
        vp.update()

    def _process_blob(self, blob, context, analyzer):
        b = context.bindings
        c = context.coll
        files = c.get_files(blob)
        for f in files:
            path = self._get_file_path(f)
            if path and os.path.exists(path):
                break
        else:
            print("No valid file found!")
            return

        return analyzer.process_blob(blob, path, context)

    def action_process_blobs(self):
        if not FileAnalyzer.check_requirements():
            print("Missing requirements")
            sys.exit(1)
        repo = self.qd.get_repo()
        b = self.qd.get_bindings()

        after = None
        while True:
            context = self.qd.get_context()
            m = Main(Blob)
            resource = m.subject_for(b.fileContent)
            allpred = resource.object_for()
            q = QDQuery(Blob).add(
                allpred.fetch(),
            )
            if after:
                q = q.add(AfterTuple((after,)))

            results = context.execute(q)
            fa = FileAnalyzer(context.bindings)
            for blob in results:
                self._process_blob(blob, context, fa)

            context.transaction.show()
            context.repo.submit(context.transaction)
            if len(results) >= q.limit:
                after = results[-1]
            else:
                break
            print("---------------- NEXT ------------------")

    def action_process_volume(self, volume_reference):
        repo = self.qd.get_repo()
        bindings = self.qd.get_bindings()

        root = pathlib.Path(self.config["volumes"][volume_reference]["path"])
        afi = ApiFileIterator(self.qd.conn, volume_reference, without_statements=True)
        transaction = Transaction()
        fa = FileAnalyzer(bindings)
        for idx, remote in enumerate(afi):
            print(idx, remote)
            path = root / pathlib.Path(remote["path"])
            blob = repo.unique_deserialize("blob:{}".format(remote["handle"]))

            resource = transaction.add(None, bindings.type, bindings.Resource)
            transaction.ensure(resource, bindings.fileContent, blob)

            preview_hash = base64.urlsafe_b64encode(blob.handle).decode()
            preview_path = "{}/{}/{}.webp".format(
                self.config["previews"]["path"],
                preview_hash[0:2],
                preview_hash[2:9],
            )

            try:
                info = fa.analyze(path, preview_path)
            except:
                continue
            for k, v in info.items():
                print(
                    bindings.reverse(k),
                    [bindings.reverse(vv) for vv in (v if type(v) == list else [v])],
                )
            for k, v in info.items():
                values = v if type(v) == list else [v]
                for val in values:
                    transaction.ensure(resource, k, val)

            if idx > 1000:
                break
                # TODO: Multiple chunks into multiple transactions

        transaction.show()
        print("THE END")
        return
        repo.submit(transaction)

    def _check_include_blob(self, blob, context):
        b, c = context.get_bc()
        include = False
        rules = self.config.get("qdcli", {}).get("include_rules", [])
        resources = c.subjects_for(blob, b.fileContent)
        for resource in resources:
            for rule in rules:
                set_include = True
                skip = False
                for k, v in rule.items():
                    if k == "include":
                        set_include = v
                    else:
                        values = c.objects_for(resource, b[k])
                        if not b[v] in values:
                            skip = True
                            break
                if skip:
                    continue

                include = set_include
            if include:
                return include
        return include

    def action_list_blobs(self):

        after = None
        while True:
            context = self.qd.get_context()
            b, c = context.get_bc()
            m = Main(Blob)
            resource = m.subject_for(b.fileContent)
            allpred = resource.object_for()
            q = QDQuery(Blob).add(
                allpred.fetch(),
            )
            if after:
                q = q.add(AfterTuple((after,)))

            results = context.execute(q)
            for blob in results:
                if not self._check_include_blob(blob, context):
                    continue
                scores = []
                for r in c.subjects_for(blob, b.fileContent):
                    for s in c.objects_for(r, b.score):
                        print("SCORE", r, s)
                        scores.append(s)
                for f in c.get_files(blob):
                    path = self._get_file_path(f)
                    if path and os.path.exists(path):
                        print(safe_string(str(path)))
                        break

            if len(results) >= q.limit:
                after = results[-1]
            else:
                break

    def identifier_to_docs(self, identifier):
        docs = []
        if identifier.startswith('/'):
            dummy, *types, label = identifier.split('/')
            results, coll = self.repo.query({MatchObject(self.bindings.label): label, FetchObject(None): None})
            if len(results):
                doctf = DocProcessor(coll, self.bindings)
                docs += [doctf.value_to_doc(s) for s in results]
            else:
                types = ['Resource'] + types
                docs.append(
                    {
                        "/": identifier,
                        "label": label,
                        "type": types,
                    }
                )
        return docs

    def edit_docs(self, docs):
        text = yaml.dump_all(docs, sort_keys=False)
        text = call_text_editor(text)
        docs = list(yaml.load_all(text, Loader=yaml.SafeLoader))
        return docs

    def docs_to_transaction(self, docs):
        transaction = Transaction()
        b = self.bindings
        for doc in docs:
            if "=" in doc:
                resource = self.repo.unique_deserialize(doc["="])
                results, coll = self.repo.query({"eq": resource, FetchObject(None): None})
            else:
                resource = transaction.add(None, b.type, b.Resource)
                coll = None
            for k, v in doc.items():
                if k == "=":
                    continue
                elif k == "/":
                    dummy, *types, label = v.split("/")
                    for type_ in types:
                        if not coll or not coll.find(resource, b.type, b[type_]):
                            transaction.ensure(resource, b.type, b[type_])
                    if not coll or not coll.find(resource, b.label, label):
                        transaction.ensure(resource, b.label, label)
                else:
                    prd = b[k]
                    if type(v) != list:
                        v = [v]
                    for ser_obj in v:
                        if ser_obj in b:
                            obj = b[ser_obj]
                        else:
                            obj = self.repo.unique_deserialize(ser_obj)
                        if not coll or not coll.find(resource, prd, obj):
                            transaction.ensure(resource, prd, obj)

        if len(transaction.statements):
            transaction.show()
            if input("Submit y/n? [n] ") == "y":
                self.repo.submit(transaction)
        else:
            print("No new statements.")

    def action_show_resources(self, identifier):
        docs = []
        docs += self.identifier_to_docs(identifier)
        print(yaml.dump_all(docs, sort_keys=False), end="")

    def action_edit_resources(self, identifier):
        docs = []
        docs += self.identifier_to_docs(identifier)
        docs = self.edit_docs(docs)
        self.docs_to_transaction(docs)

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
        if querystr == "-":
            q = yaml.load(sys.stdin, Loader=yaml.SafeLoader)
        else:
            q = yaml.load(querystr, Loader=yaml.SafeLoader)
        rp = ResourceProcessor(self)
        docs = rp.query(q, target="blob")
        print(yaml.dump_all(docs, sort_keys=False), end="")

    def action_file_query(self, querystr):
        if querystr == "-":
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

    def action_process_schema_template(self, template_file, output_file):
        with open(template_file, "r") as f:
            tpl = yaml.load(f, Loader=yaml.SafeLoader)
        rp = self.get_rp()
        result = rp.process_schema_template(tpl)
        with open(output_file, "w") as f:
            json.dump(result, f)

    def action_fill_prototype(self, input_filename, output_filename=None):
        if output_filename is None:
            output_filename = input_filename
        with open(input_filename, "r") as f:
            input_schema = json.load(f)
        schema_processor = SchemaProcessor()
        output_schema = schema_processor.fill_prototype(input_schema)
        with open(output_filename, "w") as f:
            f.write("{}\n".format(json.dumps(output_schema, indent=4)))

    def action_process_files(self, *paths):
        sp = self.get_sp()
        return sp.process_files(paths)

    def action_test(self):
        context = self.qd.get_context()
        b, c = context.get_bc()
        st = context.transaction.add(None, b.type, b.Resource)
        st2 = context.transaction.add(st, b.label, "Testing")
        context.transaction.show()
        print(st, st.triple)
        print(st2, st2.triple)
        context.repo.submit(context.transaction)
        print(st, st.triple)
        print(st2, st2.triple)
