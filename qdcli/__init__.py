import argparse
import base64
import json
import os
import pathlib
import sys

import yaml

from functools import partial

from queryduck.main import QueryDuck
from queryduck.query import MatchObject, MatchSubject, FetchObject, FetchSubject
from queryduck.schema import SchemaProcessor
from queryduck.serialization import serialize, parse_identifier, make_identifier
from queryduck.storage import VolumeFileAnalyzer, VolumeProcessor, ApiFileIterator
from queryduck.transaction import Transaction
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
            self.action_query(args.options[0], target=args.target, output=args.output)
        elif args.command == "show":
            self.action_show_resources(args.options[0])
        elif args.command == "edit":
            self.action_edit_resources(args.options[0])
        elif args.command == "analyze_file":
            self.action_analyze_file(args.options[0], output=args.output)
        elif args.command == "set_file":
            self.action_set_file(args.options[0], options=args.options[1:])
        elif args.command == "import_schema":
            self.action_import_schema(args.options[0])
        elif args.command == "update_volume":
            self.action_update_volume(args.options[0])
        elif args.command == "process_blobs":
            self.action_process_blobs()
        elif args.command == "process_volume":
            self.action_process_volume(args.options[0])
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

    def _result_to_yaml(self, result, coll):
        doctf = DocProcessor(coll, self.bindings)
        docs = [doctf.value_to_doc(s) for s in result.values]
        print(yaml.dump_all(docs, sort_keys=False), end="")

    def _show_result(self, result, coll):
        b = self.qd.get_bindings()
        for v in result.values:
            print(coll.object_for(v, b.label))
            blob = coll.object_for(v, b.fileContent)
            if blob in coll.files:
                print(coll.files[blob])

    def _show_files(self, result, coll):
        b = self.qd.get_bindings()
        for v in result.values:
            if v in coll.files:
                blob = v
            else:
                blob = coll.object_for(v, self.bindings.fileContent)
            if not blob in coll.files:
                continue
            for f in coll.files[blob]:
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

    def action_query(self, querystr, target, output):
        query = self._process_query_string(querystr)
        result, coll = self.repo.query(query, target=target)
        if output == "show":
            self._result_to_yaml(result, coll)
        elif output == "filepath":
            self._show_files(result, coll)

    def action_analyze_file(self, filepath, output):
        vfa = VolumeFileAnalyzer(self.config["volumes"])

        f = vfa.analyze(pathlib.Path(filepath))
        result, coll = self.repo.query({MatchObject(self.bindings.fileContent): f})
        if output == "show":
            self._result_to_yaml(result, coll)
        elif output == "filepath":
            self._show_files(result, coll)

    def action_set_file(self, filepath, options):
        vfa = VolumeFileAnalyzer(self.config["volumes"])

        f = vfa.analyze(pathlib.Path(filepath))
        result, coll = self.repo.query({MatchObject(self.bindings.fileContent): f})
        if len(result.values) != 1:
            print("Need exactly one file!")
            return

        main = result.values[0]
        transaction = Transaction()
        for opt in options:
            pred_str, obj_str = opt.split("=")
            if pred_str.startswith("+"):
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

    def action_process_blobs(self):
        repo = self.qd.get_repo()
        b = self.qd.get_bindings()

        query = {
            MatchSubject(b.fileContent): {
                MatchObject(b.fileType): None,
                FetchObject(None): None,
            },
        }
        after = None
        more = True
        seen = 0
        avail = 0
        unkn = 0
        fa = FileAnalyzer(self.bindings)
        while more:
            res, coll = repo.query(query=query, target="blob", after=after)
            transaction = Transaction()
            more = res.more
            if more:
                after = res.values[-1]
            for blob in res.values:
                seen += 1
                if not blob in coll.files:
                    continue
                for f in coll.files[blob]:
                    path = self._get_file_path(f)
                    if path:
                        break
                else:
                    continue
                avail += 1
                for file_content in coll.find(o=blob):
                    resource = file_content.triple[0]
                    if coll.objects_for(resource, b.fileType):
                        continue
                    unkn += 1
                    print(safe_string(str(path)))
                    try:
                        info = fa.analyze(path)
                    except Exception as e:
                        print("ERROR", e)
                        break

                    for k, values in info.items():
                        if type(values) != list:
                            values = [values]
                        for v in values:
                            if not coll.find(s=resource, p=k, o=v):
                                transaction.ensure(resource, k, v)
                            else:
                                print("KNOWN", resource, k, v)
                    # print(res.object_for(resource, b.label))
                    # print(" ", [b.reverse(s) for s in coll.objects_for(resource, b.fileType)])
            transaction.show()
            repo.submit(transaction)
            print(seen, avail, unkn, after)

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

    def identifier_to_docs(self, identifier):
        docs = []
        if identifier.startswith('/'):
            dummy, *types, label = identifier.split('/')
            result, coll = self.repo.query({MatchObject(self.bindings.label): label, FetchObject(None): None})
            if len(result.values):
                doctf = DocProcessor(coll, self.bindings)
                docs += [doctf.value_to_doc(s) for s in result.values]
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
                result, coll = self.repo.query({"eq": resource, FetchObject(None): None})
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

    def action_export(self, filename):
        rp = self.get_rp()
        with open(filename, "w") as f:
            for q in rp.export_statements():
                f.write(json.dumps(q) + "\n")

    def action_import(self, filename):
        rp = self.get_rp()
        with open(filename, "r") as f:
            quads = []
            for line in f:
                quads.append(json.loads(line))
        rp.import_statements(quads)

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
