import os
import subprocess
import tempfile
import yaml
import sys

from crunchylib.repository import NewStatementList
from crunchylib.result import StatementSet, ResultSet
from crunchylib.types import Statement, Placeholder, serialize, deserialize
from crunchylib.utility import transform_doc

from .utility import call_text_editor

class ResourceProcessor:

    def __init__(self, master):
        self.master = master
        self.config = self.master.config
        self.api = self.master.api
        self.statements = self.master.statements

    def update_resource(self, resource, attributes):
        s = self.master.schema
        new_statements = NewStatementList()
        if resource:
            main_ref = resource
        else:
            main_ref = new_statements.add(None, s.type, s.Resource)
        for p_ref, objects in attributes.items():
            if p_ref.startswith('__'):
                continue
            if type(objects) != list:
                objects = [objects]
            p = self._parse_identifier(p_ref)
            existing_statements = self.statements.sts.find(subject=resource, predicate=p)
            existing_objects = [s.triple[2] for s in existing_statements]
            for o_ref in objects:
                if type(o_ref) == dict and '+' in o_ref:
                    o = self._parse_identifier(o_ref['+'])
                    meta_objs = []
                    for k, v in o_ref.items():
                        if type(v) != list:
                            v = [v]
                        if k == '+':
                            continue
                        kk = self._parse_identifier(k)
                        meta_objs += [(kk, vv) for vv in v]
                else:
                    o = self._parse_identifier(o_ref)
                    meta_objs = []
                if o in existing_objects:
                    #st = existing_objects[o]
                    st = self.statements.sts.find(subject=resource, predicate=p, object_=o)[0]
                    for pr, ob in meta_objs:
                        existing_meta_objects = self.statements.sts.find(subject=st, predicate=pr, object_=ob)
                        if not existing_meta_objects:
                            new_statements.add(st, pr, ob)
                    print("EXISTING", o)
                elif not o in existing_objects and not (
                        p == s.type and o == s.Resource):
                    print("NEW", o)
                    st = new_statements.add(main_ref, p, o)
                    for pr, ob in meta_objs:
                        new_statements.add(st, pr, ob)
        new_statements.show()
        self.master.statements.create(new_statements)

    def update_from_doc(self, doc):
        if '__s' in doc:
            r = self.load(doc['__s'])
        elif '__r' in doc:
            r = self.load(doc['__r'])
        else:
            r = None

        return self.update_resource(r, doc)

    def describe(self, resource):
        doc = self._value_to_doc(resource)
        print(yaml.dump(doc), end='')

    def read(self, filename):
        with open(filename, 'r') as f:
            docs = yaml.load_all(f, Loader=yaml.SafeLoader)
            for doc in docs:
                self.update_from_doc(doc)

    def write(self, filename, *references):
        docs = [self._value_to_doc(self.load(ref))
            for ref in references]
        with open(filename, 'w') as f:
            yaml.dump_all(docs, f, sort_keys=False)

    def output(self, *references):
        docs = [self._value_to_doc(self.load(ref))
            for ref in references]
        print(yaml.dump_all(docs, sort_keys=False), end='')

    def query(self, *filter_strings):
        s = self.master.schema
        filters = []
        for fs in filter_strings:
            k_str, v_str = fs.split('=', 1)
            k = self._parse_identifier(k_str)
            v = self._parse_identifier(v_str)
            filters.append(k==v)
        r = self.master.statements.query(*filters)
        for st in r:
            blobs = st[s.content]
            print(st, blobs[0].path if len(blobs) else None)

    def do_query(self, querystr):
        if querystr == '-':
            q = yaml.load(sys.stdin, Loader=yaml.SafeLoader)
        else:
            q = yaml.load(querystr, Loader=yaml.SafeLoader)
        query = transform_doc(q, self._parse_identifier)
        r = self.statements.query(query=query)
        docs = [self._value_to_doc(st) for st in r]
        print(yaml.dump_all(docs, sort_keys=False), end='')

    def set(self, *params):
        filters = []
        changes = {}
        identifier = None
        for param in params:
            if '==' in param:
                k_str, v_str = param.split('==', 1)
                k = self._parse_identifier(k_str)
                v = self._parse_identifier(v_str)
                filters.append(k==v)
            elif '=' in param:
                k, v = param.split('=', 1)
                if not k in changes:
                    changes[k] = []
                changes[k].append(v)
            else:
                identifier = param
        if identifier:
            statement = self._parse_identifier(identifier)
        else:
            statements = self.master.statements.query(*filters)
            if len(statements) > 1:
                print("TOO MANY")
                return
            statement = statements[0]
        self.describe(statement)
        self.update_resource(statement, changes)

    def new(self, reference):
        if reference.startswith('/'):
            parts = reference[1:].split('/')
            doc = {
                '__r': reference,
                '_type': ['_Resource'] + ['_' + t for t in parts[:-1]
                    if t != 'Resource'],
                '_label': parts[-1]
            }
        return doc

    def edit(self, *references):
        docs = []
        for ref in references:
            r = self.load(ref)
            if r:
                docs.append(self._value_to_doc(r))
            else:
                docs.append(self.new(ref))

        seen_new = True
        while seen_new:
            text = yaml.dump_all(docs, sort_keys=False)
            text = call_text_editor(text)
            seen_new = False
            docs = list(yaml.load_all(text, Loader=yaml.SafeLoader))
            references = []
            def add_reference(ref):
                if type(ref) == str and ref.startswith('/'):
                    references.append(ref)
                return ref
            for doc in docs:
                transform_doc(docs, add_reference)
            for ref in references:
                for doc in docs:
                    if doc['__r'] == ref:
                        break
                else:
                    r = self.load(ref)
                    if not r:
                        docs.append(self.new(ref))
                        seen_new = True

        for doc in docs[::-1]:
            self.update_from_doc(doc)

    def _parse_identifier(self, value):
        s = self.master.schema
        if type(value) != str:
            v = value
        elif value.startswith('_'):
            v = s[value[1:]]
        elif value.startswith('/'):
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

    def _make_identifier_lazy(self, value):
        s = self.master.schema
        if s.reverse(value):
            return "_{}".format(s.reverse(value))
        elif type(value) == Statement:
            types = [s.triple[2] for s in
                self.statements.sts.find(subject=value, predicate=s.type)]
            type_elements = [s.reverse(t) for t in types if t != s.Resource]
            type_elements = list(filter(None, type_elements))
            labels = [s.triple[2] for s in
                self.statements.sts.find(subject=value, predicate=s.label)]
            if len(type_elements) and len(labels):
                return '/'.join([''] + type_elements + labels[0:1])
        return value if type(value) in (str, int) else serialize(value)

    def _make_identifier(self, value):
        s = self.master.schema
        if s.reverse(value):
            return "_{}".format(s.reverse(value))
        elif type(value) == Statement and \
                self.statements.get(serialize(value)) and value[s.label]:
            types = value[s.type]
            type_elements = [s.reverse(t) for t in types if t != s.Resource]
            type_elements = list(filter(None, type_elements))
            labels = value[s.label]
            if len(type_elements) and len(labels):
                return '/'.join([''] + type_elements + labels[0:1])
        return value if type(value) in (str, int) else serialize(value)

    def _value_to_doc(self, r):
        statements = self.statements.sts.find(subject=r)
        doc = {
            '__s': serialize(r),
            '__r': self._make_identifier_lazy(r),
        }
        for s in statements:
            if not s.triple[1] in doc:
                doc[s.triple[1]] = []
            if s.triple[0] != s:
                meta = self.statements.sts.find(subject=s)
            else:
                meta = []
            if meta:
                val = {'+': s.triple[2]}
                for m in meta:
                    key = self._make_identifier(m.triple[1])
                    if not key in val:
                        val[key] = []
                    val[key].append(self._make_identifier(m.triple[2]))
                val = {k: v if k == '+' or len(v) != 1 else v[0] for k, v in val.items()}
            else:
                val = s.triple[2]
            doc[s.triple[1]].append(val)
        doc = {k: v[0] if type(v) == list and len(v) == 1 else v
            for k, v in doc.items()}
        doc = transform_doc(doc, self._make_identifier_lazy)
        return doc

    def load(self, identifier):
        r = self._parse_identifier(identifier)
        if r:
            self.statements.get(serialize(r))
        return r
