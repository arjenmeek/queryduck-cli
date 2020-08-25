import os
import subprocess
import tempfile
import yaml
import sys

from queryduck.types import Statement, Inverted, serialize, deserialize
from queryduck.utility import transform_doc

from .utility import call_text_editor


class ResourceProcessor:

    def __init__(self, master):
        self.master = master
        self.repo = self.master.get_statement_repository()

    def update_resource(self, resource, attributes):
        s = self.master.get_schema()
        transaction = self.statements.transaction()
        if resource:
            main_ref = resource
        else:
            main_ref = transaction.add(None, s.type, s.Resource)
        for p_ref, objects in attributes.items():
            if p_ref.startswith('..'):
                continue
            if type(objects) != list:
                objects = [objects]
            p = self._parse_identifier(p_ref)
            existing_statements = self.statements.sts.find(s=resource, p=p)
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
                    st = self.statements.sts.find(s=resource, p=p, o=o)[0]
                    for pr, ob in meta_objs:
                        existing_meta_objects = self.statements.sts.find(
                            s=st, p=pr, o=ob)
                        if not existing_meta_objects:
                            transaction.add(st, pr, ob)
                    print("EXISTING", o)
                elif not o in existing_objects and not (
                        p == s.type and o == s.Resource):
                    print("NEW", o)
                    st = transaction.add(main_ref, p, o)
                    for pr, ob in meta_objs:
                        transaction.add(st, pr, ob)
        transaction.show()
        self.statements.submit(transaction)

    def update_from_doc(self, doc):
        if '..s' in doc:
            r = self.load(doc['..s'])
        elif '..r' in doc:
            r = self.load(doc['..r'])
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

    def export_statements(self):
        statements = self.statements.export_statements()
        return statements

    def import_statements(self, quads):
        self.statements.import_statements(quads)

    def query(self, q, target='statement'):
        query = transform_doc(q, self._parse_identifier)
        result = self.repo.query(query=query, target=target)
        if target == 'blob':
            for value in result.values:
                print(value)
                if value in result.files:
                    print(result.files[value])
                else:
                    print("NO VALUE")
        docs = [self._value_to_doc(st, result) for st in result.values]
        return docs

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
            statements = self.master.statements.legacy_query(*filters)
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
                '..r': reference,
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
        docs = self.edit_docs(docs)
        for doc in docs[::-1]:
            self.update_from_doc(doc)

    def edit_docs(self, docs):
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
                tmpdoc = {k: v for k, v in doc.items()
                    if not k.startswith('..')}
                transform_doc(tmpdoc, add_reference)
            for ref in references:
                for doc in docs:
                    if doc['..r'] == ref:
                        break
                else:
                    r = self.load(ref)
                    if not r:
                        docs.append(self.new(ref))
                        seen_new = True
        return docs

    def _parse_identifier(self, value):
        b = self.master.get_bindings()
        if type(value) != str:
            v = value
        elif value.startswith('.'):
            v = b[value[1:]]
        elif value.startswith('/'):
            parts = value[1:].split('/')
            filters = [b.type==b.Resource]
            for type_ in parts[:-1]:
                if type_ == 'Resource':
                    continue
                filters.append(b.type==b[type_])
            filters.append(b.label==parts[-1])
            repo = self.master.get_statement_repository()
            statements = repo.legacy_query(*filters)
            return statements[0] if len(statements) else None
        elif value.startswith('file:'):
            sp = self.master.get_sp()
            v = sp.get_blob_by_path(value[5:])
        elif ':' in value:
            repo = self.master.get_statement_repository()
            v = repo.unique_deserialize(value)
        else:
            v = value
        return v

    def _make_identifier_lazy(self, value, result):
        b = self.master.get_bindings()
        if b.reverse_exists(value):
            return ".{}".format(b.reverse(value))
        elif type(value) == Inverted and b.reverse_exists(value.value):
            return "~.{}".format(b.reverse(value.value))
        elif type(value) == Statement:
            types = [s.triple[2] for s in
                result.find(s=value, p=b.type)]
            type_elements = [b.reverse(t) for t in types if t != b.Resource]
            type_elements = list(filter(None, type_elements))
            labels = [s.triple[2] for s in
                result.find(s=value, p=b.label)]
            if len(type_elements) and len(labels):
                return '/'.join([''] + type_elements + labels[0:1])
        return value if type(value) in (str, int) else serialize(value)

    def _value_to_doc(self, r, result):
        statements = result.find(s=r)
        doc = {
            '..s': serialize(r),
            '..r': self._make_identifier_lazy(r, result),
        }
        for s in statements:
            if not s.triple[1] in doc:
                doc[s.triple[1]] = []
            if s.triple[0] != s:
                meta = result.find(s=s)
            else:
                meta = []
            if meta:
                val = {'+': s.triple[2]}
                for m in meta:
                    key = self._make_identifier_lazy(m.triple[1], result)
                    if not key in val:
                        val[key] = []
                    val[key].append(self._make_identifier_lazy(m.triple[2]), result)
                val = {k: v if k == '+' or len(v) != 1 else v[0] for k, v in val.items()}
            else:
                val = s.triple[2]
            doc[s.triple[1]].append(val)
        inverse_statements = result.find(o=r)
        for s in inverse_statements:
            inv = Inverted(s.triple[1])
            if not inv in doc:
                doc[inv] = []
            doc[inv].append(s.triple[0])
        print("INV", inverse_statements)
        doc = {k: v[0] if type(v) == list and len(v) == 1 else v
            for k, v in doc.items()}
        def my_make_identifier(v):
            return self._make_identifier_lazy(v, result)
        doc = transform_doc(doc, my_make_identifier)
        return doc

    def load(self, identifier):
        r = self._parse_identifier(identifier)
        if r:
            self.statements.get(serialize(r))
        return r

    def process_schema_template(self, tpl):
        transaction = self.statements.transaction()
        schema = {}
        schema_label = None
        for subj_s, v in tpl.items():
            if not subj_s.startswith('.'):
                print("INVALID", sub_s)
                continue
            subj = subj_s[1:]
            schema[subj] = transaction.add(None, None, None)
            if schema_label is None:
                schema_label = schema[subj]
            transaction.add(schema[subj], schema_label, subj)
        for subj_s, v in tpl.items():
            first = True
            if not subj_s.startswith('.'):
                print("INVALID subj", sub_s)
                continue
            subj = schema[subj_s[1:]]
            for pred_s, objs in v.items():
                if not pred_s.startswith('.'):
                    print("INVALID pred", pred_s)
                    continue
                pred = schema[pred_s[1:]]
                if type(objs) != list:
                    objs = [objs]
                for obj_s in objs:
                    if obj_s.startswith('.'):
                        obj = schema[obj_s[1:]]
                    else:
                        obj = obj_s
                    if first:
                        first = False
                        subj.triple = (subj, pred, obj)
                    else:
                        res = transaction.add(subj, pred, obj)
                        name = '__{}__{}__{}'.format(subj_s[1:], pred_s[1:], obj_s.lstrip('.'))
                        transaction.add(res, schema_label, name)
        transaction.show()
        result = self.statements.submit(transaction)
        return result
