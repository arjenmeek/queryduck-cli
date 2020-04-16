import os
import subprocess
import tempfile
import yaml


from crunchylib.repository import NewStatementList
from crunchylib.result import StatementSet, ResultSet
from crunchylib.types import Statement, Placeholder, serialize, deserialize


class ResourceProcessor:

    def __init__(self, master):
        self.master = master
        self.config = self.master.config
        self.api = self.master.api
        self.sp = self.master.get_sp()

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
            v = self.sp.get_blob_by_path(value[5:])
        else:
            if ':' in value:
                v = self.master.statements.sts.unique_deserialize(value)
            else:
                v = value
        return v

    def _make_identifier(self, value):
        s = self.master.schema
        if s.reverse(value):
            return "_{}".format(s.reverse(value))
        elif type(value) == Statement and value[s.label]:
            types = value[s.type]
            type_elements = [s.reverse(t) for t in types if t != s.Resource]
            labels = value[s.label]
            return '/'.join([''] + type_elements + labels[0:1])
        else:
            return value if type(value) in (str, int) else serialize(value)

    def value_to_doc(self, r):
        statements = self.master.statements.sts.find(subject=r)
        doc = {
            '__s': serialize(r),
            '__r': self._make_identifier(r),
        }
        for s in statements:
            p = self._make_identifier(s.triple[1])
            o = self._make_identifier(s.triple[2])
            if not p in doc:
                doc[p] = []
            doc[p].append(o)
        doc = {k: v[0] if type(v) == list and len(v) == 1 else v
            for k, v in doc.items()}
        return doc

    def load(self, identifier):
        r = self._parse_identifier(identifier)
        if r:
            self.master.statements.get(serialize(r))
        return r

    def update_resource(self, resource, attributes):
        s = self.master.schema
        new_statements = NewStatementList()
        if resource:
            main_ref = resource
        else:
            main_ref = new_statements.append((main_ref, s.type, s.Resource))
        for p_ref, objects in attributes.items():
            if p_ref.startswith('__'):
                continue
            if type(objects) != list:
                objects = [objects]
            p = self._parse_identifier(p_ref)
            existing_objects = resource[p] if resource else []
            for o_ref in objects:
                o = self._parse_identifier(o_ref)
                if o in existing_objects:
                    print("EXISTING", o)
                elif not o in existing_objects and not (
                    p == s.type and o == s.Resource):
                    print("NEW", o)
                    new_statements.add(main_ref, p, o)
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
        doc = self.value_to_doc(resource)
        print(yaml.dump(doc), end='')

    def read(self, filename):
        with open(filename, 'r') as f:
            docs = yaml.load_all(f, Loader=yaml.SafeLoader)
            for doc in docs:
                self.update_from_doc(doc)

    def write(self, filename, *references):
        docs = [self.value_to_doc(self.load(ref))
            for ref in references]
        with open(filename, 'w') as f:
            yaml.dump_all(docs, f, sort_keys=False)

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

    def _edit_text(self, text):
        editor = os.environ.get('EDITOR','vim')
        fd, fname = tempfile.mkstemp(suffix=".tmp")
        with os.fdopen(fd, 'w') as f:
            f.write(text)
            f.close()

        before = os.path.getmtime(fname)
        first = True
        while first or (os.path.getmtime(fname) == before
                and input("File unchanged, [r]eopen or [c]ontinue? ") != 'c'):
            subprocess.call([editor, fname])
            first = False
        with open(fname, 'r') as f:
            result = f.read()
        os.unlink(fname)
        return result

    def edit(self, *references):

        docs = [self.value_to_doc(self.load(ref))
            for ref in references]
        text = yaml.dump_all(docs, sort_keys=False)
        result = self._edit_text(text)
        docs = yaml.load_all(result, Loader=yaml.SafeLoader)
        for doc in docs:
            self.update_from_doc(doc)
