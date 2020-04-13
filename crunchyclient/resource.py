import yaml

from crunchylib.result import StatementSet, ResultSet
from crunchylib.types import Statement
from crunchylib.value import serialize, deserialize

class ResourceProcessor:

    def __init__(self, config, api):
        self.config = config
        self.api = api
        self.sts = StatementSet()
        self.schema = self._get_schema(self.config['schema']['root_uuid'])
        self.reverse_schema = {v: k for k, v in self.schema.items()}

    def _get_schema(self, root_uuid):
        schema = {}
        schema_simple = self.api.get_schema(self.config['schema']['root_uuid'])
        for k, v in schema_simple.items():
            schema[k] = self.sts.unique_deserialize(v)
        return schema

    def _find_statements(self, params):
        r = self.api.find_statements(params)
        self.sts.add(r['statements'])
        return [self.sts.get(deserialize(ref).uuid) for ref in r['references']]

    def _process_value(self, value):
        if type(value) != str:
            v = value
        elif value.startswith('_'):
            v = self.schema[value[1:]]
        elif value.startswith('/'):
            parts = value[1:].split('/')
            params = [
                ('returnstyle', 'split'),
                (serialize(self.schema['type']), serialize(self.schema['Resource'])),
            ]
            for type_ in parts[:-1]:
                if type_ == 'Resource':
                    continue
                params.append((serialize(self.schema['type']), serialize(self.schema[type_])))
            params.append((serialize(self.schema['label']), 'str:{}'.format(parts[-1])))
            statements = self._find_statements(params)
            return statements[0] if len(statements) else None
        else:
            if ':' in value:
                v = self.sts.unique_deserialize(value)
            else:
                v = value
        return v

    def _identifier_for(self, value):
        if value in self.reverse_schema:
            return "_{}".format(self.reverse_schema[value])
        elif type(value) == Statement and value[self.schema['label']]:
            types = value[self.schema['type']]
            type_elements = [self.reverse_schema[t] for t in types if t != self.schema['Resource']]
            labels = value[self.schema['label']]
            return '/'.join([''] + type_elements + labels[0:1])
        else:
            return serialize(value) if type(value) == Statement else value

    def load(self, reference):
        r = self._process_value(reference)
        if r:
            self._find_statements([('returnstyle', 'split'), ('ref', serialize(r))])
        return r


    def write(self, reference, filename):
        r = self.load(reference)
        statements = self.sts.find(subject=r)
        doc = {
            '__s': serialize(r),
            '__r': self._identifier_for(r),
        }
        for s in statements:
            p = self._identifier_for(s.triple[1])
            o = self._identifier_for(s.triple[2])
            if not p in doc:
                doc[p] = []
            doc[p].append(o)
        doc = {k: v[0] if type(v) == list and len(v) == 1 else v for k, v in doc.items()}
        with open(filename, 'w') as f:
            yaml.dump(doc, f, sort_keys=False)

    def read(self, filename):
        with open(filename, 'r') as f:
            doc = yaml.load(f, Loader=yaml.SafeLoader)
        if '__s' in doc:
            r = self.load(doc['__s'])
        elif '__r' in doc:
            r = self.load(doc['__r'])
        else:
            r = None

        new_statements = []
        if r:
            main_ref = serialize(r)
        else:
            main_ref = 0
            new_statements.append((
                main_ref,
                serialize(self.schema['type']),
                serialize(self.schema['Resource'])
            ))

        for p_ref, values in doc.items():
            if type(values) != list:
                values = [values]
            if p_ref.startswith('__'):
                continue
            p = self._process_value(p_ref)
            existing_objects = r[p] if r else []
            for v_ref in values:
                v = self._process_value(v_ref)
                if v in existing_objects:
                    print("EXISTING", v)
                elif not (p == self.schema['type'] and v == self.schema['Resource']):
                    print("NEW", v)
                    new_statements.append((
                        main_ref,
                        serialize(p),
                        serialize(v)
                    ))

        print(new_statements)
        if len(new_statements):
            self.api.create_statements(new_statements)
