from base64 import urlsafe_b64encode
from pathlib import Path

import yaml

from queryduck.exceptions import UserError
from queryduck.types import Blob
from queryduck.transaction import Transaction
from queryduck.utility import (
    safe_string,
    transform_doc,
)
from queryduck.storage import VolumeProcessor


from .resource import ResourceProcessor
from .utility import (
    FileAnalyzer,
)


class Volume:

    def __init__(self, name, path):
        self.name = name
        self.path = path

    def __hash__(self):
        return hash(self.name)

    def contains(self, path):
        return (self.path in path.parents)


class File:

    def __init__(self, real, volume):
        self.real = real
        self.volume = volume
        self.relative = self.real.relative_to(self.volume.path)


class StorageProcessor:

    def __init__(self, master):
        self.master = master
        self.repo = self.master.get_statement_repository()
        self.config = self.master.config
        self.api = self.master._connection
        self.volume_paths = {k: Path(v['path'])
            for k, v in self.config['volumes'].items()}
        self.volumes = [Volume(k, Path(v['path']))
            for k, v in self.config['volumes'].items()]
        self.rp = ResourceProcessor(self.master)

    def file_info(self, paths):
        files_by_path = {p: self._process_path(p) for p in paths}
        files = files_by_path.values()
        self._update_files(files)
        all_docs = self._files_to_docs(paths_info)
        print(yaml.dump_all(all_docs), end='')

    def file_edit(self, paths):
        files_by_path = {p: self._process_path(p) for p in paths}
        files = files_by_path.values()
        self._update_files(files)
        all_docs = self._files_to_docs(files)
        all_docs = self.rp.edit_docs(all_docs)
        for doc in all_docs[::-1]:
            self.rp.update_from_doc(doc)

    def file_process(self, paths):
        files_by_path = {p: self._process_path(p) for p in paths}
        files = list(files_by_path.values())
        self._update_files(files)
        self._add_file_statements(files)

        transaction = self.master.statements.transaction()
        schema = self.master.get_schema()
        sts = self.master.statements.sts
        for f in files:
            if len(f.statements) == 0:
                s = transaction.add(None, schema.type, schema.Resource)
            elif len(f.statements) == 1:
                s = f.statements[0]
            else:
                print("Can't handle more than 1 statement per file")
                continue
            transaction.ensure(s, schema.type, schema.ComputerFile)
            transaction.ensure(s, schema.label, f.real.name)
            blob = self.master.statements.sts.unique_deserialize(
                'blob:{}'.format(f.file['sha256']))
            transaction.ensure(s, schema.content, blob)
            transaction.ensure(s, schema.file_size, f.real.stat().st_size)

            main, sub = self._get_mime_type(f.real)
            current_types = sts.get_statement_attribute(s, schema.type)
            if main == 'video':
                transaction.ensure(s, schema.type, schema.VideoFile)
        print('---')
        transaction.show()
        self.master.statements.submit(transaction)

    def file_query(self, q):
        schema = self.master.get_schema()
        query = transform_doc(q, self.rp._parse_identifier)
        r = self.rp.statements.query(query=query)
        paths = []
        for st in r:
            content_sts = self.rp.statements.sts.find(s=st, p=schema.content)
            path = None
            for s in content_sts:
                if type(s.triple[2]) == Blob and s.triple[2].volume:
                    content = s.triple[2]
                    path = self.volume_paths[content.volume] / Path(content.path.decode('utf-8'))
            if path:
                paths.append(path)
        return paths

    def _add_file_statements(self, files):
        schema = self.master.get_schema()
        statements = self._find_file_statements(files)
        for f in files:
            f.statements = []
            for statement in statements:
                for v in self.statements.get_statement_attribute(statement, schema.content):
                    if (hasattr(v, 'sha256') and f.file
                            and v.encoded_sha256() == f.file['sha256']):
                        f.statements.append(statement)
                        break
        return files

    def _files_to_docs(self, files):
        schema = self.master.get_schema()
        r = self._find_file_statements(files)
        all_docs = []
        for f in files:
            docs = []
            for statement in r:
                for v in self.statements.get_statement_attribute(statement, schema.content):
                    if (hasattr(v, 'sha256') and f.file
                            and v.encoded_sha256() == f.file['sha256']):
                        doc = {'__path': str(f.real)}
                        doc.update(self.rp._value_to_doc(statement))
                        docs.append(doc)
                        break
            if not docs and f.file:
                docs.append({
                    '__path': str(f.real),
                    '__r': '/ComputerFile/{}'.format(f.real.name),
                    '_content': 'blob:{}'.format(f.file['sha256'])
                })
            all_docs += docs
        return all_docs

    def _process_path(self, path):
        real = Path(path).resolve()
        if real.is_dir():
            raise UserError("Cannot process directory: {}".format(real))
        for volume in self.volumes:
            if volume.contains(real):
                file_ = File(real, volume)
                break
        else:
            raise UserError("No volume found for {}".format(path))
        return file_

    def get_blob_by_path(self, path_str):
        path_info = self._process_path(path_str)
        self._update_volume_files(path_info['volume_name'],
            {path_str: path_info})
        blob = self.master.statements.sts.unique_deserialize(
            'blob:{}'.format(path_info['file']['sha256']))
        return blob

    def update_volume(self, volume_reference):
        vcfg = self.config['volumes'][volume_reference]
        vp = VolumeProcessor(
            self.api,
            volume_reference,
            vcfg['path'],
            vcfg['exclude'] if 'exclude' in vcfg else None,
        )
        vp.update()

    def file_options(self, path, *options):
        paths_info = {path: self._process_path(path)}
        self._update_files(paths_info)
        r = self._find_file_statements(paths_info)
        p = paths_info[path]
        attributes = {}
        for o in options:
            k, v = o.split('=', 1)
            if not k in attributes:
                attributes[k] = []
            attributes[k].append(v)
        attributes['_content'] = ['blob:{}'.format(p['file']['sha256'])]
        self.rp.update_resource(r[0] if len(r) else None, attributes)

    def _find_file_statements(self, files):
        schema = self.master.get_schema()
        obj_values = [Blob(f.file['sha256']) for f in files]
        r = self.rp.statements.query(
            query={schema.content: {'in': obj_values}})
        return r

    def _update_files(self, files):
        volumes = {f.volume for f in files}
        for volume in volumes:
            volume_files = [f for f in files if f.volume == volume]
            self._update_volume_files(volume, files)

    def _update_volume_files(self, volume, files):
        api_files = self.api.find_files(volume.name,
            [f.relative for f in files])

        batch = {}
        for f in files:
            relpath = str(f.relative)
            api_file = api_files[relpath] if relpath in api_files else None
            k, v = self._update_file_status(volume.path, f.real, api_file)
            if k:
                batch[k] = v
            f.file = v if v else None
        self._handle_file_batch(volume.name, batch, 1)

    def process_volume(self, volume_reference):
        repo = self.master.get_statement_repository()
        bindings = self.master.get_bindings()

        res = repo.query(query={}, target='blob')
        print(res.values)

        print("THE END")
        return
        root = Path(self.config['volumes'][volume_reference]['path'])
        afi = ApiFileIterator(self.api, volume_reference, without_statements=True)
        transaction = Transaction()
        fa = FileAnalyzer(bindings)
        for idx, remote in enumerate(afi):
            print(idx, remote)
            path = root / Path(remote['path'])

            resource = transaction.add(None, bindings.type, bindings.Resource)
            blob = repo.unique_deserialize('blob:{}'.format(remote['sha256']))
            transaction.ensure(resource, bindings.fileContent, blob)

            preview_hash = urlsafe_b64encode(blob.sha256).decode()
            preview_path = '{}/{}/{}.webp'.format(
                self.config['previews']['path'],
                preview_hash[0:2],
                preview_hash[2:9],
            )

            try:
                info = fa.analyze(path, preview_path)
            except:
                continue
            for k, v in info.items():
                print(bindings.reverse(k), [bindings.reverse(vv) for vv in (v if type(v) == list else [v])])
            for k, v in info.items():
                values = v if type(v) == list else [v]
                for val in values:
                    transaction.ensure(resource, k, val)

            if idx > 1000:
                break
                # TODO: Multiple chunks into multiple transactions

        transaction.show()
        repo.submit(transaction)

    def process_files(self, paths):
        b = self.master.get_bindings()
        fa = FileAnalyzer(b)
        for path in [Path(p) for p in paths]:
            if path.is_dir():
                continue
            print('***', path)
            info = fa.analyze(path)
            for k, v in info.items():
                print(b.reverse(k), [b.reverse(vv) for vv in (v if type(v) == list else [v])])
            print()
