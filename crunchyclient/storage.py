import datetime
import hashlib
import json
import mimetypes

from base64 import b64encode
from collections import defaultdict
from functools import partial
from datetime import datetime as dt
from pathlib import Path

import magic
import yaml

from crunchylib.exceptions import UserError
from crunchylib.types import Blob, serialize
from crunchylib.utility import transform_doc

from .resource import ResourceProcessor
from .utility import TreeFileIterator, ApiFileIterator, CombinedIterator


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
        self.statements = master.statements
        self.config = self.master.config
        self.api = self.master.api
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

    def _get_mime_type(self, path):
        mime = magic.Magic(mime=True)
        ext_type = mimetypes.guess_type(str(path))[0]
        file_type = mime.from_file(str(path))
        if ext_type != file_type:
            raise UserError("Extension and content MIME type mismatch in {}"
                .format(path))
        return file_type.split('/')[0:2]

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
        tfi = TreeFileIterator(vcfg['path'],
            vcfg['exclude'] if 'exclude' in vcfg else None)
        afi = ApiFileIterator(self.api, volume_reference)
        ci = CombinedIterator(tfi, afi,
            lambda x: str(x.relative_to(tfi.root)),
            lambda x: x['path'])
        batch = {}
        for local, remote in ci:
            k, v = self._update_file_status(tfi.root, local, remote)
            if k:
                batch[k] = v
            batch = self._handle_file_batch(volume_reference, batch, 10)
        self._handle_file_batch(volume_reference, batch, 1)

    def _handle_file_batch(self, volume_reference, batch, treshold):
        if len(batch) >= treshold:
            print("Send file batch...", end="")
            self.api.mutate_files(volume_reference, batch)
            print(" done.")
            batch = {}
        return batch

    def _update_file_status(self, root, local, remote):
        if local is None:
            print("DELETED", remote['path'])
            return remote['path'], None
        elif (remote is None
                or local.stat().st_size != remote['size']
                or dt.fromtimestamp(local.stat().st_mtime)
                    != dt.fromisoformat(remote['mtime'])
                ):
            relpath = str(local.relative_to(root))
            print("NEW" if remote is None else "CHANGED",
                relpath.encode('utf-8', errors='replace'))
            return relpath, self._process_file(local)
        else:
            return None, remote

    def _get_file_sha256(self, path):
        s = hashlib.sha256()
        with path.open('rb') as f:
            for chunk in iter(partial(f.read, 256 * 1024), b''):
                s.update(chunk)
        return s.digest()

    def _process_file(self, path):
        try:
            file_info = {
                'mtime': dt.fromtimestamp(path.stat().st_mtime).isoformat(),
                'size': path.stat().st_size,
                'lastverify': dt.now().isoformat(),
                'sha256': b64encode(self._get_file_sha256(path)).decode('utf-8'),
            }
        except PermissionError:
            print("Permission error, ignoring:", path)
            file_info = None
        return file_info

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
