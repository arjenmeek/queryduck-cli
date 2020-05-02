import datetime
import hashlib
import json
import pathlib

from base64 import b64encode
from collections import defaultdict
from datetime import datetime as dt
from pathlib import Path

import yaml

from crunchylib.types import Blob, serialize
from crunchylib.utility import transform_doc

from .resource import ResourceProcessor
from .utility import TreeFileIterator, ApiFileIterator, CombinedIterator


class StorageProcessor:

    def __init__(self, master):
        self.master = master
        self.config = self.master.config
        self.api = self.master.api
        self.volume_paths = {k: pathlib.Path(v['path'])
            for k, v in self.config['volumes'].items()}
        self.rp = ResourceProcessor(self.master)

    def file_info(self, paths):
        paths_info = {p: self._process_path(p) for p in paths}
        self._update_files(paths_info)
        all_docs = self._files_to_docs(paths_info)
        print(yaml.dump_all(all_docs), end='')

    def file_edit(self, paths):
        paths_info = {p: self._process_path(p) for p in paths}
        self._update_files(paths_info)
        all_docs = self._files_to_docs(paths_info)
        all_docs = self.rp.edit_docs(all_docs)
        for doc in all_docs[::-1]:
            self.rp.update_from_doc(doc)

    def file_query(self, q):
        query = transform_doc(q, self.rp._parse_identifier)
        r = self.rp.statements.query(query=query)
        paths = []
        for st in r:
            content_sts = self.rp.statements.sts.find(subject=st,
                predicate=self.master.schema.content)
            path = None
            for s in content_sts:
                if type(s.triple[2]) == Blob and s.triple[2].volume:
                    content = s.triple[2]
                    path = self.volume_paths[content.volume] / pathlib.Path(content.path.decode('utf-8'))
            if path:
                paths.append(path)
        return paths

    def _files_to_docs(self, paths_info):
        r = self._find_file_statements(paths_info)
        all_docs = []
        for path, info in paths_info.items():
            docs = []
            for statement in r:
                for v in statement[self.master.schema.content]:
                    if (hasattr(v, 'sha256')
                            and 'file' in info
                            and v.encoded_sha256() == info['file']['sha256']):
                        doc = {'__path': path}
                        doc.update(self.rp._value_to_doc(statement))
                        docs.append(doc)
                        break
            if not docs and 'file' in info:
                docs.append({
                    '__path': path,
                    '__r': '/ComputerFile/{}'.format(path.split('/')[-1]),
                    '_content': 'blob:{}'.format(info['file']['sha256'])
                })
            all_docs += docs
        return all_docs

    def _process_path(self, path):
        p = {'real': pathlib.Path(path).resolve()}
        for volume_name, volume_path in self.volume_paths.items():
            if volume_path in p['real'].parents:
                p['volume_name'] = volume_name
                p['volume_path'] = volume_path
                p['relative'] = p['real'].relative_to(volume_path)
                break
        return p

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
            batch = self._handle_file_batch(volume_reference, batch, 10000)
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
        with path.open('rb') as f:
            sha256 = hashlib.sha256(f.read()).digest()
        return sha256

    def _process_file(self, path):
        file_info = {
            'mtime': dt.fromtimestamp(path.stat().st_mtime).isoformat(),
            'size': path.stat().st_size,
            'lastverify': dt.now().isoformat(),
            'sha256': b64encode(self._get_file_sha256(path)).decode('utf-8'),
        }
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

    def _find_file_statements(self, paths_info):
        obj_values = [Blob(i['file']['sha256'])
            for i in paths_info.values() if 'file' in i]
        r = self.rp.statements.query(
            query={self.master.schema.content: {'in': obj_values}})
        return r

    def _update_files(self, paths_info):
        volume_names = {pi['volume_name']
            for pi in paths_info.values() if 'volume_name' in pi}
        for volume_name in volume_names:
            volume_paths_info = {k: v for k, v in paths_info.items()
                if 'volume_name' in v and v['volume_name'] == volume_name}
            self._update_volume_files(volume_name, paths_info)

    def _update_volume_files(self, volume_name, paths_info):
        files = self.api.find_files(volume_name,
            [v['relative'] for v in paths_info.values()])

        batch = {}
        for path, info in paths_info.items():
            if not 'volume_path' in info or info['real'].is_dir():
                continue
            relpath = str(info['relative'])
            api_file = files[relpath] if relpath in files else None
            k, v = self._update_file_status(info['volume_path'],
                info['real'], api_file)
            if k:
                batch[k] = v
            if v:
                info['file'] = v
        self._handle_file_batch(volume_name, batch, 1)
