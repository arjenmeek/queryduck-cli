import base64
import datetime
import hashlib
import json
import pathlib

from collections import defaultdict
from datetime import datetime as dt

from .api import CrunchyAPI
from .utility import TreeFileIterator, ApiFileIterator, CombinedIterator
from .resource import ResourceProcessor


class CrunchyCLIClient(object):
    """Main class for the CrunchyVicar client application."""

    def __init__(self, config):
        """Make the config available for use, and initialize the API wrapper."""
        self.config = config
        self.api = CrunchyAPI(self.config['api']['url'])
        self.volume_paths = {k: pathlib.Path(v['path']) for k, v in self.config['volumes'].items()}

    def run(self, *params):
        """Perform the action requested by the user with appropriate parameters."""
        if params[0] == 'update_volume':
            self.update_volume(params[1])
        elif params[0] == 'file_info':
            self.file_info(params[1:])
        elif params[0] == 'write':
            self.write(params[1], params[2])
        elif params[0] == 'read':
            self.read(params[1])

    def update_volume(self, volume_reference):
        vcfg = self.config['volumes'][volume_reference]
        tfi = TreeFileIterator(vcfg['path'], vcfg['exclude'] if 'exclude' in vcfg else None)
        afi = ApiFileIterator(self.api, volume_reference)
        ci = CombinedIterator(tfi, afi, lambda x: str(x.relative_to(tfi.root)), lambda x: x['path'])
        batch = {}
        for left, right in ci:
            if left is None:
                print("DELETED", right['path'])
                batch[right['path']] = None
            elif right is None or changed or left.stat().st_size != right['size'] \
                    or dt.fromtimestamp(left.stat().st_mtime) != dt.fromisoformat(right['mtime']):
                relpath = str(left.relative_to(tfi.root))
                batch[relpath] = self._process_file(left)
                print("NEW" if right is None else "CHANGED",
                    relpath.encode('utf-8', errors='replace'))

            if len(batch) > 10000:
                print("Send batch...")
                self.api.mutate_files('testvol', batch)
                print("Done.")
                batch = {}
        if len(batch):
            print("Send last batch...")
            self.api.mutate_files('testvol', batch)
            print("Done.")
            batch = {}

    def _get_file_sha256(self, path):
        with path.open('rb') as f:
            sha256 = hashlib.sha256(f.read()).digest()
        return sha256

    def _process_file(self, path):
        file_info = {
            'mtime': datetime.datetime.fromtimestamp(path.stat().st_mtime).isoformat(),
            'size': path.stat().st_size,
            'lastverify': datetime.datetime.now().isoformat(),
            'sha256': base64.b64encode(self._get_file_sha256(path)).decode('utf-8'),
        }
        return file_info

    def file_info(self, paths):
        paths_info = self._process_paths(paths)
        volume_names = {p['volume_name'] for p in paths_info.values() if 'volume_name' in p}
        for volume_name in volume_names:
            volume_paths = {str(v['relative']): k for k, v in paths_info.items()
                if 'volume_name' in v and v['volume_name'] == volume_name}
            params = [('path', base64.b64encode(str(p).encode('utf-8'))) for p in volume_paths.keys()]
            r = self.api.find_files(volume_name, params=params)
            for row in r['results']:
                paths_info[volume_paths[row['path']]]['file'] = row
        for path, info in paths_info.items():
            print(path)
            if 'volume_name' in info:
                print("  found")
            else:
                print("  no matching volume found")

    def _process_paths(self, paths):
        paths_info = {}
        for path in paths:
            p = {'real': pathlib.Path(path).resolve()}
            for volume_name, volume_path in self.volume_paths.items():
                if volume_path in p['real'].parents:
                    p['volume_name'] = volume_name
                    p['volume_path'] = volume_path
                    p['relative'] = p['real'].relative_to(volume_path)
                    break
            paths_info[path] = p
        return paths_info

    def write(self, reference, filename):
        rp = ResourceProcessor(self.config, self.api)
        rp.write(reference, filename)

    def read(self, filename):
        rp = ResourceProcessor(self.config, self.api)
        rp.read(filename)
