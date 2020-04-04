import base64
import datetime
import hashlib

from .api import CrunchyAPI
from .utility import TreeFileIterator, ApiFileIterator, CombinedIterator


class CrunchyCLIClient(object):
    """Main class for the CrunchyVicar client application."""

    def __init__(self, config):
        """Make the config available for use, and initialize the API wrapper."""
        self.config = config
        self.api = CrunchyAPI(self.config['api']['url'])
        self.schema = self.api.get_schema(self.config['schema']['root_uuid'])
        self.reverse_schema = {v: k for k, v in self.schema.items()}

    def run(self, *params):
        """Perform the action requested by the user with appropriate parameters."""
        if params[0] == 'update_volume':
            self.update_volume(params[1])

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
            else:
                relpath = str(left.relative_to(tfi.root))
                stat = left.stat()
                left_mtime = datetime.datetime.fromtimestamp(stat.st_mtime)

                if right is None:
                    new = True
                else:
                    new = False
                    right_mtime = datetime.datetime.fromisoformat(right['mtime'])

                if new or left_mtime != right_mtime or stat.st_size != right['size']:
                    with left.open('rb') as f:
                        sha256 = hashlib.sha256(f.read()).digest()
                    tf = {
                        'mtime': left_mtime.isoformat(),
                        'size': stat.st_size,
                        'lastverify': datetime.datetime.now().isoformat(),
                        'sha256': base64.b64encode(sha256).decode('utf-8'),
                        'new': new,
                    }
                    batch[relpath] = tf
                    print("NEW" if new else "CHANGED", relpath.encode('utf-8', errors='replace'))

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
