from .api import CrunchyAPI
from .resource import ResourceProcessor
from .storage import StorageProcessor


class CrunchyCLIClient(object):
    """Main class for the CrunchyVicar client application."""

    def __init__(self, config):
        """Make the config available for use, and initialize the API wrapper."""
        self.config = config
        self.api = CrunchyAPI(self.config['api']['url'])

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

    def file_info(self, paths):
        sp = StorageProcessor(self.config, self.api)
        sp.file_info(paths)

    def write(self, reference, filename):
        rp = ResourceProcessor(self.config, self.api)
        rp.write(reference, filename)

    def read(self, filename):
        rp = ResourceProcessor(self.config, self.api)
        rp.read(filename)
