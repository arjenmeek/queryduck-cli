from .api import CrunchyAPI
from .resource import ResourceProcessor
from .storage import StorageProcessor


class CrunchyCLIClient(object):
    """Main class for the CrunchyVicar client application."""

    def __init__(self, config):
        """Make the config available and initialize the API wrapper."""
        self.config = config
        self.api = CrunchyAPI(self.config['api']['url'])

    def run(self, *params):
        """Perform the action requested by the user"""
        method = getattr(self, 'action_{}'.format(params[0]))
        return method(*params[1:])

    def action_update_volume(self, volume_reference):
        sp = StorageProcessor(self.config, self.api)
        return sp.update_volume(volume_reference)

    def action_file_info(self, *paths):
        sp = StorageProcessor(self.config, self.api)
        return sp.file_info(paths)

    def action_write(self, reference, filename):
        rp = ResourceProcessor(self.config, self.api)
        return rp.write(reference, filename)

    def action_read(self, filename):
        rp = ResourceProcessor(self.config, self.api)
        return rp.read(filename)
