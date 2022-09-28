"""
Entrypoint to the discovery project
Reads a filesystem and tries to make some analysis based off of it
"""
import yaml
import logging.config

from utils.decorators.type_enforcer import type_enforcer

# set up local logging before importing local libs
with open('logging_conf.yaml', 'r') as f:
    config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)

logger = logging.getLogger(__name__)

from utils.file_handler import FileHandler
from utils.decorators.persist_execution import persistence


class Discovery:
    def __init__(self, discovery_config: dict):
        self.config = discovery_config
        self.file_handler = FileHandler()

    @type_enforcer
    def add_files(self, path: str):
        self.file_handler.scan_filesystem(path)

    @persistence
    @type_enforcer
    def add_file(self, path: str):
        self.file_handler.load_file(path)


if __name__ == "__main__":
    # locally test the mock filesystem
    launch_config = yaml.safe_load(open("launch_config.yaml"))
    discovery_instance = Discovery(launch_config)
    import os

    if not os.path.exists('../test/mock_filesystem'):
        raise "You forgot to create a mock filesystem!"

    discovery_instance.add_files("../test/mock_filesystem")
