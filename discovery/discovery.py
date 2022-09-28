"""
Entrypoint to the discovery project
Reads a filesystem and tries to make some analysis based off of it
"""
import yaml
import logging.config

from utils.file_handler import FileHandler


class Discovery:
    def __init__(self, discovery_config: dict):
        self.config = discovery_config
        self.file_handler = FileHandler()

    def add_files(self, path):
        self.file_handler.scan_filesystem(path)

    def add_file(self, path):
        self.file_handler.load_file(path)


if __name__ == "__main__":
    # set up local logging
    with open('logging_conf.yaml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)

    logger = logging.getLogger(__name__)

    # locally test the mock filesystem
    launch_config = yaml.safe_load(open("launch_config.yaml"))
    discovery_instance = Discovery(launch_config)
    discovery_instance.add_files("../test/mock_filesystem")

    discovery_instance.add_file("../test/mock_filesystem/subdir1/weather_data_part1.cs")
