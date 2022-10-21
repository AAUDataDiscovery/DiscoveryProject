"""
Entrypoint to the discovery project
Reads a filesystem and tries to make some analysis based off of it
"""
import yaml
import logging.config
from pandas.api.types import is_numeric_dtype

# set up local logging before importing local libs
# TODO: do this a better way
if __name__ == "__main__":
    with open('logging_conf.yaml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)

    logger = logging.getLogger(__name__)

from utils.decorators.type_enforcer import type_enforcer
from utils.dataframe_matcher import DataFrameMatcher
from utils.file_handler import FileHandler
from utils.decorators.persist_execution import persistence
from utils.visualizer import Visualizer
from utils.metadata import Metadata, ColMetadata


class Discovery:
    def __init__(self, discovery_config: dict):
        self.config = discovery_config
        self.file_handler = FileHandler()
        self.file_metadata = []
        self.visualiser = Visualizer()

    def create_visual(self, pathname):
        """
        Build a visual based on stored metadata
        """
        self.visualiser.draw(self.file_metadata, pathname)

    def reconstruct_metadata(self):
        """
        Builds metadata based on the currently loaded files
        """
        file_metadata = []
        for path, dataframe in self.file_handler.loaded_files.items():
            col_meta = []
            for col_name in dataframe.columns:
                average, col_min, col_max = None, None, None
                if is_numeric_dtype(dataframe[col_name]):
                    average = dataframe[col_name].mean()
                    col_min = dataframe[col_name].min()
                    col_max = dataframe[col_name].max()
                col_meta.append(ColMetadata(col_name, dataframe[col_name].dtype, average, col_min, col_max))

            file_metadata.append(Metadata(path, col_meta))
        self.file_metadata = file_metadata

    def get_loaded_files(self):
        return self.file_handler.loaded_files

    @type_enforcer
    def add_files(self, path: str, reconstruct=False):
        self.file_handler.scan_filesystem(path)
        if reconstruct:
            self.reconstruct_metadata()

    @persistence
    @type_enforcer
    def add_file(self, path: str, reconstruct=False):
        self.file_handler.load_file(path)
        if reconstruct:
            self.reconstruct_metadata()


if __name__ == "__main__":
    pass
