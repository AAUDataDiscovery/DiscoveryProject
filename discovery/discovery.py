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
        for path, file_descriptor in self.file_handler.loaded_files.items():
            metadatum = self.construct_metadatum(file_descriptor)
            file_metadata.append(metadatum)
        self.file_metadata = file_metadata

    def construct_metadatum(self, file_descriptor):
        metadatum = Metadata(file_descriptor.path, file_descriptor.extension, file_descriptor.size, file_descriptor.hash)
        col_meta = []
        dataframe = file_descriptor.dataframe
        for col_name in dataframe.columns:
            column_data = self.construct_column(dataframe[col_name])
            col_meta.append(column_data)
        metadatum.set_columns(col_meta)
        return metadatum

    def construct_column(self, column):
        data_type = self.get_column_type(column)
        average, col_min, col_max = self.get_col_statistical_values(column)
        return ColMetadata(column.name, data_type, average, col_min, col_max)

    def get_col_statistical_values(self, column):
        average, col_min, col_max = None, None, None

        col_min = column.min()
        col_max = column.max()

        if is_numeric_dtype(column):
            average = column.mean()
        return average, col_min, col_max

    def get_column_type(self, column):
        return column.dtype

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
    # locally test the mock filesystem
    launch_config = yaml.safe_load(open("launch_config.yaml"))
    discovery_instance = Discovery(launch_config)
    import os

    if not os.path.exists('../test/mock_filesystem'):
        raise "You forgot to create a mock filesystem!"

    discovery_instance.add_files("../test/mock_filesystem")
    discovery_instance.reconstruct_metadata()
    discovery_instance.create_visual("test_visual")

    from test.datagen import FakeDataGen

    fake_data = FakeDataGen()
    fake_files = fake_data.build_df_to_file(1000, "matcher_test", index_type="categoric", continuous_data=5,
                                            file_spread=2)
    discovery_instance.add_file(fake_files[0])
    discovery_instance.add_file(fake_files[1])

    dataframe_matcher = DataFrameMatcher(
        discovery_instance.file_handler.loaded_files[fake_files[0]],
        discovery_instance.file_handler.loaded_files[fake_files[1]])
    dataframe_matcher.match_dataframes()
