"""
Entrypoint to the discovery project
Reads a filesystem and tries to make some analysis based off of it
"""
import yaml
import logging.config
from pandas.api.types import is_numeric_dtype

from utils.metadata.metadata import Metadata, ColMetadata
from utils.metadata.metadata_json_handler import write_metadata_to_json

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


class Discovery:
    def __init__(self, discovery_config: dict):
        self.config = discovery_config
        self.file_handler = FileHandler()
        self.dataframe_file_metadata_pairs = []
        self.visualiser = Visualizer()

    def create_visual(self, pathname):
        """
        Build a visual based on stored metadata
        """

        # TODO: find a fancy python way to do this
        metadata = []
        for dataframe, metadatum in self.dataframe_file_metadata_pairs:
            metadata.append(metadatum)
        self.visualiser.draw(metadata, pathname)

    def reconstruct_metadata(self):
        """
        Builds metadata based on the currently loaded files
        """
        file_metadata = []
        for path, file_descriptor in self.file_handler.loaded_files.items():
            metadatum = self._construct_metadatum(file_descriptor)
            file_metadata.append((file_descriptor["dataframe"], metadatum))
        self.dataframe_file_metadata_pairs = file_metadata

    def _construct_metadatum(self, file_descriptor):
        metadatum = Metadata(file_descriptor["file_path"], file_descriptor["extension"],
                             file_descriptor["size"], file_descriptor["file_hash"])
        col_meta = []
        dataframe = file_descriptor["dataframe"]

        for col_name in dataframe.columns:
            column_data = self._construct_column(dataframe[col_name])
            col_meta.append(column_data)
        metadatum.columns = col_meta
        return metadatum

    def construct_relationships(self):
        for reference_dataframe, reference_metadatum in self.dataframe_file_metadata_pairs:
            for subject_dataframe, subject_metadatum in self.dataframe_file_metadata_pairs:
                if reference_metadatum.hash == subject_metadatum.hash:
                    continue
                self._match_metadata(reference_dataframe, reference_metadatum, subject_dataframe, subject_metadatum)
                print(reference_metadatum.file_path+" "+subject_metadatum.file_path)

    def _match_metadata(self, reference_dataframe, reference_metadatum, subject_dataframe, subject_metadatum):
        dataframe_matcher = DataFrameMatcher(
            reference_dataframe,
            subject_dataframe
        )
        results = dataframe_matcher.match_dataframes()
        for reference_col_name, subject_col_name, certainty in results:
            column = next(iter([x for x in reference_metadatum.columns if x.name == reference_col_name]), None)
            if column is not None:
                column.add_relationship(certainty, subject_metadatum.hash, subject_col_name)

    def _construct_column(self, column):
        data_type = self._get_column_type(column)
        average, col_min, col_max = self._get_col_statistical_values(column)
        return ColMetadata(column.name, data_type, average, col_min, col_max)

    def _get_col_statistical_values(self, column):
        average, col_min, col_max = None, None, None

        col_min = column.min()
        col_max = column.max()

        if is_numeric_dtype(column):
            average = column.mean()
        return average, col_min, col_max

    def _get_column_type(self, column):
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
    discovery_instance.construct_relationships()

    for dataframe, metadata in discovery_instance.dataframe_file_metadata_pairs:
        write_metadata_to_json(metadata)

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
