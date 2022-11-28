"""
Entrypoint to the discovery project
Reads a filesystem and tries to make some analysis based off of it
"""
import itertools

import yaml
import logging.config
from typing import Type

# set up local logging before importing local libs
# TODO: do this a better way
if __name__ == "__main__":
    with open('logging_conf.yaml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)

    logger = logging.getLogger(__name__)

from discovery.data_matching.data_match_interface import DataMatcher
from discovery.data_matching.dataframe_matcher import DataFrameMatcher
from discovery.utils.metadata.metadata_json_handler import write_metadata_to_json
from discovery.utils.decorators.type_enforcer import type_enforcer
from discovery.utils.file_handler import FileHandler
from discovery.utils.decorators.persist_execution import persistence
from discovery.utils.visualizer import Visualizer
from discovery.data_matching.matching_methods import *


class DiscoveryClient:
    def __init__(self, discovery_config: dict):
        self.config = discovery_config
        self.file_handler = FileHandler()
        self.loaded_metadata = {}
        self.visualiser = Visualizer()
        self.dataframe_matcher = DataFrameMatcher()

    def create_visual(self, pathname):
        """
        Build a visual based on stored metadata
        """

        # TODO: find a fancy python way to do this
        metadata = []
        for metadatum in self.loaded_metadata.values():
            metadata.append(metadatum)
        self.visualiser.draw(metadata, pathname)

    def construct_relationships(self, methods: list[Type[DataMatcher]]):
        for meta1, meta2 in itertools.combinations(self.loaded_metadata.values(), 2):
            self._match_metadata(meta1, meta2, methods)

    def _match_metadata(self, origin_metadatum, target_metadatum, methods):
        """
        Run the dataframe matcher with the two given dataframes
        Update the metadata to reflect the changes
        """
        # load in full df data, as matching isn't paged for now
        origin_data = next(origin_metadatum.datagen())
        target_data = next(target_metadatum.datagen())
        for col1, col2 in itertools.product(origin_metadatum.columns.values(), target_metadatum.columns.values()):
            # results = self.dataframe_matcher.match_dataframes(reference_dataframe, subject_dataframe)
            result = self.dataframe_matcher.match_columns(
                methods=methods,
                col_meta1=col1,
                col_meta2=col2,
                series1=origin_data[col1.name],
                series2=target_data[col2.name]
            )
            # keep just the average for now
            result_average = result[1]
            col1.add_relationship(
                result_average, target_metadatum.hash, col2.name
            )

    @type_enforcer
    def load_files(self, path: str):
        for metadata_object in self.file_handler.scan_filesystem(path):
            self.loaded_metadata[metadata_object.file_path] = metadata_object

    @persistence
    @type_enforcer
    def load_file(self, path: str):
        self.loaded_metadata[path] = self.file_handler.load_file(path)


if __name__ == "__main__":
    # locally test the mock filesystem
    launch_config = yaml.safe_load(open("launch_config.yaml"))
    discovery_instance = DiscoveryClient(launch_config)

    from utils.datagen import FakeDataGen

    fake_data = FakeDataGen()
    fake_files = fake_data.build_df_to_file(1000, "matcher_test", index_type="categoric", continuous_data=5,
                                            file_spread=2)
    discovery_instance.load_file(fake_files[0])
    discovery_instance.load_file(fake_files[1])

    # discovery_instance.reconstruct_metadata()
    discovery_instance.construct_relationships(
        methods=[
            MatchColumnNamesLCS,
            MatchIdenticalColumnNames
        ]
    )

    for metadata in discovery_instance.loaded_metadata.values():
        write_metadata_to_json(metadata)

    discovery_instance.create_visual("test_visual")
