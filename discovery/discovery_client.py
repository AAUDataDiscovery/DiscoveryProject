"""
Entrypoint to the discovery project
Reads a filesystem and tries to make some analysis based off of it
"""
import itertools

import yaml
import logging.config

# set up local logging before importing local libs
# TODO: do this a better way
if __name__ == "__main__":
    with open('logging_conf.yaml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)

    logger = logging.getLogger(__name__)

from discovery.utils.metadata.metadata import construct_metadata_from_file_descriptor
from discovery.utils.metadata.metadata_json_handler import write_metadata_to_json
from discovery.utils.decorators.type_enforcer import type_enforcer
from discovery.utils.dataframe_matcher import DataFrameMatcher
from discovery.utils.file_handler import FileHandler
from discovery.utils.decorators.persist_execution import persistence
from discovery.utils.visualizer import Visualizer


class DiscoveryClient:
    def __init__(self, discovery_config: dict):
        self.config = discovery_config
        self.file_handler = FileHandler()
        self.dataframe_file_metadata_pairs = []
        self.visualiser = Visualizer()
        self.dataframe_matcher = DataFrameMatcher(
            DataFrameMatcher.match_name_wordnet,
            DataFrameMatcher.match_data_pearson_coefficient
        )

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
            metadatum = construct_metadata_from_file_descriptor(file_descriptor)
            file_metadata.append((file_descriptor["dataframe"], metadatum))
        self.dataframe_file_metadata_pairs = file_metadata

    def construct_relationships(self):
        for pair in itertools.combinations(self.dataframe_file_metadata_pairs, 2):
            self._match_metadata(pair[0][0], pair[0][1], pair[1][0], pair[1][1])

    def _match_metadata(self, reference_dataframe, reference_metadatum, subject_dataframe, subject_metadatum):
        """
        Run the dataframe matcher with the two given dataframes
        Update the metadata to reflect the changes
        """
        results = self.dataframe_matcher.match_dataframes(reference_dataframe, subject_dataframe)
        for relationship in results:
            column = next((x for x in reference_metadatum.columns if x.name == relationship['column_a']), None)
            if column is not None:
                column.add_relationship(
                    relationship['data_confidence'], subject_metadatum.hash, relationship['column_b']
                )

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
    discovery_instance = DiscoveryClient(launch_config)

    from utils.datagen import FakeDataGen

    fake_data = FakeDataGen()
    fake_files = fake_data.build_df_to_file(1000, "matcher_test", index_type="categoric", continuous_data=5,
                                            file_spread=2)
    discovery_instance.add_file(fake_files[0])
    discovery_instance.add_file(fake_files[1])

    discovery_instance.reconstruct_metadata()
    discovery_instance.construct_relationships()

    for dataframe, metadata in discovery_instance.dataframe_file_metadata_pairs:
        write_metadata_to_json(metadata)

    discovery_instance.create_visual("test_visual")
