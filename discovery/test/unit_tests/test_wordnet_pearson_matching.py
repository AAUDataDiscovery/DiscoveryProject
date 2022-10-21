"""
Ensure that the WordNet and Pearson matching algorithms complete without errors
"""
import os
import logging.config

from _pytest.fixtures import fixture
from discovery import Discovery
from utils.datagen import FakeDataGen
from utils.dataframe_matcher import DataFrameMatcher

logger = logging.getLogger(__name__)


@fixture
def discovery_instance():
    return Discovery({})


def test_wordnet_pearson_matching(discovery_instance, tmp_path_factory):
    fake_data = FakeDataGen()
    temp_path = tmp_path_factory.mktemp("mock_fs")
    fake_files = fake_data.build_df_to_file(100000, path=temp_path, index_type="counter",
                                            continuous_data=10, file_spread=2)
    discovery_instance.add_file(fake_files[0])
    discovery_instance.add_file(fake_files[1])

    dataframe_matcher = DataFrameMatcher(DataFrameMatcher.match_name_wordnet,
                                         DataFrameMatcher.match_data_pearson_coefficient)
    logger.debug(dataframe_matcher.match_dataframes(discovery_instance.file_handler.loaded_files[fake_files[0]],
                                                    discovery_instance.file_handler.loaded_files[fake_files[1]]))
