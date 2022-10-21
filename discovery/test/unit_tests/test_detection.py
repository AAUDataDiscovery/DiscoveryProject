"""
Ensure that the discovery system can detect and load test data
"""
import os

from _pytest.fixtures import fixture
from discovery import Discovery
from utils.datagen import FakeDataGen


@fixture(scope="session")
def mock_data_folder(tmp_path_factory):
    generate_data = FakeDataGen()
    temp_path = tmp_path_factory.mktemp("mock_fs")
    generate_data.build_df_to_file(10000, path=temp_path, continuous_data=10, categoric_data=10, file_spread=5)
    return temp_path


@fixture
def discovery_instance():
    return Discovery({})


def test_simple_detection(discovery_instance, mock_data_folder):
    discovery_instance.add_files(str(mock_data_folder))
    assert all(x in list(discovery_instance.get_loaded_files()) for x in os.listdir(mock_data_folder))
