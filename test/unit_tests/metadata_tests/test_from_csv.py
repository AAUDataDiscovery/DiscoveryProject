"""
Ensure that the read csv metadata builder is functioning
"""
import os

from _pytest.fixtures import fixture
from discovery.utils.datagen import FakeDataGen
import metadata


@fixture(scope="session")
def mock_data_folder(tmp_path_factory):
    generate_data = FakeDataGen()
    temp_path = tmp_path_factory.mktemp("mock_fs")
    generate_data.build_df_to_file(10000, path=temp_path, continuous_data=10, categoric_data=10, file_spread=5)
    return temp_path


@fixture(scope="session")
def mock_data_file(tmp_path_factory):
    generate_data = FakeDataGen()
    temp_path = tmp_path_factory.mktemp("mock_fs")
    generate_data.build_df_to_file(10000, path=temp_path, continuous_data=10, categoric_data=10, file_spread=1)
    return os.listdir(temp_path)[0]


# def test_read_small_csv():
    # builder = metadata.from_csv("/home/mii/PycharmProjects/DiscoveryProject/discovery/matcher_test_0.csv")
    # builder.get_data()
    # builder.rebuild_metadata_object()
    # for filename in os.listdir(mock_data_folder):
    #     builder = metadata.from_csv(os.path.join(mock_data_folder, filename))
    # discovery_instance.load_files(str(mock_data_folder))
    # assert all(x in list(discovery_instance.get_loaded_files()) for x in os.listdir(mock_data_folder))
