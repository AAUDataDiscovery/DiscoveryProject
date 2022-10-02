"""
Ensure that the discovery system can detect and load test data
"""
from _pytest.fixtures import fixture
from test.datagen import FakeDataGen
from discovery import Discovery


@fixture
def mock_filesystem():
    generate_data = FakeDataGen()
    new_files = generate_data.build_df_to_file(10000, path='mock_filesystem/simple_test',
                                               continuous_data=10, categoric_data=10, file_spread=5)
    return new_files


@fixture
def discovery_instance():
    return Discovery({})


def test_simple_detection(discovery_instance, mock_filesystem):
    discovery_instance.add_files("mock_filesystem")
    assert all([x in list(discovery_instance.get_loaded_files()) for x in mock_filesystem])
