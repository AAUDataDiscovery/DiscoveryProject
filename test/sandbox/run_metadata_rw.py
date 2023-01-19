import os

from data_matching.dataframe_matcher import DataFrameMatcher
from discovery import DiscoveryClient
from utils.metadata.metadata import construct_metadata_from_file_descriptor
from utils.metadata.metadata_json_reader import read_metadata_from_json
from utils.metadata.metadata_json_writer import write_metadata_to_json

filenames = []
for root, dirs, files in os.walk("/home/balazs/Downloads/test_data"):
    for filename in files:
        if filename.endswith(".csv"):
            filenames.append(root + "/" + filename)

import yaml
import logging.config

from utils.file_handler import FileHandler

if __name__ == "__main__":
    file1_path = '/home/balazs/Documents/repos/DiscoveryProject/test/mock_filesystem/new_test_0.csv'
    file2_path = '/home/balazs/Documents/repos/DiscoveryProject/test/mock_filesystem/new_test_1.csv'

    discovery_client = DiscoveryClient({})
    discovery_client.load_file(file1_path)
    discovery_client.load_file(file2_path)

    metadata1 = discovery_client.loaded_metadata[file1_path]
    metadata2 = discovery_client.loaded_metadata[file2_path]
    dataframe1 = next(metadata1.datagen())
    dataframe2 = next(metadata2.datagen())

    for column_name, column in metadata1.columns.items():
        # best_name, best_similarity = DataFrameMatcher.match_column_in_dataframe(dataframe1, column,
        #                                                                         metadata2, dataframe2)
        best_name = metadata2.columns[next(iter(metadata2.columns))].name
        best_similarity = 100
        metadata1.columns[column_name].add_relationship(best_similarity, metadata2.hash, best_name)

    # write_metadata_to_json(metadata1)

    output = write_metadata_to_json(metadata1)
    metadata1 = read_metadata_from_json(output)
    output = write_metadata_to_json(metadata1)
    metadata1 = read_metadata_from_json(output)
    output = write_metadata_to_json(metadata1)
    metadata1 = read_metadata_from_json(output)
    print("end")