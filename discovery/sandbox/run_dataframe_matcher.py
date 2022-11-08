import yaml
import logging.config

from utils.dataframe_matcher import DataFrameMatcher
from utils.file_handler import FileHandler
from discovery.utils.metadata.metadata import construct_metadata_from_file_descriptor
from discovery.utils.metadata.metadata_json_handler import write_metadata_to_json
from discovery.utils.visualizer import Visualizer

if __name__ == "__main__":
    with open('../../discovery/logging_conf.yaml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)

    logger = logging.getLogger(__name__)

    file1_path = 'data/world_happiness_report_2015.csv'
    file2_path = 'data/world_happiness_report_2016.csv'

    file_handler = FileHandler()
    file_handler.load_file(file1_path)
    file_handler.load_file(file2_path)

    file1_descriptor = file_handler.loaded_files[file1_path]
    file2_descriptor = file_handler.loaded_files[file2_path]

    metadata1 = construct_metadata_from_file_descriptor(file1_descriptor)
    metadata2 = construct_metadata_from_file_descriptor(file2_descriptor)

    dataframe1 = file1_descriptor['dataframe']
    dataframe2 = file2_descriptor['dataframe']

    file_hash1 = file1_descriptor['file_hash']
    file_hash2 = file2_descriptor['file_hash']

    for i in range(len(metadata1.columns)):
        best_name, best_similarity = DataFrameMatcher.match_column_in_dataframe(dataframe1, metadata1.columns[i],
                                                                                metadata2, dataframe2)
        metadata1.columns[i].add_relationship(best_similarity, file_hash2, best_name)

    # write_metadata_to_json(metadata1)

    for i in range(len(metadata2.columns)):
        best_name, best_similarity = DataFrameMatcher.match_column_in_dataframe(dataframe2, metadata2.columns[i],
                                                                                metadata1, dataframe1)
        metadata2.columns[i].add_relationship(best_similarity, file_hash1, best_name)

    # write_metadata_to_json(metadata2)

    visualizer = Visualizer()
    visualizer.draw([metadata1, metadata2], 'output/run_dataframe_matcher')
