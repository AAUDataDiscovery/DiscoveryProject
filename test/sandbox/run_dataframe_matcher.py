import yaml
import logging.config

from data_matching.dataframe_matcher import DataFrameMatcher
from discovery.utils.visualizer import Visualizer
from discovery import DiscoveryClient

if __name__ == "__main__":
    with open('../../discovery/logging_conf.yaml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)

    logger = logging.getLogger(__name__)

    file1_path = 'data/DailyDelhiClimateTrain.csv'
    file2_path = 'data/seattle-weather.csv'

    discovery_client = DiscoveryClient({})
    discovery_client.load_file(file1_path)
    discovery_client.load_file(file2_path)

    metadata1 = discovery_client.loaded_metadata[file1_path]
    metadata2 = discovery_client.loaded_metadata[file2_path]
    dataframe1 = next(metadata1.datagen())
    dataframe2 = next(metadata2.datagen())

    for column_name, column in metadata1.columns.items():
        best_name, best_similarity = DataFrameMatcher.match_column_in_dataframe(dataframe1, column,
                                                                                metadata2, dataframe2)
        metadata1.columns[column_name].add_relationship(best_similarity, metadata2.hash, best_name)

    # write_metadata_to_json(metadata1)

    for column_name, column in metadata2.columns.items():
        best_name, best_similarity = DataFrameMatcher.match_column_in_dataframe(dataframe2, column,
                                                                                metadata1, dataframe1)
        metadata2.columns[column_name].add_relationship(best_similarity, metadata1.hash, best_name)

    # write_metadata_to_json(metadata2)

    visualizer = Visualizer()
    visualizer.draw([metadata1, metadata2], 'output/DailyDelhiClimateTrain-seattle-weather-dataframe_matcher')
