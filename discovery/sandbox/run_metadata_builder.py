import yaml
import logging.config

from utils.dataframe_matcher import DataFrameMatcher
from utils.file_handler import FileHandler
from discovery.utils.metadata.metadata import construct_metadata_from_file_descriptor
from discovery.utils.metadata.metadata_json_handler import write_metadata_to_json
from discovery.utils.visualizer import Visualizer

if __name__ == "__main__":
    with open('../logging_conf.yaml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)

    logger = logging.getLogger(__name__)

    file_path = 'output/unsdg_2002_2021.csv'

    file_handler = FileHandler()
    file_handler.load_file(file_path)

    dataframe_name = file_path[file_path.rindex('/') + 1:]
    dataframe_name = dataframe_name[:dataframe_name.rfind(".")]

    file_descriptor = file_handler.loaded_files[file_path]
    metadatum = construct_metadata_from_file_descriptor(file_descriptor)

    dataframe = file_descriptor['dataframe']
    file_hash = file_descriptor['file_hash']
    for i in range(len(metadatum.columns)):
        for column_name in dataframe.columns:
            column = dataframe.loc[:, column_name]
            metadatum.columns[i].add_relationship(
                DataFrameMatcher.match_columns(dataframe, metadatum.columns[i].name, dataframe, column_name),
                file_hash,
                column_name)

    write_metadata_to_json(metadatum)

    visualizer = Visualizer()
    visualizer.draw([metadatum], 'output/run_metadata_builder')
