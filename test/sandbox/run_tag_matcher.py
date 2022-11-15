import yaml
import logging.config
import os

from utils.dataframe_matcher import DataFrameMatcher
from utils.file_handler import FileHandler
from discovery.utils.metadata.metadata import construct_metadata_from_file_descriptor
from discovery.utils.metadata.metadata_json_handler import write_metadata_to_json
from discovery.utils.visualizer import Visualizer

if __name__ == "__main__":
    """
    Deduce the tags of a new dataframe by matching its columns with all other columns in the catalogue.
    """

    with open('../../discovery/logging_conf.yaml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)

    logger = logging.getLogger(__name__)

    test_file_path = 'data/world_happiness_report_2021.csv'

    file_handler = FileHandler()
    file_handler.load_file(test_file_path)

    test_file_descriptor = file_handler.loaded_files[test_file_path]
    test_metadata = construct_metadata_from_file_descriptor(test_file_descriptor)
    test_dataframe = test_file_descriptor['dataframe']
    test_file_hash = test_file_descriptor['file_hash']

    catalogue = []

    for filename in os.listdir('data'):
        if test_file_path == 'data/' + filename:
            continue
        file_handler.load_file('data/' + filename)
        file_descriptor = file_handler.loaded_files['data/' + filename]
        catalogue.append({
            'metadata': construct_metadata_from_file_descriptor(file_descriptor),
            'dataframe': file_descriptor['dataframe']
        })

    tags = {}

    for i in range(len(test_metadata.columns)):
        columns = {}

        for item in catalogue:
            for j in range(len(item['metadata'].columns)):
                levenshtein_percentage = DataFrameMatcher.match_name_levenshtein(test_metadata.columns[i].name,
                                                                                 item['metadata'].columns[j].name)
                data_type_percentage = 100 if test_metadata.columns[i].col_type == item['metadata'].columns[
                    j].col_type else 0

                continuity_percentage = 100 * (
                        1 - abs(test_metadata.columns[i].continuity - item['metadata'].columns[j].continuity))

                numerical_percentage = 100 * (1 - abs(test_metadata.columns[i].is_numeric_percentage -
                                                      item['metadata'].columns[j].is_numeric_percentage))

                match_percentage = (levenshtein_percentage + continuity_percentage + numerical_percentage) / 3

                columns[item['metadata'].columns[j].name] = match_percentage

                for tag in item['metadata'].tags:
                    if tag not in tags:
                        tags[tag] = 0
                    tags[tag] += match_percentage

        columns = dict(sorted(columns.items(), key=lambda item: item[1], reverse=True))

        print(test_metadata.columns[i].name + ':')

        for name, percentage in columns.items():
            print('\t' + name + ' ' + str(percentage))

    tags = dict(sorted(tags.items(), key=lambda item: item[1], reverse=True))
