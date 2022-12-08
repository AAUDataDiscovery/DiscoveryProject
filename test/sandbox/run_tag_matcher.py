import yaml
import logging.config
import os

from discovery.data_matching.matching_methods import MatchColumnNamesLevenshtein
from metadata import add_tags_to_metadata
from discovery import DiscoveryClient
from test.sandbox.setup import DATASETS
from test.sandbox.setup import download_datasets

if __name__ == "__main__":
    """
    Deduce the tags of a new dataframe by matching its columns with all other columns in the catalogue.
    """

    with open('../../discovery/logging_conf.yaml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)

    logger = logging.getLogger(__name__)

    download_datasets()
    exit()

    test_file_path = 'data/world-happiness-report-2021.csv'

    discovery_client = DiscoveryClient({})
    discovery_client.load_file(test_file_path)

    test_metadata = discovery_client.loaded_metadata[test_file_path]
    test_dataframe = next(test_metadata.datagen())

    catalogue = []

    for filename in os.listdir('data'):
        file_path = 'data/' + filename
        if test_file_path == file_path:
            continue
        discovery_client.load_file(file_path)
        catalogue_item = {
            'metadata': discovery_client.loaded_metadata[file_path],
            'dataframe': next(discovery_client.loaded_metadata[file_path].datagen())
        }
        add_tags_to_metadata(catalogue_item['metadata'], DATASETS[filename]['tags'])
        catalogue.append(catalogue_item)

    tags = {}

    for test_column_name, test_column in test_metadata.columns.items():
        columns = {}

        for item in catalogue:
            for column_name, column in item['metadata'].columns.items():
                levenshtein_matcher = MatchColumnNamesLevenshtein()
                levenshtein_percentage = \
                    levenshtein_matcher.run_process(test_column, column)

                data_type_percentage = 100 if test_column.col_type == column.col_type else 0

                continuity_percentage = 100 * (
                        1 - abs(test_column.continuity - column.continuity))

                numerical_percentage = 100 * (1 - abs(test_column.is_numeric_percentage -
                                                      column.is_numeric_percentage))

                match_percentage = (levenshtein_percentage + continuity_percentage + numerical_percentage) / 3

                columns[column.name] = match_percentage

                for tag in item['metadata'].tags:
                    if tag not in tags:
                        tags[tag] = 0
                    tags[tag] += match_percentage

        columns = dict(sorted(columns.items(), key=lambda item: item[1], reverse=True))

        print(test_column.name + ':')

        for name, percentage in columns.items():
            print('\t' + name + ' ' + str(percentage))

    tags = dict(sorted(tags.items(), key=lambda item: item[1], reverse=True))
    print()
    print()
    print(tags)
