import yaml
import logging.config

from utils.dataframe_matcher import DataFrameMatcher
from utils.file_handler import FileHandler
from utils.datagen import FakeDataGen


if __name__ == "__main__":
    with open('../logging_conf.yaml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)

    logger = logging.getLogger(__name__)

    file_handler = FileHandler()
    fake_data = FakeDataGen()

    fake_files = fake_data.build_df_to_file(100000, path="output/run_dataframe_matcher", index_type="counter",
                                            continuous_data=10, file_spread=2)
    file_handler.load_file(fake_files[0])
    file_handler.load_file(fake_files[1])

    dataframe_matcher = DataFrameMatcher(DataFrameMatcher.match_name_wordnet,
                                         DataFrameMatcher.match_data_pearson_coefficient,
                                         logger)
    dataframe_matcher.match_dataframes(file_handler.loaded_files[fake_files[0]],
                                       file_handler.loaded_files[fake_files[1]])
