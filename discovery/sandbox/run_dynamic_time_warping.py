import yaml
import logging.config
from dtaidistance import dtw
from dtaidistance import dtw_visualisation as dtwvis

from utils.file_handler import FileHandler
from utils.datagen import FakeDataGen
from utils.dataframe_matcher import DataFrameMatcher

if __name__ == "__main__":
    with open('../logging_conf.yaml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)

    logger = logging.getLogger(__name__)

    file_handler = FileHandler()
    fake_data = FakeDataGen()

    fake_files = fake_data.build_df_to_file(1000, path="output/run_dataframe_matcher", index_type="counter",
                                            continuous_data=3, categoric_data=0, file_spread=2)
    file_handler.load_file(fake_files[0])
    file_handler.load_file(fake_files[1])

    df1 = file_handler.loaded_files[fake_files[0]]
    df2 = file_handler.loaded_files[fake_files[1]]

    for column1 in df1.columns:
        if column1 == 'Unnamed: 0':
            continue

        for column2 in df2.columns:
            distance = DataFrameMatcher.match_data_dynamic_time_warping(df1.loc[:, column1], df2.loc[:, column2])
            logger.debug(
                f"{column1} {column2} {distance}")
