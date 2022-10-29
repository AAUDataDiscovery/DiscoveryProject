import numpy as np
import yaml
import logging.config
import plotly.express as px
import plotly

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
    df = file_handler.loaded_files[fake_files[0]]['dataframe']

    for column in df.columns:
        # Calculate the probability that the column is a sine wave
        resolution = len(df.loc[:, column])
        probability = 0
        normalized_series = df.loc[:, column] / np.max(np.abs(df.loc[:, column]), axis=0)
        # We will test with 10, 50, and 100 datapoints per cycle
        for datapoints_per_cycle in [10, 50, 100]:
            cycles = resolution / datapoints_per_cycle
            length = np.pi * 2 * cycles
            sin_wave = np.sin(np.arange(0, length, length / resolution))
            # fig = px.line(sin_wave, title='Whatever')
            # plotly.offline.plot(fig)
            distance = DataFrameMatcher.match_data_dynamic_time_warping(sin_wave, normalized_series)
            logger.debug(distance)
        logger.debug(f"{column} {probability / 3}")
