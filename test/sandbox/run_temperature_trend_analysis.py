import yaml
import logging.config
import time

from discovery import DiscoveryClient
from discovery.data_matching.matching_methods import MatchDataTwoSampleTTest
from discovery.data_matching.matching_methods import MatchDataDynamicTimeWarping

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

    print()
    print("Is the unknown mean of the column 'meantemp' equal to that of 'temp_max'? (Two-Sample t-Test)")

    series1 = dataframe1.loc[:, 'meantemp']
    series2 = dataframe2.loc[:, 'temp_max']

    start_time = time.time()
    p_value = MatchDataTwoSampleTTest().run_process(series1, series2)
    end_time = time.time()

    print(("Yes" if p_value > 0.05 else "No") + f" (p-value = {p_value})")
    print(f"Runtime: {round((end_time - start_time) * 1000, 2)} ms")
    print()

    print("How similar are the trends followed by 'meantemp' and 'temp_max'? (Dynamic Time Warping)")

    start_time = time.time()
    print(f"{MatchDataDynamicTimeWarping().run_process(series1, series2)}%")
    end_time = time.time()
    print(f"Runtime: {round((end_time - start_time) * 1000, 2)} ms")
    print()

    print("What about 'meantemp' and 'precipitation'? (Dynamic Time Warping)")

    series2 = dataframe2.loc[:, 'precipitation']

    start_time = time.time()
    print(f"{round(MatchDataDynamicTimeWarping().run_process(series1, series2), 2)}%")
    end_time = time.time()
    print(f"Runtime: {round((end_time - start_time) * 1000, 2)} ms")
    print()
