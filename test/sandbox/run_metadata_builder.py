import yaml
import logging.config

from discovery.utils.visualizer import Visualizer
from discovery import DiscoveryClient
from discovery.utils.metadata.metadata import add_tags_to_metadata
from test.sandbox.setup import DATASETS
from test.sandbox.setup import download_datasets

if __name__ == "__main__":
    with open('../../discovery/logging_conf.yaml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)

    logger = logging.getLogger(__name__)

    download_datasets()

    file_path = 'data/seattle-weather.csv'

    discovery_client = DiscoveryClient({})
    discovery_client.load_file(file_path)

    metadata = discovery_client.loaded_metadata[file_path]
    dataframe = next(metadata.datagen())
    add_tags_to_metadata(metadata, DATASETS['seattle-weather.csv']['tags'])

    # write_metadata_to_json(metadata)

    visualizer = Visualizer()
    visualizer.draw([metadata], 'output/seattle-weather-metadata')
