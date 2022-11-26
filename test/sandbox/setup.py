import subprocess
import os

# Manually set the tags on the existing datasets for now
DATASETS = {
    'diabetes.csv': {
        'path': 'akshaydattatraykhare/diabetes-dataset',
        'tags': ['health']
    },
    '2015.csv': {
        'path': 'unsdsn/world-happiness',
        'tags': ['arts and entertainment', 'news', 'social science',
                 'religion and belief systems']
    },
    '2016.csv': {
        'path': 'unsdsn/world-happiness',
        'tags': ['arts and entertainment', 'news', 'social science',
                 'religion and belief systems']
    },
    'world-happiness-report-2021.csv': {
        'path': 'ajaypalsinghlo/world-happiness-report-2021',
        'tags': ['health', 'religion and belief systems', 'economics']
    },
    'All_Diets.csv': {
        'path': 'thedevastator/healthy-diet-recipes-a-comprehensive-dataset',
        'tags': ['food', 'health']
    },
    'BigmacPrice.csv': {
        'path': 'vittoriogiatti/bigmacprice',
        'tags': ['food', 'economics']
    },
    'possum.csv': {
        'path': 'abrambeyer/openintro-possum',
        'tags': ['animals', 'earth and nature']
    },
    'Health_AnimalBites.csv': {
        'path': 'rtatman/animal-bites',
        'tags': ['animals', 'health']
    },
    'penguins.csv': {
        'path': 'larsen0966/penguins',
        'tags': ['animals']
    },
    'Uncleaned_WholeFoods_Sale_Data.csv': {
        'path': 'thedevastator/new-whole-foods-on-sale-product-data-collection',
        'tags': ['food', 'business']
    },
    'open_units.csv': {
        'path': 'michaelbryantds/alcohol-content-of-beer-and-cider',
        'tags': ['food']
    },
    'ramen-ratings.csv': {
        'path': 'residentmario/ramen-ratings',
        'tags': ['food']
    },
    'coffee-listings-from-all-walmart-stores.csv': {
        'path': 'dimitryzub/walmart-coffee-listings-from-500-stores',
        'tags': ['food']
    },
    'DailyDelhiClimateTrain.csv': {
        'path': 'sumanthvrao/daily-climate-time-series-data',
        'tags': ['weather and climate']
    },
    'seattle-weather.csv': {
        'path': 'ananthr1/weather-prediction',
        'tags': ['weather and climate']
    }
}


def download_datasets():
    for filename in DATASETS.keys():
        if filename not in os.listdir('data'):
            subprocess.run(f"kaggle datasets download {DATASETS[filename]['path']} -f {filename} -p data")
            print(f"{filename} downloaded")
        else:
            print(f"{filename} already found")
