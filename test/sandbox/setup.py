import subprocess
import os

# Manually set the tags on the existing datasets for now
DATASETS = {
    'diabetes.csv': {
        'path': 'akshaydattatraykhare/diabetes-dataset',
        'tags': ['health']
    },
    'unsdg_2002_2021.csv': {
        'path': 'vittoriogiatti/unsdg-united-nations-sustainable-development-group',
        'tags': ['economics', 'government']
    },
    '2015.csv': {
        'path': 'unsdsn/world-happiness',
        'tags': ['arts and entertainment', 'news', 'social science',
                 'religion and belief systems', 'economics']
    },
    '2016.csv': {
        'path': 'unsdsn/world-happiness',
        'tags': ['arts and entertainment', 'news', 'social science',
                 'religion and belief systems', 'economics']
    },
    'world-happiness-report-2021.csv': {
        'path': 'ajaypalsinghlo/world-happiness-report-2021',
        'tags': ['health', 'healthcare', 'religion and belief systems', 'economics']
    },
}


def download_datasets():
    for filename in DATASETS.keys():
        if filename not in os.listdir('data'):
            subprocess.run(f"kaggle datasets download {DATASETS[filename]['path']} -f {filename} -p data")
            print(f"{filename} downloaded")
        else:
            print(f"{filename} already found")
