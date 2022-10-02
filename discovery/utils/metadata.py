"""
Data storage in memory
"""
import pandas


class Metadata:
    filepath: str
    dataframe: pandas.DataFrame


    def __init__(self, filepath, dataframe):
        self.filepath = filepath
        self.dataframe = dataframe
