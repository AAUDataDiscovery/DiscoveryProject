"""
Data storage in memory
"""
import xml
from typing import Optional, Union
from dicttoxml import dicttoxml

import pandas

from utils.metadata_enums import FileSizeUnit, FileExtension


class Relationship:
    certainty: int
    target_file_hash: int
    target_column_name: str

    def __init__(self, certainty, target_file_hash, target_column_name):
        self.certainty = certainty
        self.target_file_hash = target_file_hash
        self.target_column_name = target_column_name


class ColMetadata:
    name: str
    col_type: str
    mean: Union[int, float, None]
    min: any
    max: any
    columns: any  # [ColMetadata]
    relationships: [Relationship]

    def __init__(self, name: str, col_type: str, mean: Union[int, float, None], min_val, max_val, columns=None):
        self.name = name
        self.col_type = col_type
        self.mean = mean
        self.min = min_val
        self.max = max_val
        self.columns = columns
        self.relationships = []

    def set_columns(self, columns):
        self.columns = columns

    def add_relationship(self, certainty, target_file_hash, target_column_name):
        self.relationships.append(Relationship(certainty, target_file_hash, target_column_name))


class Metadata:
    filepath: str
    extension: FileExtension
    size: (int, FileSizeUnit)
    hash: int
    columns: [ColMetadata]
    reference_dataframe: Optional[pandas.DataFrame]

    def __init__(self, filepath: str, extension: FileExtension,
                 size: (int, FileSizeUnit), file_hash: int,
                 columns: [] = [], reference_dataframe: Optional[pandas.DataFrame] = None):
        self.filepath = filepath
        self.extension = extension
        self.size = size
        self.hash = file_hash
        self.columns = columns
        self.reference_dataframe = reference_dataframe

    def set_columns(self, columns):
        self.columns = columns

    def set_reference_dataframe(self, dataframe):
        self.reference_dataframe = dataframe
    def dump_to_xml(self):
        # ugly solution but I'm tired and I want to go to sleep
        temp = self.reference_dataframe
        del self.reference_dataframe

        dict = vars(self)
        xml = dicttoxml(dict, attr_type=False, custom_root='Metadata')
        with open(self.filepath+'.metadata.xml', 'a') as the_file:
            the_file.write(xml)
        self.reference_dataframe = temp
