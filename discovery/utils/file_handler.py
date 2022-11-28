"""
Reads a given file path into a dataframe
"""
import os
from functools import partial

import pandas
import logging
from pandas.util import hash_pandas_object

from discovery.utils.custom_exceptions import UnsupportedFileExtension, FileNotFoundError
from discovery.utils.metadata.metadata_enums import FileExtension, FileSizeUnit
from discovery.utils.metadata.metadata import construct_metadata_from_file_descriptor

logger = logging.getLogger(__name__)


class FileHandler:
    def __init__(self):
        self.supported_extensions = {
            FileExtension.CSV: self._handle_csv,
            FileExtension.JSON: self._handle_json
        }
        self.ignored_extensions = [".metadata.json"]
        self._loaded_streams = ()

    def consume_files(self):
        """
        Consumes every loaded file
        """
        return [(filename, file_data) for filename, file_data in self._loaded_streams]

    def scan_filesystem(self, file_path):
        """
        Loads a given file system into memory
        """
        for root, dirs, files in os.walk(file_path):
            for filename in files:
                if any([filename.endswith(extension.value) for extension in self.supported_extensions]):
                    yield self.load_file(os.path.join(root, filename))

    def load_file(self, file_path, partition_size=None):
        """
        Lock a file into a file stream to be consumed down the line
        """
        # file doesn't exist
        if not os.path.exists(file_path):
            raise FileNotFoundError(file_path)

        extension = file_path.split('.')[-1] if '.' in file_path else 'txt'
        # file doesn't have a supported extension
        try:
            extension_enum = FileExtension(extension)
        except ValueError:
            raise UnsupportedFileExtension(file_path)

        if extension_enum not in self.supported_extensions:
            raise UnsupportedFileExtension(file_path)

        if self._is_file_metadata(file_path):
            return

        # Nothing in place to prevent reloading files for now
        handler = self.supported_extensions[extension_enum]

        df_loader = partial(self._load_dataframe, file_path, partition_size, handler)
        return construct_metadata_from_file_descriptor(construct_file_descriptor(file_path, extension_enum, df_loader))

    @staticmethod
    def _load_dataframe(path, partition_size, handler):
        """
        Return a generator for the dataframe, this can be used to preserve memory
        Partition size will specify how much of the dataframe to pull

        To properly iterate over partition sizes, use an iterator:
            [x for x in _load_dataframe(*args)]
        """
        current_row = 0
        if partition_size:
            # with the partition size set, iterate over the dataframe
            output = handler(path, row_count=partition_size, row_seek=current_row)
            while not output.empty:
                yield output
                current_row += partition_size
                output = handler(path, row_count=partition_size, row_seek=current_row)
        else:
            yield handler(path, row_count=partition_size, row_seek=current_row)

    def _is_file_metadata(self, path):
        return any(path.endswith(ignored_extension) for ignored_extension in self.ignored_extensions)

    @staticmethod
    def _handle_csv(file_path, row_count, row_seek):
        return pandas.read_csv(file_path, nrows=row_count, skiprows=row_seek)

    @staticmethod
    def _handle_json(file_path, row_count, row_seek):
        return pandas.read_json(file_path)

    @staticmethod
    def get_dataframe_hash(dataframe: pandas.DataFrame):
        return hash_pandas_object(dataframe).sum()


def construct_file_descriptor(file_path: str, extension: FileExtension, dataframe_call: partial):
    size = os.stat(file_path).st_size
    dataframe_data = next(dataframe_call())
    file_hash = FileHandler.get_dataframe_hash(dataframe_data)
    # use absolute paths for df names, as they must be unique
    dataframe_name = os.path.abspath(file_path)
    return {
        "file_path": file_path,
        "extension": extension,
        "dataframe": dataframe_call,
        "size": (size, FileSizeUnit.BYTE),
        "file_hash": file_hash,
        "dataframe_name": dataframe_name
    }
