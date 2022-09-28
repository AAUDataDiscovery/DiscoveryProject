"""
Reads a given file path into a dataframe
"""
import os
import pandas
import logging

from utils.custom_exceptions import UnsupportedFileExtension, FileNotFoundError

logger = logging.getLogger(__name__)


class FileHandler:
    def __init__(self):
        self.supported_extensions = {m.split('_')[-1]: getattr(self, m) for m in dir(self)}
        self.loaded_files = {}

    def scan_filesystem(self, file_path):
        """
        Loads a given file system into memory
        """
        for root, dirs, files in os.walk(file_path):
            for filename in files:
                if any([file_path.endswith(extension) for extension in self.supported_extensions]):
                    self.load_file(os.path.join(root, filename))

    def load_file(self, file_path):
        """
        Loads a given file if a reader exists for its extension
        """
        extension = file_path.split('.')[-1] if '.' in file_path else 'txt'
        # file doesn't exist
        if not os.path.exists(file_path):
            raise FileNotFoundError

        # file doesn't have a supported extension
        if extension not in self.supported_extensions:
            raise UnsupportedFileExtension

        # Nothing in place to prevent reloading files for now
        handler = self.supported_extensions[extension]
        logging.debug(f"Loading file {file_path} using \"{handler.__name__}\" handler")
        self.loaded_files[file_path] = handler(file_path)

    @staticmethod
    def _handle_csv(file_path):
        return pandas.read_csv(file_path)
