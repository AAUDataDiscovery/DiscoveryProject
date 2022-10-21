from enum import Enum


class FileSizeUnit(Enum):
    BYTE = "byte"
    KILOBYTE = "kilobyte"
    MEGABYTE = "megabyte"
    GIGABYTE = "gigabyte"


class FileExtension(Enum):
    JSON = "json"
    CSV = "csv"
