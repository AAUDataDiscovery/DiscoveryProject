import os

import jinja2 as jinja2

from utils.metadata.metadata import Metadata


def create_metadata_href(metadatum: Metadata):
    filename = get_filename_from_path(metadatum.file_path)
    return "{}_{}.html".format(filename, metadatum.hash)


def get_filename_from_path(path):
    return path.rsplit(os.sep, 1)[-1] or path


def create_sidebar_dictionary(metadata: [Metadata]):
    sidebar_items = []
    for data in metadata:
        sidebar_items.append({
            "href": create_metadata_href(data),
            "text": data.file_path
        })
    return sidebar_items
