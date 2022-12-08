from discovery.metadata.catalogue_item import CatalogueItem
from discovery.metadata.catalogue_metadata import CatalogueMetadata
from discovery.utils.data_readers import *


def from_csv(
        path: str,
        tags: dict = None,
        dataframe_metadata: CatalogueItem = None
):
    """Construct a metadata object by providing a csv file"""
    tags = tags or {}
    tags.update({
        'file_path': path,
        'extension': "csv",
        "data_loader": "LocalCSVReader"
    })
    data = LocalCSVReader(path)
    if dataframe_metadata is None:
        # if no metadata is provided, build an empty one
        dataframe_metadata = CatalogueMetadata(tags=tags)
    return CatalogueItem(metadata=dataframe_metadata, data=data)
