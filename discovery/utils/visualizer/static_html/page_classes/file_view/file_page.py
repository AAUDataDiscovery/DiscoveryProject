from utils.metadata.metadata import Metadata
from utils.visualizer.static_html.page_classes.file_view.file_table_section import FileTableSection
from utils.visualizer.static_html.page_classes.file_view.relationship_section import RelationshipSection
from utils.visualizer.static_html.renderer import Renderer
from utils.visualizer.static_html.utils import create_metadata_href


class FilePage:
    def __init__(self, metadatum: Metadata, renderer: Renderer):
        self.renderer = renderer
        self.metadatum = metadatum

        self.file_table_dict = None
        self.relationship_dict = None

    def with_file_table(self):
        file_table = FileTableSection(self.metadatum)
        self.file_table_dict = file_table.build_dictionary()

    def with_relationship_section(self):
        relationship_section = RelationshipSection()
        self.relationship_dict = relationship_section.build_dictionary()

    def render(self):
        html_filename = create_metadata_href(self.metadatum)
        self.renderer.render(html_filename, self._build_custom_data_dict())

    def _build_custom_data_dict(self):
        return {
            "title": self.metadatum.file_path,
            "content_file": "file/file.html",
            "file_table_section": self.file_table_dict,
            "relationship_section": self.relationship_dict
        }
