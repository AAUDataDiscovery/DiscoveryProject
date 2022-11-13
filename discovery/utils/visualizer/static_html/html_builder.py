from utils.metadata.metadata import Metadata
from utils.visualizer.static_html.page_classes.file_view.file_page import FilePage
from utils.visualizer.static_html.page_classes.index_page import IndexPage
from utils.visualizer.static_html.renderer import Renderer
from utils.visualizer.static_html.utils import create_sidebar_dictionary


def build_html_report(title, metadata: [Metadata]):
    renderer = create_renderer(metadata)
    create_index_page(title, metadata, renderer)
    create_file_pages(metadata, renderer)


def create_renderer(metadata: [Metadata]):
    renderer = Renderer()
    renderer.set_sidebar(create_sidebar_dictionary(metadata))
    return renderer

def create_file_pages(metadata: [Metadata], renderer: Renderer):
    for metadatum in metadata:
        filepage = FilePage(metadatum, renderer)
        filepage.with_file_table()
        filepage.render()


def create_index_page(title, metadata, renderer):
    index = IndexPage(title, renderer)
    index.with_global_graph(metadata)
    index.render()
