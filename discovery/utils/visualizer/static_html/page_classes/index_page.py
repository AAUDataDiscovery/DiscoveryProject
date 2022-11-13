from utils.metadata.metadata import Metadata
from utils.visualizer.graphing.visualizer import Visualizer
from utils.visualizer.static_html.renderer import Renderer


class IndexPage:
    def __init__(self, title: str, renderer: Renderer):
        self.renderer = renderer
        self.title = title
        self.graph_src = None

    def with_global_graph(self, metadata: Metadata):
        graph_src = "index_graph"
        visualizer = Visualizer()

        visualizer.draw(metadata, graph_src)
        self.graph_src = graph_src+".png"

    def render(self):
        self.renderer.render("index.html", self._build_custom_data_dict())

    def _build_custom_data_dict(self):
        return {
            "title": self.title,
            "content_file": "index.html",
            "graph_src": self.graph_src
        }
