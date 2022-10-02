import graphviz

from discovery.utils.metadata import Metadata
import pandas.api.types as ptypes


class DirectoryGraph:
    children: []
    full_path: str
    graph: graphviz.Digraph

    def __init__(self, full_path):
        self.children = []
        self.full_path = full_path

    def set_graph(self, graph: graphviz.Graph):
        self.graph = graph

    def get_parent_path(self):
        return self.full_path.rsplit('/',1)[0]

    def recursively_add_subgraphs(self):
        for child in self.children:
            child.recursively_add_subgraphs()
            self.graph.subgraph(child.graph)
        return
class Visualizer:
    root_graph: DirectoryGraph
    working_directory: DirectoryGraph
    visited_directories: [DirectoryGraph]

    def __init__(self, start_path):
        self.visited_directories = []

        self.root_graph = DirectoryGraph(start_path)

        start_graph = graphviz.Graph(name="cluster_root", comment='File Visualization',
                                                   node_attr={'shape': 'plaintext'})
        start_graph.attr(label=start_path)
        start_graph.engine = "fdp"
        self.root_graph.set_graph(start_graph)
        self.working_directory = self.root_graph
        self.visited_directories.append(self.working_directory)

    def draw_metadata(self, metadata: Metadata):
        cols = self.draw_columns(metadata.dataframe)
        filled_table = self.generate_base_table_structure().format(metadata.filepath, cols)
        self.working_directory.graph.node(metadata.filepath, filled_table)

    def draw_columns(self, dataframe):
        col_row: str = ''
        for column in dataframe.columns:
            col_row += "<TR><TD>{}</TD> <TD>{}</TD> <TD>{}</TD> <TD>{}</TD> <TD>{}</TD></TR>"\
                .format(column, dataframe[column].dtype,
                        (dataframe[column].mean() if ptypes.is_numeric_dtype(dataframe[column]) else "NA"),
                        dataframe[column].min(), dataframe[column].max())
        return col_row

    def draw(self, filename: str):
        self.root_graph.recursively_add_subgraphs()
        self.root_graph.graph.view(filename)

    def generate_base_table_structure(self):
        return '''<
            <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0">
              <TR>
                <TD COLSPAN="2" BGCOLOR="darkgrey">{}</TD>
              </TR>
              <TR>
                <TD BGCOLOR="lightgray">Name</TD>
                <TD BGCOLOR="lightgray">Type</TD>
                <TD BGCOLOR="lightgray">Average</TD>
                <TD BGCOLOR="lightgray">Lowest</TD>
                <TD BGCOLOR="lightgray">Highest</TD>

              </TR>
              {}
            </TABLE>>'''

    def change_working_graph(self, full_path):
        already_visited = next( iter([x for x in self.visited_directories if x.full_path == full_path]), None)
        if already_visited is not None:
            self.working_directory = already_visited
            return

        new_graph = DirectoryGraph(full_path)
        visited_parent = next( iter([x for x in self.visited_directories
                               if x.full_path == new_graph.get_parent_path()]),
                              None)
        if (visited_parent is not None):
            subgraph = graphviz.Graph(name="cluster_"+full_path)
            subgraph.attr(label=full_path)
            subgraph.attr(color='grey')
            visited_parent.children.append(new_graph)
            new_graph.set_graph(subgraph)
            self.visited_directories.append(new_graph)
            self.working_directory = new_graph
            return
        raise Exception("Dangling path")

