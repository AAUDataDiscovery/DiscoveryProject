from typing import Optional

import graphviz

from discovery.utils.metadata import Metadata


class SubGraphNode:
    children: []
    fs_full_path: str
    graph: graphviz.Digraph

    def __init__(self, fs_full_path, graph: Optional[graphviz.Graph] = None):
        self.children = []
        self.fs_full_path = fs_full_path
        self._set_graph(graph)

    def _set_graph(self, graph: Optional[graphviz.Graph]):
        if graph is None:
            self.graph = self._generate_graph()
        else:
            self.graph = graph

    def _generate_graph(self):
        subgraph = graphviz.Graph(name="cluster_" + self.fs_full_path)
        subgraph.attr(label=self.fs_full_path)
        subgraph.attr(color='grey')
        return subgraph

    def get_parent_path(self):
        split = self.fs_full_path.rsplit('/', 1)
        if 1 < len(split):
            return split[0]
        return ""

    def recursively_add_subgraphs(self):
        for child in self.children:
            child.recursively_add_subgraphs()
            self.graph.subgraph(child.graph)
        return


class Visualizer:
    engine: str

    root: SubGraphNode
    working_node: SubGraphNode
    visited_nodes: [SubGraphNode]

    def __init__(self, engine="fdp"):
        self.engine = engine
        self.visited_nodes = []
        self.root = SubGraphNode("", self._generate_start_graph())
        self.working_node = self.root
        self.visited_nodes.append(self.working_node)

    def _generate_start_graph(self):
        start_graph = graphviz.Graph(name="cluster_root", comment='File Visualization',
                                     node_attr={'shape': 'plaintext'})
        start_graph.attr(label="")
        start_graph.engine = self.engine
        return start_graph

    def _draw_metadata(self, metadata: Metadata):
        cols = self._draw_columns(metadata)
        filled_table = self._generate_table(metadata.filepath, cols)
        self.working_node.graph.node(metadata.filepath, filled_table)

    def _draw_columns(self, metadatum):
        col_row: str = ''
        for column in metadatum.columns:
            col_row += "<TR><TD>{}</TD> <TD>{}</TD> <TD>{}</TD> <TD>{}</TD> <TD>{}</TD></TR>" \
                .format(column.name, column.col_type,
                        (column.mean if column.mean is not None else "NA"),
                        column.min, column.max)
        return col_row

    def draw(self, metadata: [Metadata], filename: str):
        for datum in metadata:
            self.working_node = self._determine_working_node(datum.filepath)
            self._draw_metadata(datum)
        self.root.recursively_add_subgraphs()
        self.root.graph.view(filename)

    # TODO: find a more generic approach
    def _generate_table(self, filename: str, col_strings: str):
        return f'''<
            <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0">
              <TR>
                <TD COLSPAN="2" BGCOLOR="darkgrey">{filename}</TD>
              </TR>
              <TR>
                <TD BGCOLOR="lightgray">Name</TD>
                <TD BGCOLOR="lightgray">Type</TD>
                <TD BGCOLOR="lightgray">Average</TD>
                <TD BGCOLOR="lightgray">Lowest</TD>
                <TD BGCOLOR="lightgray">Highest</TD>

              </TR>
              {col_strings}
            </TABLE>>'''

    def _determine_working_node(self, full_path):
        # directory already registered as a node
        already_visited = next(iter([x for x in self.visited_nodes if x.fs_full_path == full_path]), None)
        if already_visited is not None:
            return already_visited

        new_node = SubGraphNode(full_path)

        # ensure that the tree has all ancenstors, even if no files were registered previously
        child = new_node
        while True:
            visited_parent = self._get_visited_parent(child)
            if visited_parent is None:
                new_parent = SubGraphNode(child.get_parent_path())
                new_parent.children.append(child)

                # setup next loop
                child = new_parent
            else:
                visited_parent.children.append(child)
                break
        return new_node

    def _get_visited_parent(self, node: SubGraphNode):
        return next(iter([x for x in self.visited_nodes
                          if x.fs_full_path == node.get_parent_path()]), None)
