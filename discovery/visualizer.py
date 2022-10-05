from typing import Optional
import graphviz
from discovery.utils.metadata import Metadata


class SubGraphNode:
    children: []
    filesystem_full_path: str
    graph: graphviz.Digraph

    def __init__(self, filesystem_full_path, graph: Optional[graphviz.Graph] = None):
        self.children = []
        self.filesystem_full_path = filesystem_full_path
        self._set_graph(graph)

    def _set_graph(self, graph: Optional[graphviz.Graph]):
        if graph is None:
            self.graph = self._generate_graph()
        else:
            self.graph = graph

    def _generate_graph(self):
        subgraph = graphviz.Graph(name="cluster_" + self.filesystem_full_path)
        subgraph.attr(label=self.filesystem_full_path)
        subgraph.attr(color='grey')
        return subgraph

    def get_parent_path(self):
        split = self.filesystem_full_path.rsplit('/', 1)
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
    observed_nodes: [SubGraphNode]

    def __init__(self, engine="fdp"):
        self.engine = engine
        self.observed_nodes = []
        self.root = SubGraphNode("", self._generate_root_graph())
        self.working_node = self.root
        self.observed_nodes.append(self.working_node)

    def _generate_root_graph(self):
        start_graph = graphviz.Graph(name="cluster_root", comment='File Visualization',
                                     node_attr={'shape': 'plaintext'})
        start_graph.attr(label="")
        start_graph.engine = self.engine
        return start_graph

    def draw(self, metadata: [Metadata], filename: str):
        self.draw_metadata(metadata)
        self._finalize_result_graph(filename)

    def draw_metadata(self, metadata):
        for datum in metadata:
            self.working_node = self._determine_working_node(datum.filepath)
            self._draw_metadatum(datum)

    def _finalize_result_graph(self, output_filename):
        self.root.recursively_add_subgraphs()
        self.root.graph.view(output_filename)

    def _draw_metadatum(self, metadatum: Metadata):
        columns = self._draw_table_columns(metadatum)
        filled_table = self._draw_filled_metadatum_table(metadatum.filepath, columns)
        self.working_node.graph.node(metadatum.filepath, filled_table)

    # TODO: make it more generic
    def _draw_table_columns(self, metadatum):
        col_rows: str = ""
        for column in metadatum.columns:
            col_rows += "<TR><TD>{}</TD> <TD>{}</TD> <TD>{}</TD> <TD>{}</TD> <TD>{}</TD></TR>" \
                .format(column.name, column.col_type,
                        (column.mean if column.mean is not None else "NA"),
                        column.min, column.max)
        return col_rows

    # TODO: break down mean string generator
    def _draw_column_row(self, column):
        return f'<TR><TD>{column.name}</TD> <TD>{column.col_type}</TD> <TD>{(column.mean if column.mean is not None else "NA")}</TD> <TD>{column.min}</TD> <TD>{column.max}</TD></TR>'

    # TODO: find a more generic approach
    def _draw_filled_metadatum_table(self, filename: str, col_strings: str):
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

    def _determine_working_node(self, filesystem_full_path: str):
        determined_node = None

        already_visited_node = self._get_node_if_path_was_already_observed(filesystem_full_path)
        if already_visited_node is not None:
            determined_node = already_visited_node
        else:
            determined_node = SubGraphNode(filesystem_full_path)
            self._add_all_previously_unobserved_parents_to_observed_nodes(determined_node)
        return determined_node

    def _add_all_previously_unobserved_parents_to_observed_nodes(self, parameter_child_node: SubGraphNode):
        working_child_node = parameter_child_node
        while True:
            visited_parent = self._get_parent_if_parent_was_already_observed(working_child_node)
            if visited_parent is None:
                new_parent = SubGraphNode(working_child_node.get_parent_path())
                new_parent.children.append(working_child_node)
                # setup next loop
                working_child_node = new_parent
            else:
                visited_parent.children.append(working_child_node)
                break

    def _get_node_if_path_was_already_observed(self, filesystem_full_path: str):
        return next(iter([node for node in self.observed_nodes if node.filesystem_full_path == filesystem_full_path]),
                    None)

    def _get_parent_if_parent_was_already_observed(self, node: SubGraphNode):
        return next(iter([x for x in self.observed_nodes
                          if x.filesystem_full_path == node.get_parent_path()]), None)