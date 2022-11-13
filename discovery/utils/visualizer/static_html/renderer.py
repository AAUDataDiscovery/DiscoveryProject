import os

import jinja2 as jinja2

TEMPLATE_RELATIVE_PATH = "template"


class Renderer:
    def __init__(self, out_folder: str = ""):
        self.out_folder = out_folder
        self.sidebar = None
        self.custom_data = {}

    def render(self, outputfile: str, custom_data, template="template.html"):
        self.custom_data = self._extend_custom_data(custom_data)
        html_out = self._generate_html(template)
        self._write_file(outputfile, html_out)

    def set_sidebar(self, sidebar: dict):
        self.sidebar = sidebar

    def _generate_html(self, template):
        return jinja2.Environment(
            loader=jinja2.FileSystemLoader(self.get_current_template_folder())
        ) \
            .get_template(template) \
            .render(self.custom_data)

    def _write_file(self, outputfile: str, html_str: str):
        path = self.out_folder+outputfile
        with open(path, 'w') as f:
            f.write(html_str)

    def _extend_custom_data(self, custom_data: dict):
        custom_data.update({
            "sidebar": self.sidebar
        })
        return custom_data

    @staticmethod
    def get_current_template_folder():
        local = os.path.dirname(__file__)
        return "{0}/{1}".format(local, TEMPLATE_RELATIVE_PATH)
