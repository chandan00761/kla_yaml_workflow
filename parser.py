"""
YAML Parser implemented in this file is used to generate a graph from
the provided yaml build file.
"""
from yaml import safe_load, FullLoader


class Parser:
    """
    A yaml parser that returns a workflow graph.
    """
    def __init__(self, filename):
        self.filename = filename
        with open(self.filename, 'r') as file:
            self.data = safe_load(file)

    def parse(self):
        pass