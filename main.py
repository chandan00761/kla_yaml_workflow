"""
KLA YAML Workflow builder.
"""

import argparse
from parser import Parser

# getting yaml file from argument
arg_parser = argparse.ArgumentParser(description="KLA YAML Workflow builder.")
arg_parser.add_argument("filename", help="YAML File")
args = arg_parser.parse_args()

# creating a Parser to parse yaml

kla_parser = Parser(filename=args.filename)

kla_parser.parse()
