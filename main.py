"""
KLA YAML Workflow builder.

Given a filename as input, builds a workflow graph from it and runs it. The logs are written to
input file with output.txt appended.
"""

import argparse
import logging
import os
from parser import Parser

# getting yaml file from argument
arg_parser = argparse.ArgumentParser(description="KLA YAML Workflow builder.")
arg_parser.add_argument("filename", help="YAML File")
args = arg_parser.parse_args()

# creating a Parser to parse yaml

kla_parser = Parser(filename=args.filename)

logfile = args.filename + "output.txt"

if os.path.exists(logfile):
    os.remove(logfile)

logging.basicConfig(filename=args.filename+"output.txt", level=logging.DEBUG, format="")

root = kla_parser.parse()

root.run()

