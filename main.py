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

# for testing purpose only
# args.filename = './DataSet/Examples/Milestone1/Milestone1_Example.yaml'
# args.filename = './DataSet/Examples/Milestone2/Milestone2_Example.yaml'
# args.filename = './DataSet/Examples/Milestone3/Milestone3A.yaml'
# args.filename = './DataSet/Milestone1/Milestone1A.yaml'
# args.filename = './DataSet/Milestone1/Milestone1B.yaml'
# args.filename = './DataSet/Milestone2/Milestone2A.yaml'
# args.filename = './DataSet/Milestone2/Milestone2B.yaml'
args.filename = './DataSet/Milestone3/Milestone3A.yaml'

kla_parser = Parser(filename=args.filename)

logfile = args.filename + "output.txt"

if os.path.exists(logfile):
    os.remove(logfile)

logging.basicConfig(filename=args.filename+"output.txt", level=logging.DEBUG, format="")

root = kla_parser.parse()

root.run()

