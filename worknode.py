"""
This file contains the required classes and methods to generate a workflow graph/tree.
"""

import os
import logging
import time
import pandas as pd
from enum import Enum
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor


memo = {}


class TaskType(Enum):
    """
    Enum denoting types of Tasks
    """
    flow = 1
    task = 2


class ExecutionType(Enum):
    """
    Enum denoting the types of executions.
    """
    sequential = 1
    concurrent = 2


class Condition:
    def __init__(self, condition: str):
        self.condition = condition

    def is_valid(self):
        elements = self.condition.split(" ")
        key = elements[0]
        check = elements[1]
        value = int(elements[2])
        key = key[2:len(key)-1]
        if check == '<':
            return memo[key] < value
        else:
            return memo[key] > value


class Input:
    """
    Represents the input passed to the WorkNode.
    """
    def __init__(self):
        self.functionInput: str = None
        self.executionTime = None
        self.fileName = None


class Output:
    def __init__(self):
        self.DataTable = True
        self.NoOfDefects = True


class WorkNode:
    """
    Denotes a node in the workflow graph.
    """

    def time_function(self):
        key = self.inputs.functionInput
        if self.inputs and self.inputs.functionInput.startswith("$"):
            key = key[2:len(key)-1]
            logging.info(f'{datetime.now()};{self.path} Executing TimeFunction ({memo[key]}, {self.inputs.executionTime})')
        else:
            logging.info(f'{datetime.now()};{self.path} Executing TimeFunction ({key}, {self.inputs.executionTime})')
        time.sleep(self.inputs.executionTime)

    def dataload_function(self):
        logging.info(
            f'{datetime.now()};{self.path} Executing DataLoad ({os.path.basename(self.inputs.fileName)})')
        data = 0
        with open(self.inputs.fileName, 'r') as file:
            data = len(file.readlines())
        # if we want to do something with the data we can do here
        # if self.outputs.DataTable:
        #     memo[self.path+"DataTable"] = data
        if self.outputs.NoOfDefects:
            memo[self.path+".NoOfDefects"] = data-1

    def __init__(self):
        """
        name : name of node
        path : path to node
        activities: nodes connected
        """
        self.name = None
        self.path = ""
        self.type = TaskType.task
        self.execution = None
        self.activities = []
        self.function = None
        self.inputs: Input = None
        self.condition = None
        self.outputs = None

    def run(self):
        """
        Run the node and log the output.
        :return:
        """
        logging.info(f'{datetime.now()};{self.path} Entry')

        if self.condition and not self.condition.is_valid():
            logging.info(f'{datetime.now()};{self.path} Skipped')
            logging.info(f'{datetime.now()};{self.path} Exit')
            return

        if self.type == TaskType.task:
            self.function(self)
        else:
            if self.execution == ExecutionType.sequential:
                for task in self.activities:
                    task.run()
            else:
                executor = ThreadPoolExecutor(max_workers=len(self.activities))
                for task in self.activities:
                    executor.submit(task.run)
                executor.shutdown(wait=True, cancel_futures=False)
        logging.info(f'{datetime.now()};{self.path} Exit')
