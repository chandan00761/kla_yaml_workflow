"""
This file contains the required classes and methods to generate a workflow graph/tree.
"""

import logging
import time
from enum import Enum
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor


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


class Input:
    """
    Represents the input passed to the WorkNode.
    """
    def __init__(self):
        self.functionInput = None
        self.executionTime = None


class WorkNode:
    """
    Denotes a node in the workflow graph.
    """

    def time_function(self):
        logging.info(f'{datetime.now()};{self.path} Executing TimeFunction({self.inputs.functionInput}, {self.inputs.executionTime})')
        time.sleep(self.inputs.executionTime)

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
        self.inputs = None

    def run(self):
        """
        Run the node and log the output.
        :return:
        """
        logging.info(f'{datetime.now()};{self.path} Entry')
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
