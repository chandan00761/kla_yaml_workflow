"""
This file contains the required classes and methods to generate a workflow graph/tree.
"""

from enum import Enum


class TaskType(Enum):
    """
    Enum denoting types of Tasks
    """
    flow = 1
    task = 2


class ExecutionType(Enum):
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

    @classmethod
    def time_function(cls):
        pass

    def __init__(self):
        self.name = None
        self.type = TaskType.task
        self.execution = ExecutionType.sequential
        self.activities = []
        self.function = None
        self.inputs = None
