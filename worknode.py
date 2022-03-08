"""
This file contains the required classes and methods to generate a workflow graph/tree.
"""

import os
import logging
import threading
import time
from enum import Enum
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, wait


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
        self.functionInput = None
        self.executionTime = None
        self.fileName = None
        self.ruleFileName = None
        self.dataSet = None
        self.precedenceFile = None
        self.dataSets = []
        self.defectTable = None


class Output:
    def __init__(self):
        self.DataTable = False
        self.NoOfDefects = False
        self.BinningResultsTable = False
        self.MergedResults = False


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
        data = []
        lines = 0
        with open(self.inputs.fileName, 'r') as file:
            for line in file:
                lines = lines + 1
                if lines == 1:
                    continue
                line = [int(x) for x in line.split(",")]
                data.append(line)
        if self.outputs.DataTable:
            memo[self.path+".DataTable"] = data
        if self.outputs.NoOfDefects:
            memo[self.path+".NoOfDefects"] = lines-1

    def binning_function(self):
        self.lock.acquire(blocking=True)
        try:
            rules = []
            with open(self.inputs.ruleFileName, 'r') as file:
                lines = 0
                for line in file:
                    lines = lines + 1
                    if lines == 1:
                        continue
                    if line == "\n":
                        continue
                    line = line.split(",")
                    rule = [int(line[0]), -999999, 999999]
                    line = line[1].split(" ")
                    if line[0] == 'Signal' and line[1] == '<':
                        rule[2] = int(line[2])
                    else:
                        rule[1] = int(line[2])
                        rule[2] = int(line[6])
                    rules.append(rule)
            key = self.inputs.dataSet
            key = key[2:len(key)-1]
            data_set = []
            for data in memo[key]:
                row = [x for x in data]
                data_set.append(row)
            for data in data_set:
                if len(data) == 4:
                    data.append(-1)
                for rule in rules:
                    if rule[1] < data[-2] < rule[2]:
                        data[-1] = rule[0]
        except Exception as e:
            print("exception" + e)
        if self.outputs.BinningResultsTable:
            memo[self.path+".BinningResultsTable"] = data_set
        if self.outputs.NoOfDefects:
            memo[self.path+".NoOfDefects"] = len(data_set)
        self.lock.release()

    def merge_results_function(self):
        precedence = []
        with open(self.inputs.precedenceFile) as file:
            precedence = [int(x) for x in file.readline().split(" >> ")]
        precedence.append(-1)
        datasets = []
        for dataset in self.inputs.dataSets:
            key = dataset[2:len(dataset)-1]
            datasets.append(memo[key])

        merged_result = []
        for i in range(len(datasets[0])):
            row = [datasets[0][i][0], datasets[0][i][1], datasets[0][i][2], datasets[0][i][3], -1]
            for p in precedence:
                flag = False
                for dataset in datasets:
                    if dataset[i][-1] == p:
                        row[-1] = p
                        flag = True
                        break
                if flag:
                    break
            merged_result.append(row)

        memo[self.path + ".MergedResults"] = merged_result

    def export_result_function(self):
        key = self.inputs.defectTable
        key = key[2:len(key)-1]
        with open(self.inputs.fileName, "w") as file:
            file.write("Id,X,Y,Signal,Bincode\n")
            data = memo[key]
            for row in data:
                row = [str(x) for x in row]
                file.write(",".join(row))
                file.write("\n")

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
        self.outputs: Output = None
        self.lock = threading.Lock()
        self.executor: ThreadPoolExecutor = None

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
                self.executor = ThreadPoolExecutor(max_workers=len(self.activities))
                futures = []
                for task in self.activities:
                    futures.append(self.executor.submit(task.run))
                self.executor.shutdown(wait=True, cancel_futures=False)
        logging.info(f'{datetime.now()};{self.path} Exit')
