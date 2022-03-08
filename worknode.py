"""
This file contains the required classes and methods to generate a workflow graph/tree.
"""

import os
import logging
import threading
import time
from enum import Enum
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# a dictionary that contains output of tasks with their absolute path along with type of output as key
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
    """
    A string denoting an expression that returns either true or false.
    """
    def __init__(self, condition: str):
        self.condition = condition

    def is_valid(self):
        """
        parses the expression string and returns it value
        :return: bool
        """
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
    functionInput : function to run when the WorkNode is run.
    executionTime : execution time of the function.
    fileName : input or output file path for stdin and stdout.
    ruleFileName: path of file containing rules for binning.
    dataSet: dataSet for binning
    precedenceFile: precedence rules to follow for binning during merging.
    dataSets: list of dataSets for merging.
    defectTable: data to export as csv
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
        """
        A time function simply sleeps for a specified amount of time.
        """
        key = self.inputs.functionInput
        if self.inputs and self.inputs.functionInput.startswith("$"):
            # if the function is actually a value of some task
            key = key[2:len(key)-1]
            logging.info(f'{datetime.now()};{self.path} Executing TimeFunction ({memo[key]}, {self.inputs.executionTime})')
        else:
            logging.info(f'{datetime.now()};{self.path} Executing TimeFunction ({key}, {self.inputs.executionTime})')
        time.sleep(self.inputs.executionTime)

    def dataload_function(self):
        """
        Loads data from self.inputs.fileName to memory for operations.
        """
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
        """
        Assigns bins to data according to the rules specified in ruleFileName
        """
        self.lock.acquire(blocking=True)
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
                # rule is [bincode, lower_bound, upper_bound]
                rule = [int(line[0]), -999999, 999999]
                line = line[1].split(" ")
                # this code is not fault tolerant
                if line[0] == 'Signal' and line[1] == '<':
                    rule[2] = int(line[2])
                else:
                    rule[1] = int(line[2])
                    rule[2] = int(line[6])
                rules.append(rule)
        key = self.inputs.dataSet
        key = key[2:len(key)-1]
        data_set = []
        # create a new deep copy of data in memo[key] to prevent overwrite
        for data in memo[key]:
            row = [x for x in data]
            data_set.append(row)
        for data in data_set:
            if len(data) == 4:
                data.append(-1)
            for rule in rules:
                if rule[1] < data[-2] < rule[2]:
                    data[-1] = rule[0]
        if self.outputs.BinningResultsTable:
            memo[self.path+".BinningResultsTable"] = data_set
        if self.outputs.NoOfDefects:
            memo[self.path+".NoOfDefects"] = len(data_set)
        self.lock.release()

    def merge_results_function(self):
        """
        Assigns bincode after merging all the self.inputs.dataSets according to precedence
        """
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
            # for each row of the datasets
            row = [datasets[0][i][0], datasets[0][i][1], datasets[0][i][2], datasets[0][i][3], -1]
            for p in precedence:
                # starting from the highest precedence
                flag = False
                # check if that bincode has been assigned to some dataset.
                for dataset in datasets:
                    if dataset[i][-1] == p:
                        # if assigned no need to check further
                        row[-1] = p
                        flag = True
                        break
                if flag:
                    break
            merged_result.append(row)

        memo[self.path + ".MergedResults"] = merged_result

    def export_result_function(self):
        """
        Given a defectTable as input write its data to output csv file.
        """
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
        self.inputs = None
        self.condition = None
        self.outputs = None
        self.lock = threading.Lock()
        self.executor = None

    def run(self):
        """
        Run the node and log the output.
        :return:
        """
        logging.info(f'{datetime.now()};{self.path} Entry')

        # check if there is some condition on this WorkNode
        if self.condition and not self.condition.is_valid():
            # if the condition on this WorkNode is false then skip it
            logging.info(f'{datetime.now()};{self.path} Skipped')
            logging.info(f'{datetime.now()};{self.path} Exit')
            return

        if self.type == TaskType.task:
            # if WorkNode is a single task simply run it.
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
                # wait for the tasks to finish
                self.executor.shutdown(wait=True, cancel_futures=False)
        logging.info(f'{datetime.now()};{self.path} Exit')
