"""
YAML Parser implemented in this file is used to generate a graph from
the provided yaml build file.
"""
import os

from yaml import safe_load
from worknode import WorkNode, TaskType, ExecutionType, Input, Output, Condition


class Parser:
    """
    A yaml parser that returns a workflow graph.
    """

    def __init__(self, filename):
        """
        Given filename read the file and parse the yaml file.
        :param filename:
        """
        self.filename = filename
        with open(self.filename, 'r') as file:
            self.data: dict = safe_load(file)

    def _parse_util_flow(self, data: dict, root):
        """
        function to parse the flow nodes
        :param data:
        :param root:
        :return:
        """
        activities = []

        for key, value in data['Activities'].items():
            if value['Type'] == 'Task':
                task_node = self._parse_util_task(value)
                task_node.name = key
                task_node.path += root.path + f'.{key}'
                activities.append(task_node)
            else:
                flow_node = WorkNode()
                flow_node.name = key
                flow_node.path += root.path + f'.{key}'
                flow_node.type = TaskType.flow
                flow_node.execution = ExecutionType.sequential if value[
                                                                      'Execution'] == 'Sequential' else ExecutionType.concurrent
                flow_node.activities = self._parse_util_flow(value, flow_node)
                activities.append(flow_node)

        return activities

    def _parse_util_task(self, data: dict):
        """
        function to parse the task nodes
        :param data:
        :return:
        """
        task_node = WorkNode()
        task_node.type = TaskType.task
        if 'Condition' in data:
            task_node.condition = Condition(data['Condition'])
        if data['Function'] == "TimeFunction":
            task_node.function = WorkNode.time_function
            task_node.inputs = Input()
            task_node.inputs.functionInput = data['Inputs']['FunctionInput']
            task_node.inputs.executionTime = int(data['Inputs']['ExecutionTime'])
        elif data['Function'] == "DataLoad":
            task_node.function = WorkNode.dataload_function
            task_node.inputs = Input()
            if 'Filename' in data['Inputs']:
                file_dir = os.path.dirname(os.path.abspath(self.filename))
                input_file = os.path.join(file_dir, data['Inputs']['Filename'])
                task_node.inputs.fileName = input_file
            if 'Outputs' in data:
                task_node.outputs = Output()
                task_node.outputs.DataTable = 'DataTable' in data['Outputs']
                task_node.outputs.NoOfDefects = 'NoOfDefects' in data['Outputs']
        elif data['Function'] == "Binning":
            task_node.function = WorkNode.binning_function
            task_node.inputs = Input()
            if "Condition" in data:
                task_node.condition = Condition(data["Condition"])
            file_dir = os.path.dirname(os.path.abspath(self.filename))
            rule_file = os.path.join(file_dir, data['Inputs']['RuleFilename'])
            task_node.inputs.ruleFileName = rule_file
            task_node.inputs.dataSet = data['Inputs']['DataSet']
            if 'Outputs' in data:
                task_node.outputs = Output()
                task_node.outputs.DataTable = 'DataTable' in data['Outputs']
                task_node.outputs.BinningResultsTable = 'BinningResultsTable' in data['Outputs']
                task_node.outputs.NoOfDefects = 'NoOfDefects' in data['Outputs']
        elif data['Function'] == 'MergeResults':
            task_node.function = WorkNode.merge_results_function
            task_node.inputs = Input()
            task_node.inputs.precedenceFile = data['Inputs']['PrecedenceFile']
            for key, value in data['Inputs'].items():
                if key.startswith('DataSet'):
                    task_node.inputs.dataSets.append(value)
            task_node.outputs = Output()
            task_node.outputs.MergedResults = 'MergedResults' in data['Outputs']
            task_node.outputs.NoOfDefects = 'NoOfDefects' in data['Outputs']
        elif data['Function'] == 'ExportResults':
            task_node.function = WorkNode.export_result_function
            task_node.inputs = Input()
            task_node.inputs.fileName = data['Inputs']['FileName']
            task_node.inputs.defectTable = data['Inputs']['DefectTable']
        return task_node

    def parse(self):
        """
        uses the parsed yaml data to generate workflow graph of WorkNodes
        :return:
        """
        root = WorkNode()
        root.name = list(self.data.keys())[0]
        root.path = root.name
        root.type = TaskType.flow if self.data[root.name]['Type'] == 'Flow' else TaskType.task
        if root.type == TaskType.flow:
            root.execution = ExecutionType.sequential \
                if self.data[root.name]['Execution'] == 'Sequential' else ExecutionType.concurrent
            root.activities = self._parse_util_flow(self.data[root.name], root)
        else:
            if self.data['Function'] == 'TimeFunction':
                root.function = WorkNode.time_function
            elif self.data['Function'] == 'DataLoad':
                root.function = WorkNode.dataload_function
            elif self.data['Function'] == 'Binning':
                root.function = WorkNode.binning_function
            elif self.data['Function'] == 'MergeResults':
                root.function = WorkNode.merge_results_function
            elif self.data['Function'] == 'ExportResults':
                root.function = WorkNode.export_result_function
            root.inputs = Input()
            root.inputs.functionInput = self.data[root.name]['FunctionInput']
            root.inputs.executionTime = int(self.data[root.name]['ExecutionTime'])

        return root
