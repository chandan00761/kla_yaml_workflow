"""
YAML Parser implemented in this file is used to generate a graph from
the provided yaml build file.
"""
from yaml import safe_load, FullLoader

from worknode import WorkNode, TaskType, ExecutionType, Input


class Parser:
    """
    A yaml parser that returns a workflow graph.
    """

    def __init__(self, filename):
        self.filename = filename
        with open(self.filename, 'r') as file:
            self.data: dict = safe_load(file)

    def _parse_util_flow(self, data: dict):
        activities = []

        for key, value in data['Activities'].items():
            if value['Type'] == 'Task':
                task_node = self._parse_util_task(value)
                task_node.name = key
                activities.append(task_node)
            else:
                flow_node = WorkNode()
                flow_node.name = key
                flow_node.type = TaskType.flow
                flow_node.execution = ExecutionType.sequential if value[
                                                                      'Execution'] == 'Sequential' else ExecutionType.concurrent
                flow_node.activities = self._parse_util_flow(value)
                activities.append(flow_node)

        return activities

    def _parse_util_task(self, data: dict):
        task_node = WorkNode()
        task_node.type = TaskType.task
        task_node.function = WorkNode.time_function
        task_node.inputs = Input()
        task_node.inputs.functionInput = data['Inputs']['FunctionInput']
        task_node.inputs.executionTime = int(data['Inputs']['ExecutionTime'])
        return task_node

    def parse(self):
        root = WorkNode()
        root.name = list(self.data.keys())[0]
        root.type = TaskType.flow if self.data[root.name]['Type'] == 'Flow' else TaskType.task
        if root.type == TaskType.flow:
            root.execution = ExecutionType.sequential \
                if self.data[root.name]['Execution'] == 'Sequential' else ExecutionType.concurrent
            root.activities = self._parse_util_flow(self.data[root.name])
        else:
            root.function = WorkNode.time_function
            root.inputs = Input()
            root.inputs.functionInput = self.data[root.name]['FunctionInput']
            root.inputs.executionTime = int(self.data[root.name]['ExecutionTime'])

        return root
