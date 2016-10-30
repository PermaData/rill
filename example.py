import ast
from rill.engine.network import Graph, Network
from rill.components.hello_world import LineToWords, StartsWith, WordsToLine, Output

graph_file = open('example_serialized.txt')
graph_str = graph_file.read()
graph_file.close()
graph_from_file = ast.literal_eval(graph_str)

graph = Graph.from_dict(graph_from_file)
net = Network(graph)
net.go()
