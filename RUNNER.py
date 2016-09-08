import json

import rill

f = open('Demonstration_flow.json')
data = json.load(f)

graph = rill.engine.network.Graph.from_dict(data)
net = rill.engine.network.Network(graph)

net.go()
