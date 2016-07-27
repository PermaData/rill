from rill.engine.runner import ComponentRunner
from rill.runtime import Runtime, get_graph_messages
from rill.engine.network import Graph

from tests.components import *

import logging
ComponentRunner.logger.setLevel(logging.DEBUG)


def get_graph(graph_name):
    graph = Graph(name=graph_name)

    gen = graph.add_component('Generate', GenerateTestData)
    gen.metadata['x'] = 5
    gen.metadata['y'] = 5

    passthru = graph.add_component('Pass', Passthru)
    outside = graph.add_component('Outside', Passthru)

    graph.connect('Generate.OUT', 'Pass.IN')
    graph.connect('Outside.OUT', 'Pass.IN')
    graph.initialize(5, 'Generate.COUNT')
    graph.export('Pass.OUT', 'OUTPORT')
    graph.export('Outside.IN', 'INPORT')
    return graph, gen, passthru, outside


# FIXME: create a fixture for the network in test_network_serialization and use that here
def test_get_graph_messages():
    """
    Test that runtime can build graph with graph protocol messages
    """
    graph_id = 'graph1'
    graph_name = 'My Graph'
    runtime = Runtime()
    runtime.new_graph(graph_id)

    graph, gen, passthru, outside = get_graph(graph_name)
    runtime.add_graph(graph_id, graph)

    messages = list(get_graph_messages(runtime.get_graph(graph_id), graph_id))

    # FIXME: paste in an exact copy of the document here
    assert ('clear', {
        'id': graph_id,
        'name': graph_name
    }) in messages

    assert ('addnode', {
        'graph': graph_id,
        'id': gen.get_name(),
        'component': gen.get_type(),
        'metadata': gen.metadata
    }) in messages
    assert ('addnode', {
        'graph': graph_id,
        'id': passthru.get_name(),
        'component': passthru.get_type(),
        'metadata': passthru.metadata
    }) in messages
    assert ('addnode', {
        'graph': graph_id,
        'id': outside.get_name(),
        'component': outside.get_type(),
        'metadata': outside.metadata
    }) in messages

    assert ('addedge', {
        'graph': graph_id,
        'src': {
            'node': gen.get_name(),
            'port': 'OUT'
        },
        'tgt': {
            'node': passthru.get_name(),
            'port': 'IN'
        }
    }) in messages
    assert ('addedge', {
        'graph': graph_id,
        'src': {
            'node': outside.get_name(),
            'port': 'OUT'
        },
        'tgt': {
            'node': passthru.get_name(),
            'port': 'IN'
        }
    }) in messages

    assert ('addinitial', {
        'graph': graph_id,
        'src': {
            'data': 5,
        },
        'tgt': {
            'node': gen.get_name(),
            'port': 'COUNT'
        }
    }) in messages

    assert ('addinport', {
        'graph': graph_id,
        'public': 'INPORT',
        'node': outside.get_name(),
        'port': 'IN',
        'metadata': {}
    }) in messages
    assert ('addoutport', {
        'graph': graph_id,
        'public': 'OUTPORT',
        'node': passthru.get_name(),
        'port': 'OUT',
        'metadata': {}
    }) in messages


