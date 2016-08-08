from rill.events.dispatchers.base import GraphDispatcher
from rill.events.dispatchers.memory import InMemoryGraphDispatcher
from rill.events.listeners.memory import InMemoryGraphListener
from rill.utils.observer import supports_listeners, Event

from tests.components import *
import logging
from mock import MagicMock

logger = logging.getLogger(__name__)


def test_supports_listeners():
    class TestObject(object):
        def __init__(self, prop):
            self.prop = prop

        @supports_listeners
        def test_method(self):
            self.test_method.event.emit(self.prop)
            return self.prop

    object1 = TestObject(1)
    object2 = TestObject(2)

    # assert object1.test_method is object1.test_method
    assert object1.test_method.event is object1.test_method.event
    assert object1.test_method.event is not object2.test_method.event

    callback = MagicMock()
    object1.test_method.event.listen(callback)
    assert object1.test_method.event is not object2.test_method.event

    assert object1.test_method.event is object1.test_method.event

    assert object1.test_method() == 1
    assert object2.test_method() == 2
    callback.assert_called_once()


class TestGraphDispatcher(GraphDispatcher):
    new_graph = MagicMock()
    add_node = MagicMock()
    set_graph_metadata = MagicMock()
    rename_graph = MagicMock()
    remove_node = MagicMock()
    rename_node = MagicMock()
    set_node_metadata = MagicMock()
    add_edge = MagicMock()
    remove_edge = MagicMock()
    set_edge_metadata = MagicMock()
    initialize_port = MagicMock()
    uninitialize_port = MagicMock()
    add_inport = MagicMock()
    remove_inport = MagicMock()
    set_inport_metadata = MagicMock()
    add_outport = MagicMock()
    remove_outport = MagicMock()
    set_outport_metadata = MagicMock()


def test_memory_graph_dispatcher():
    graph_id = 'test_graph'

    dispatcher = InMemoryGraphDispatcher()

    dispatcher._component_types['tests.components/Passthru'] = {
        'class': Passthru,
        'spec': Passthru.get_spec()
    }
    dispatcher.logger = logger

    graph = dispatcher.new_graph({'id': graph_id})
    assert dispatcher.get_graph(graph_id) == graph

    test_dispatcher = TestGraphDispatcher()
    listener = InMemoryGraphListener(graph, [test_dispatcher])
    listener.snapshot()

    test_dispatcher.new_graph.assert_called_once_with({
        'id': graph_id,
        'name': graph.name
    })

    dispatcher.set_graph_metadata({
        'graph': graph_id,
        'metadata': {
            'description': 'this is a description'
        }
    })
    assert graph.metadata == {
        'description': 'this is a description'
    }
    test_dispatcher.set_graph_metadata.assert_called_with({
        'graph': graph_id,
        'metadata': {
            'description': 'this is a description'
        }
    })

    new_graph_id = 'hello'
    dispatcher.rename_graph({
        'from': graph_id,
        'to': new_graph_id
    })
    test_dispatcher.rename_graph.assert_called_with({
        'from': graph_id,
        'to': new_graph_id
    })
    graph_id = new_graph_id

    node = dispatcher.add_node({
        'graph': graph_id,
        'id': 'pass',
        'component': 'tests.components/Passthru',
        'metadata': {
            'x': 100,
            'y': 200,
            'label': 'test'
        }
    })
    assert node.get_name() == 'pass'
    test_dispatcher.add_node.assert_called_once_with({
        'graph': graph_id,
        'id': 'pass',
        'component': 'tests.components/Passthru',
        'metadata': {
            'x': 100,
            'y': 200,
            'label': 'test'
        }
    })
    assert node.metadata == {
        'x': 100,
        'y': 200,
        'label': 'test'
    }

    dispatcher.set_node_metadata({
        'graph': graph_id,
        'id': 'pass',
        'metadata': {
            'x': 6,
            'label': None
        }
    })
    assert node.metadata == {
        'x': 6,
        'y': 200
    }

    test_dispatcher.set_node_metadata.assert_called_once_with({
        'graph': graph_id,
        'id': 'pass',
        'metadata': {
            'x': 6,
            'label': None
        }
    })

    node2 = dispatcher.add_node({
        'graph': graph_id,
        'id': 'pass2',
        'component': 'tests.components/Passthru'
    })
    assert node2.get_name() == 'pass2'
    test_dispatcher.add_node.assert_called_with({
        'graph': graph_id,
        'id': 'pass2',
        'component': 'tests.components/Passthru',
        'metadata': {}
    })

    edge = dispatcher.add_edge({
        'graph': graph_id,
        'src': {
            'node': 'pass',
            'port': 'OUT'
        },
        'tgt': {
            'node': 'pass2',
            'port': 'IN'
        },
        'metadata': {
            'route': 6
        }
    })
    test_dispatcher.add_edge.assert_called_with({
        'graph': graph_id,
        'src': {
            'node': 'pass',
            'port': 'OUT'
        },
        'tgt': {
            'node': 'pass2',
            'port': 'IN'
        },
        'metadata': {
            'route': 6
        }
    })

    dispatcher.set_edge_metadata({
        'graph': graph_id,
        'src': {
            'node': 'pass',
            'port': 'OUT'
        },
        'tgt': {
            'node': 'pass2',
            'port': 'IN'
        },
        'metadata': {
            'route': None,
            'label': 'hello'
        }
    })
    test_dispatcher.set_edge_metadata.assert_called_with({
        'graph': graph_id,
        'src': {
            'node': 'pass',
            'port': 'OUT'
        },
        'tgt': {
            'node': 'pass2',
            'port': 'IN'
        },
        'metadata': {
            'label': 'hello',
            'route': None
        }
    })

    dispatcher.initialize_port({
        'graph': graph_id,
        'src': {
            'data': [10]
        },
        'tgt': {
            'node': node.get_name(),
            'port': 'IN'
        }
    })
    assert node.ports['IN']._connection._content == [10]
    test_dispatcher.initialize_port.assert_called_with({
        'graph': graph_id,
        'src': {
            'data': [10]
        },
        'tgt': {
            'node': node.get_name(),
            'port': 'IN'
        }
    })

    dispatcher.uninitialize_port({
        'graph': graph_id,
        'tgt': {
            'node': node.get_name(),
            'port': 'IN'
        }
    })
    assert not node.ports['IN']._connection
    test_dispatcher.uninitialize_port.assert_called_with({
        'graph': graph_id,
        'tgt': {
            'node': node.get_name(),
            'port': 'IN'
        }
    })

    dispatcher.add_inport({
        'graph': graph_id,
        'node': node.get_name(),
        'port': 'IN',
        'public': 'IN',
        'metadata': {}
    })
    assert graph.inports['IN'] == node.ports['IN']
    test_dispatcher.add_inport.assert_called_with({
        'graph': graph_id,
        'node': node.get_name(),
        'port': 'IN',
        'public': 'IN',
        'metadata': {}
    })

    dispatcher.set_inport_metadata({
        'graph': graph_id,
        'public': 'IN',
        'metadata': {
            'x': 10,
            'y': 10
        }
    })
    test_dispatcher.set_inport_metadata.assert_called_with({
        'graph': graph_id,
        'public': 'IN',
        'metadata': {
            'x': 10,
            'y': 10
        }
    })

    dispatcher.remove_inport({
        'graph': graph_id,
        'public': 'IN'
    })
    assert len(graph.inports.keys()) == 0
    test_dispatcher.remove_inport.assert_called_with({
        'graph': graph_id,
        'public': 'IN'
    })

    dispatcher.add_outport({
        'graph': graph_id,
        'node': node2.get_name(),
        'port': 'OUT',
        'public': 'OUT',
        'metadata': {}
    })
    assert graph.outports['OUT'] == node2.ports['OUT']
    test_dispatcher.add_outport.assert_called_with({
        'graph': graph_id,
        'node': node2.get_name(),
        'port': 'OUT',
        'public': 'OUT',
        'metadata': {}
    })

    dispatcher.set_outport_metadata({
        'graph': graph_id,
        'public': 'OUT',
        'metadata': {
            'x': 10,
            'y': 10
        }
    })
    test_dispatcher.set_outport_metadata.assert_called_with({
        'graph': graph_id,
        'public': 'OUT',
        'metadata': {
            'x': 10,
            'y': 10
        }
    })

    dispatcher.remove_outport({
        'graph': graph_id,
        'public': 'OUT'
    })
    assert len(graph.outports.keys()) == 0
    test_dispatcher.remove_outport.assert_called_with({
        'graph': graph_id,
        'public': 'OUT'
    })

    dispatcher.remove_edge({
        'graph': graph_id,
        'src': {
            'node': 'pass',
            'port': 'OUT'
        },
        'tgt': {
            'node': 'pass2',
            'port': 'IN'
        }
    })
    assert not node.ports['OUT']._connection
    assert not node.ports['OUT'].is_connected()
    test_dispatcher.remove_edge.assert_called_with({
        'graph': graph_id,
        'src': {
            'node': 'pass',
            'port': 'OUT'
        },
        'tgt': {
            'node': 'pass2',
            'port': 'IN'
        }
    })

    dispatcher.remove_node({
        'graph': graph_id,
        'id': 'pass'
    })
    test_dispatcher.remove_node.assert_called_with({
        'graph': graph_id,
        'id': 'pass'
    })
