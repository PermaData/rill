import pytest

from rill.engine.network import Graph

from tests.components import *
from tests.subnets import PassthruNet
from tests.utils import names

from rill.engine.exceptions import FlowError
from rill.engine.outputport import OutputPort, OutputArray
from rill.engine.inputport import InputPort, InputArray
from rill.components.basic import Counter
from rill.components.merge import Group
from rill.components.timing import SlowPass


@pytest.fixture
def serialized_graph():
    definition = {
        'processes': {
            'Counter1': {
                'component': 'rill.components.basic/Counter',
                'metadata': {
                    'x': 20.0,
                    'y': 300.5
                }
            },
            'Discard1': {
                'component': 'tests.components/Discard',
                'metadata': {}
            },
            'Pass': {
                'component': 'tests.subnets/PassthruNet',
                'metadata': {}
            },
            'Generate': {
                'component': 'tests.components/GenerateArray',
                'metadata': {}
            },
            'Merge': {
                'component': 'rill.components.merge/Group',
                'metadata': {}
            }
        },
        'connections': [
            {
                'src': {'data': 5},
                'tgt': {
                    'process': 'Counter1',
                    'port': 'IN'
                }
            },
            {
                'src': {
                    'process': 'Counter1',
                    'port': 'OUT'
                },
                'tgt': {
                    'process': 'Pass',
                    'port': 'IN'
                }
            },
            {
                'src': {
                    'process': 'Pass',
                    'port': 'OUT'
                },
                'tgt': {
                    'process': 'Discard1',
                    'port': 'IN'
                }
            },
            {
                'src': {
                    'process': 'Generate',
                    'port': 'OUT',
                    'index': 0
                },
                'tgt': {
                    'process': 'Merge',
                    'port': 'IN',
                    'index': 1
                }
            },
            {
                'src': {
                    'process': 'Generate',
                    'port': 'OUT',
                    'index': 1
                },
                'tgt': {
                    'process': 'Merge',
                    'port': 'IN',
                    'index': 2
                }
            }
        ],
        'inports': {},
        'outports': {}
    }
    definition['connections'] = sorted(
        definition['connections'],
        key=str
    )
    return definition


def test_network_serialization(serialized_graph):
    graph = Graph()

    counter = graph.add_component('Counter1', Counter)
    counter.metadata.update({
        'x': 20.0,
        'y': 300.5
    })
    graph.add_component('Pass', PassthruNet)
    graph.add_component('Discard1', Discard)
    graph.add_component('Generate', GenerateArray)
    graph.add_component("Merge", Group)
    graph.connect('Counter1.OUT', 'Pass.IN')
    graph.connect('Pass.OUT', 'Discard1.IN')
    graph.connect("Generate.OUT[0]", "Merge.IN[1]")
    graph.connect("Generate.OUT[1]", "Merge.IN[2]")

    graph.initialize(5, "Counter1.IN")

    assert len(graph.get_components().keys()) == 5

    definition = graph.to_dict()

    # Order of connections array shouldn't matter
    definition['connections'] = sorted(definition['connections'], key=str)

    assert definition == serialized_graph


def test_network_deserialization(serialized_graph):

    graph = Graph.from_dict(serialized_graph, {
        'rill.components.basic/Counter': Counter,
        'rill.components.merge/Group': Group,
        'tests.components/Discard': Discard,
        'tests.subnets/PassthruNet': PassthruNet,
        'tests.components/GenerateArray': GenerateArray
    })

    assert len(graph.get_components().keys()) == 5

    Counter1 = graph.get_component('Counter1')
    Discard1 = graph.get_component('Discard1')
    Pass = graph.get_component('Pass')
    Generate = graph.get_component('Generate')
    Merge = graph.get_component('Merge')

    assert Counter1.ports.OUT._connections[0].inport.component is Pass
    assert Counter1.metadata == {
        'x': 20.0,
        'y': 300.5
    }
    assert Pass.ports.OUT._connections[0].inport.component is Discard1
    assert Counter1.ports.IN._connection._content is 5

    assert (
        Generate.ports.OUT.get_element(0)._connections[0].inport.component is Merge
    )

    assert (
        Generate.ports.OUT.get_element(1)._connections[0].inport.component is Merge
    )

    assert (
        Generate.ports.OUT.get_element(0)._connections[0].inport.index is 1
    )

    assert (
        Generate.ports.OUT.get_element(1)._connections[0].inport.index is 2
    )

    expected = graph.to_dict()

    # Order of connections array shouldn't matter
    expected['connections'] = sorted(expected['connections'], key=str)

    assert serialized_graph == expected


def test_network_export():
    graph = Graph()
    passthru = graph.add_component("Pass", SlowPass, DELAY=0.1)

    graph.export('Pass.OUT', 'OUT')
    graph.export('Pass.IN', 'IN')

    assert len(graph.inports.keys()) == 1
    assert len(graph.outports.keys()) == 1


def test_export_serialization():
    graph = Graph()

    graph.add_component('Head', SlowPass, DELAY=0.01)
    graph.add_component('Tail', SlowPass, DELAY=0.01)

    graph.connect('Head.OUT', 'Tail.IN')

    graph.export('Head.IN', 'IN')
    graph.export('Tail.OUT', 'OUT')

    definition = graph.to_dict()
    expected = {
        'processes': {
            'Head': {
                'component': 'rill.components.timing/SlowPass',
                'metadata': {}
            },
            'Tail': {
                'component': 'rill.components.timing/SlowPass',
                'metadata': {}
            }
        },
        'connections': [
            {
                'src': {'data': 0.01},
                'tgt': {
                    'process': 'Head',
                    'port': 'DELAY'
                }
            },
            {
                'src': {'data': 0.01},
                'tgt': {
                    'process': 'Tail',
                    'port': 'DELAY'
                }
            },
            {
                'src': {
                    'process': 'Head',
                    'port': 'OUT'
                },
                'tgt': {
                    'process': 'Tail',
                    'port': 'IN'
                }
            }
        ],
        'inports': {
            'IN': {
                'process': 'Head',
                'port': 'IN'
            }
        },
        'outports': {
            'OUT': {
                'process': 'Tail',
                'port': 'OUT'
            }
        }
    }

    # Order of connections array shouldn't matter
    definition['connections'] = sorted(definition['connections'], key=str)
    expected['connections'] = sorted(expected['connections'], key=str)

    assert definition == expected


def test_export_of_exports():
    definition = {
        'processes': {
            'Head': {
                'component': 'rill.components.timing/SlowPass',
                'metadata': {}
            },
            'Tail': {
                'component': 'rill.components.timing/SlowPass',
                'metadata': {}
            }
        },
        'connections': [
            {
                'src': {'data': 0.01},
                'tgt': {
                    'process': 'Head',
                    'port': 'DELAY'
                }
            },
            {
                'src': {'data': 0.01},
                'tgt': {
                    'process': 'Tail',
                    'port': 'DELAY'
                }
            },
            {
                'src': {
                    'process': 'Head',
                    'port': 'OUT'
                },
                'tgt': {
                    'process': 'Tail',
                    'port': 'IN'
                }
            }
        ],
        'inports': {
            'IN': {
                'process': 'Head',
                'port': 'IN'
            }
        },
        'outports': {
            'OUT': {
                'process': 'Tail',
                'port': 'OUT'
            }
        }
    }

    graph = Graph.from_dict(definition, {
        'rill.components.timing/SlowPass': SlowPass
    })

    Head = graph.get_component('Head')
    Tail = graph.get_component('Tail')

    assert Head.ports.OUT._connections[0].inport.component is Tail

    expected = graph.to_dict()

    # Order of connections array shouldn't matter
    definition['connections'] = sorted(definition['connections'], key=str)
    expected['connections'] = sorted(expected['connections'], key=str)

    assert definition == expected


def test_static_type_validation():
    """When initializing a port to a static value, the type is immediately
    validated"""
    graph = Graph()

    with pytest.raises(FlowError):
        graph.add_component("Repeat", Repeat, COUNT='foo')


def test_fixed_array_connections():
    graph = Graph()
    gen = graph.add_component("Generate", GenerateFixedSizeArray)
    dis1 = graph.add_component("Discard1", Discard)
    graph.add_component("Discard2", Discard)

    assert gen.ports['OUT'].get_full_name() == 'Generate.OUT'

    # fixed array ports create their ports immediately
    assert gen.ports.OUT.fixed_size == 2
    assert names(gen.outports) == ['OUT[0]', 'OUT[1]']
    # assert list(gen.ports._ports.keys()) == ['OUT', 'NULL']
    assert type(gen.ports['OUT']) is OutputArray
    assert type(dis1.ports['IN']) is InputPort

    # nothing is connected yet
    # assert gen.ports['OUT'].is_connected() is False
    assert dis1.ports['IN'].is_connected() is False

    # make a connection
    graph.connect("Generate.OUT[1]", "Discard1.IN")
    assert names(gen.outports) == ['OUT[0]', 'OUT[1]']
    assert gen.ports['OUT'][1].is_connected() is True

    # uses first unconnected index (index 0)
    graph.connect("Generate.OUT", "Discard2.IN")
    assert gen.ports['OUT'][0].is_connected() is True

    with pytest.raises(FlowError):
        # outports can only have one connection
        graph.connect("Generate.OUT[1]", "Discard2.IN")

    with pytest.raises(FlowError):
        # cannot connect outside the fixed range
        graph.connect("Generate.OUT[2]", "Discard2.IN")

    assert type(gen.ports['OUT']) is OutputArray
    assert type(gen.ports['OUT'][0]) is OutputPort
    assert type(dis1.ports['IN']) is InputPort
    # assert gen.ports['OUT'].is_connected() is False
    assert dis1.ports['IN'].is_connected() is True


def test_array_connections():
    graph = Graph()
    gen = graph.add_component("Generate", GenerateArray)
    dis1 = graph.add_component("Discard1", Discard)
    graph.add_component("Discard2", Discard)

    assert gen.ports['OUT'].get_full_name() == 'Generate.OUT'

    # non-fixed array ports delay element creation
    assert gen.ports.OUT.fixed_size is None
    assert names(gen.outports) == []
    # assert list(gen.ports._ports.keys()) == ['OUT', 'NULL']
    assert type(gen.ports['OUT']) is OutputArray
    assert type(dis1.ports['IN']) is InputPort

    # nothing is connected yet
    # assert gen.ports['OUT'].is_connected() is False
    assert dis1.ports['IN'].is_connected() is False

    # make a connection
    graph.connect("Generate.OUT[1]", "Discard1.IN")
    assert names(gen.outports) == ['OUT[1]']
    assert gen.ports['OUT'][1].is_connected() is True

    # uses first unused index (index 0)
    graph.connect("Generate.OUT", "Discard2.IN")
    assert names(gen.outports) == ['OUT[0]', 'OUT[1]']
    assert gen.ports['OUT'][0].is_connected() is True

    assert type(gen.ports['OUT']) is OutputArray
    assert type(gen.ports['OUT'][0]) is OutputPort
    assert type(dis1.ports['IN']) is InputPort
    # assert gen.ports['OUT'].is_connected() is False
    assert dis1.ports['IN'].is_connected() is True


def test_required_port_error():
    graph = Graph()
    graph.add_component("Generate", GenerateFixedSizeArray)
    graph.add_component("Discard1", Discard)
    with pytest.raises(FlowError):
        graph.validate()


def test_required_array_error():
    """if fixed_size is provided and the array port is required, all elements
    must be connected"""
    graph = Graph()
    gen = graph.add_component("Generate", GenerateFixedSizeArray)
    assert gen.ports['OUT'].required is True
    graph.add_component("Discard1", Discard)
    graph.connect("Generate.OUT[0]", "Discard1.IN")
    assert names(gen.outports) == ['OUT[0]', 'OUT[1]']

    gen.init()
    with pytest.raises(FlowError):
        gen.ports.OUT.open()


def test_add_component():
    graph = Graph()
    gen = graph.add_component("generate", GenerateTestData, COUNT=10)
    assert type(gen.ports.COUNT) is InputPort
    assert gen.ports.COUNT.is_static()
    assert gen.ports.COUNT._connection._content == 10

    with pytest.raises(FlowError):
        graph.add_component("generate", GenerateTestData)

    def foo(): pass

    with pytest.raises(TypeError):
        graph.add_component("not_component", foo)


def test_optional_fixed_size_array_error():
    """if an array port specifies fixed_size, all elements must be
    connected if the array port is required"""
    graph = Graph()
    gen = graph.add_component("Generate", GenerateFixedSizeArray)
    assert gen.ports['OUT'].required is True
    gen.init()
    with pytest.raises(FlowError):
        gen.ports.OUT.open()
