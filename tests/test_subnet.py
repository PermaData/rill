import pytest

import rill.engine.utils
rill.engine.utils.patch()

from rill.engine.network import Network, Graph, run_graph
from rill.engine.runner import ComponentRunner
from rill.components.basic import Capture
from rill.components.timing import SlowPass
from tests.components import *
from tests.subnets import PassthruNet
from tests.test_components import graph, discard

import logging
ComponentRunner.logger.setLevel(logging.DEBUG)


# we use socket as our canary
is_patched = rill.engine.utils.is_patched
requires_patch = pytest.mark.skipif(not is_patched,
                                    reason='requires patched gevent')


@requires_patch
def test_subnet_with_substreams(graph, discard):
    # tracing = True
    gen = graph.add_component("Generate", GenSS, COUNT=15)
    passnet = graph.add_component("Subnet", PassthruNet)
    dis = graph.add_component("Discard", discard)

    graph.connect("Generate.OUT", "Subnet.IN")
    graph.connect("Subnet.OUT", "Discard.IN")

    # graph.reset()
    # graph._build_runners()
    # graph._open_ports()
    # assert isinstance(passnet.ports['OUT'].component, Passthru)
    # assert isinstance(passnet.ports['OUT'].sender, ComponentRunner)
    # assert isinstance(passnet.ports['IN'].component, Passthru)
    # assert isinstance(passnet.ports['IN'].receiver, ComponentRunner)


    # FIXME: need a separate test for the NULL port behavior
    # graph.connect("Subnet.*SUBEND", "WTC.IN")
    run_graph(graph)

    assert dis.values == [
        '', '000015', '000014', '000013', '000012', '000011', '',
        '', '000010', '000009', '000008', '000007', '000006', '',
        '', '000005', '000004', '000003', '000002', '000001', ''
    ]


def test_subnet_decorator():
    @outport("OUT")
    @inport("IN", description='an input')
    @subnet
    def DecoratedPassNet(sub):
        sub.add_component('Head', SlowPass, DELAY=0.01)
        sub.add_component('Tail', SlowPass, DELAY=0.01)

        sub.connect('Head.OUT', 'Tail.IN')

        sub.export('Head.IN', 'IN')
        sub.export('Tail.OUT', 'OUT')

    assert issubclass(DecoratedPassNet, SubGraph)
    assert DecoratedPassNet._inport_definitions[0].description == 'an input'

    graph = Graph()

    gen = graph.add_component("Generate", GenSS, COUNT=5)
    passnet = graph.add_component("Subnet", DecoratedPassNet)
    dis = graph.add_component("Discard", Discard)

    graph.connect("Generate.OUT", "Subnet.IN")
    graph.connect("Subnet.OUT", "Discard.IN")

    run_graph(graph)

    assert dis.values == [
        '', '000005', '000004', '000003', '000002', '000001', '',
    ]


def test_name():
    graph = Graph()
    passnet = graph.add_component("Subnet", PassthruNet)
    child = passnet.subgraph.component("Pass")
    assert child.get_full_name() == 'Pass'

    # a component's full name is not available until all of the graphs' and
    # sub-graphs' runners have been initialized.
    # this is the result of two conflicting requirements:
    #  - avoid adding state to graphs.  they should be reusable without being
    #    polluted
    #  - delay building runners until absolutely necessary
    # we might want to revisit this if it becomes a nuisance.
    net = Network(graph)
    net._build_runners()
    snet = Network(passnet.subgraph)
    snet.parent_network = passnet._runner.parent_network
    assert child.get_full_name() == 'Subnet.Pass'


def test_initialize_subnet():
    @outport("OUT")
    @inport("IN")
    @subnet
    def PassNet(sub):
        sub.add_component('Head', Passthru)
        sub.add_component('Tail', Passthru)

        sub.connect('Head.OUT', 'Tail.IN')

        sub.export('Head.IN', 'IN')
        sub.export('Tail.OUT', 'OUT')

    graph = Graph()
    capture = graph.add_component('Capture', Capture)

    graph.add_component('Pass', PassNet)
    graph.initialize(5, 'Pass.IN')
    graph.connect('Pass.OUT', 'Capture.IN')

    run_graph(graph)

    assert capture.value == 5


