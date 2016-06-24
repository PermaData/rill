import pytest

import rill.engine.utils
rill.engine.utils.patch()

from rill.engine.network import Network, apply_network
from rill.engine.runner import ComponentRunner
from rill.components.basic import Capture
from rill.components.timing import SlowPass
from tests.components import *
from tests.subnets import PassthruNet
from tests.test_components import net, discard

import logging
ComponentRunner.logger.setLevel(logging.DEBUG)


# we use socket as our canary
is_patched = rill.engine.utils.is_patched
requires_patch = pytest.mark.skipif(not is_patched,
                                    reason='requires patched gevent')


@requires_patch
def test_subnet_with_substreams(net, discard):
    # tracing = True
    gen = net.add_component("Generate", GenSS, COUNT=15)
    passnet = net.add_component("Subnet", PassthruNet)
    dis = net.add_component("Discard", discard)

    net.connect("Generate.OUT", "Subnet.IN")
    net.connect("Subnet.OUT", "Discard.IN")

    # net.reset()
    # net._build_runners()
    # net._open_ports()
    # assert isinstance(passnet.ports['OUT'].component, Passthru)
    # assert isinstance(passnet.ports['OUT'].sender, ComponentRunner)
    # assert isinstance(passnet.ports['IN'].component, Passthru)
    # assert isinstance(passnet.ports['IN'].receiver, ComponentRunner)


    # FIXME: need a separate test for the NULL port behavior
    # net.connect("Subnet.*SUBEND", "WTC.IN")
    net.go()

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

    assert issubclass(DecoratedPassNet, SubNet)
    assert DecoratedPassNet.inport_definitions[0].description == 'an input'

    net = Network()

    gen = net.add_component("Generate", GenSS, COUNT=5)
    passnet = net.add_component("Subnet", DecoratedPassNet)
    dis = net.add_component("Discard", Discard)

    net.connect("Generate.OUT", "Subnet.IN")
    net.connect("Subnet.OUT", "Discard.IN")

    net.go()

    assert dis.values == [
        '', '000005', '000004', '000003', '000002', '000001', '',
    ]


def test_name():
    net = Network()
    passnet = net.add_component("Subnet", PassthruNet)
    child = passnet.sub_network.component("Pass")
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

    net = Network()
    capture = net.add_component('Capture', Capture)

    net.add_component('Pass', PassNet)
    net.initialize(5, 'Pass.IN')
    net.connect('Pass.OUT', 'Capture.IN')

    net.go()

    assert capture.value == 5


