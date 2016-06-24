import pytest

import gevent.monkey
import gevent

import rill.engine.utils
rill.engine.utils.patch()

from rill.engine.exceptions import FlowError
from rill.engine.network import Network, apply_network
from rill.engine.outputport import OutputPort, OutputArray
from rill.engine.inputport import InputPort, InputArray
from rill.engine.runner import ComponentRunner
from rill.engine.component import Component
from rill.decorators import inport, outport, component, subnet

from tests.components import *
from tests.subnets import PassthruNet

from rill.components.basic import Counter, Sort, Inject, Repeat, Cap, Kick
from rill.components.filters import First
from rill.components.merge import Group
from rill.components.split import RoundRobinSplit, Replicate
from rill.components.math import Add
from rill.components.files import ReadLines, WriteLines, Write
from rill.components.timing import SlowPass
from rill.components.text import Prefix, LineToWords, LowerCase, StartsWith, WordsToLine

import logging
ComponentRunner.logger.setLevel(logging.DEBUG)


is_patched = rill.engine.utils.is_patched
requires_patch = pytest.mark.skipif(not is_patched,
                                    reason='requires patched gevent')

# TODO
# - test inport closing while it is waiting in a `while self.is_empty()` loop
# - define what happens when sending to a closed InputInterface: closed state
#   currently seems to be disregarded
# - test whether an InitializationConnection repeats the same value: it
#   re-opens each time it is activated, so that suggests it is the behavior, but
#   that implies that a component written to use an InitConn won't work with
#   a Conn. i.e. if a component closes that connection, there should be no more
#   values, regardless of whether it is an InitConn or Conn
# - test differences between loopers and non-loopers.
#     - is reactivating a looper safe? for example, what happens when a looper
#       calls receive_once() on a Connection, but is later re-activated?  does
#       it get the value again?
#     - what about NULL ports. do they fire once or multiple times?


def names(ports):
    return [x.name for x in ports]


def run(network, *pairs):
    """Run a network twice to ensure that it shuts down properly"""
    with gevent.Timeout(2):
        network.go()
        for real, ref in pairs:
            assert real.values == ref
            real.values = []
        network.go()
        for real, ref in pairs:
            assert real.values == ref


@pytest.fixture(params=[dict(default_capacity=1),
                        dict(default_capacity=2)],
                ids=['net.capacity=1', 'net.capacity=2'])
def net(request):
    return Network(**request.param)


# non-loopers are only safe when using gevent.monkey.patch_all().
# FIXME: find out why... OR stop supporting component reactivation (i.e. non-loopers)
@pytest.fixture(params=['looper', requires_patch('nonlooper')])
def discard(request):
    return DiscardLooper if request.param == 'looper' else Discard


def test_aggregation(net, discard):
    net.add_component("Generate", GenerateTestData)
    dis = net.add_component("Discard", discard)
    net.add_component("Counter", Counter)

    net.connect("Generate.OUT", "Counter.IN")
    net.connect("Counter.COUNT", "Discard.IN")
    net.initialize(5, "Generate.COUNT")
    run(net, (dis, [5]))


@requires_patch
def test_pickle(net):
    net.add_component("Generate", GenerateTestData, COUNT=5)
    passthru = net.add_component("Pass", SlowPass, DELAY=0.1)
    count = net.add_component("Counter", Counter)
    dis1 = net.add_component("Discard1", Discard)
    dis2 = net.add_component("Discard2", Discard)

    net.connect("Generate.OUT", "Pass.IN")
    net.connect("Pass.OUT", "Counter.IN")
    net.connect("Counter.COUNT", "Discard1.IN")
    net.connect("Counter.OUT", "Discard2.IN")

    netrunner = gevent.spawn(net.go)
    try:
        with gevent.Timeout(.35) as timeout:
            gevent.wait([netrunner])
    except gevent.Timeout:
        print(count.execute)

    assert count.count == 4
    assert dis2.values == ['000005', '000004', '000003', '000002']

    import pickle
    # dump before terminating to get the runner statuses
    data = pickle.dumps(net)
    # FIXME: do we need to auto-terminate inside wait_for_all if there is an error?
    net.terminate()
    net.wait_for_all()
    # gevent.wait([netrunner])  # this causes more packets to be sent. no good.
    net2 = pickle.loads(data)
    assert net2.component('Counter').count == 4
    assert net2.component('Discard2').values == ['000005', '000004', '000003', '000002']
    net2.go(resume=True)
    assert net2.component('Counter').count == 5
    assert net2.component('Discard2').values == ['000005', '000004', '000003', '000002', '000001']

    # FIXME: test the case where a packet is lost due to being shut-down
    # packet counting should catch it.  to test, use this component, which
    # can be killed while it sleeps holding a packet:
    # @component
    # @outport("OUT")
    # @inport("IN")
    # @inport("DELAY", type=float, required=True)
    # def SlowPass(IN, DELAY, OUT):
    #     """
    #     Pass a stream of packets to an output stream with a delay between packets
    #     """
    #     delay = DELAY.receive_once()
    #     for p in IN:
    #         time.sleep(delay)
    #         OUT.send(p)


def test_static_type_validation():
    """When initializing a port to a static value, the type is immediately
    validated"""
    net = Network()

    with pytest.raises(FlowError):
        net.add_component("Repeat", Repeat, COUNT='foo')


def test_component_with_inheritance():
    @inport('IN')
    @outport('OUT')
    class A(Component):
        def execute(self, IN, OPT, OUT):
            pass

    @inport('OPT', type=int)
    class B(A):
        pass

    assert names(B.port_definitions()) == ['IN_NULL', 'IN', 'OPT', 'OUT_NULL',
                                           'OUT']

    net = Network()
    b = net.add_component('b', B)
    assert names(b.inports) == ['IN', 'OPT']


@pytest.mark.xfail(is_patched, reason='order is ACB instead of ABC')
# @requires_patch
def test_multiple_inputs(net, discard):
    net.add_component("GenerateA", GenerateTestData, COUNT=5)
    net.add_component("PrefixA", Prefix, PRE='A')
    net.add_component("GenerateB", GenerateTestData, COUNT=5)
    net.add_component("PrefixB", Prefix, PRE='B')
    net.add_component("GenerateC", GenerateTestData, COUNT=5)
    net.add_component("PrefixC", Prefix, PRE='C')
    dis = net.add_component("Discard", discard)

    net.connect("GenerateA.OUT", "PrefixA.IN")
    net.connect("GenerateB.OUT", "PrefixB.IN")
    net.connect("GenerateC.OUT", "PrefixC.IN")
    net.connect("PrefixA.OUT", "Discard.IN")
    net.connect("PrefixB.OUT", "Discard.IN")
    net.connect("PrefixC.OUT", "Discard.IN")
    run(net,
        (dis,
         [
          'A000005', 'A000004', 'A000003', 'A000002', 'A000001',
          'B000005', 'B000004', 'B000003', 'B000002', 'B000001',
          'C000005', 'C000004', 'C000003', 'C000002', 'C000001',
         ]))


def test_intermediate_non_looper(net, discard):
    """non-looping components continue processing as long as there are
    upstream packets"""
    net.add_component("Generate", GenerateTestData)
    net.add_component("Passthru", Passthru)
    dis = net.add_component("Discard", discard)

    net.connect("Generate.OUT", "Passthru.IN")
    # Passthru is a non-looper
    net.connect("Passthru.OUT", "Discard.IN")
    net.initialize(5, "Generate.COUNT")
    run(net,
        (dis, ['000005', '000004', '000003', '000002', '000001']))


def test_basic_connections():
    net = Network()
    count = net.add_component("Count", Counter)
    dis = net.add_component("Discard1", Discard)
    net.add_component("Discard2", Discard)

    # ports are stored in the order they are declared
    assert names(count.outports) == ['OUT', 'COUNT']
    # assert list(count.ports._ports.keys()) == ['OUT', 'COUNT', 'NULL']

    # nothing is connected yet
    assert count.ports['OUT'].is_connected() is False
    assert dis.ports['IN'].is_connected() is False

    with pytest.raises(FlowError):
        # non-existent port
        net.connect("Count.FOO", "Discard1.IN")

    with pytest.raises(FlowError):
        # non-existent Component
        net.connect("Foo.FOO", "Discard1.IN")

    # make a connection
    net.connect("Count.OUT", "Discard1.IN")

    with pytest.raises(FlowError):
        # outports can only have one connection
        net.connect("Count.OUT", "Discard2.IN")

    with pytest.raises(FlowError):
        # connected ports cannot be initialized
        net.initialize(1, "Discard1.IN")

    assert type(count.ports['OUT']) is OutputPort
    assert type(dis.ports['IN']) is InputPort
    assert count.ports['OUT'].is_connected() is True
    assert dis.ports['IN'].is_connected() is True

    net.reset()
    net._build_runners()
    net._open_ports()
    assert count.ports['OUT'].component is count
    assert isinstance(count.ports['OUT'].sender, ComponentRunner)
    assert dis.ports['IN'].component is dis
    assert isinstance(dis.ports['IN'].receiver, ComponentRunner)


def test_fixed_array_connections():
    net = Network()
    gen = net.add_component("Generate", GenerateFixedSizeArray)
    dis1 = net.add_component("Discard1", Discard)
    net.add_component("Discard2", Discard)

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
    net.connect("Generate.OUT[1]", "Discard1.IN")
    assert names(gen.outports) == ['OUT[0]', 'OUT[1]']
    assert gen.ports['OUT'][1].is_connected() is True

    # uses first unconnected index (index 0)
    net.connect("Generate.OUT", "Discard2.IN")
    assert gen.ports['OUT'][0].is_connected() is True

    with pytest.raises(FlowError):
        # outports can only have one connection
        net.connect("Generate.OUT[1]", "Discard2.IN")

    with pytest.raises(FlowError):
        # cannot connect outside the fixed range
        net.connect("Generate.OUT[2]", "Discard2.IN")

    assert type(gen.ports['OUT']) is OutputArray
    assert type(gen.ports['OUT'][0]) is OutputPort
    assert type(dis1.ports['IN']) is InputPort
    # assert gen.ports['OUT'].is_connected() is False
    assert dis1.ports['IN'].is_connected() is True


def test_array_connections():
    net = Network()
    gen = net.add_component("Generate", GenerateArray)
    dis1 = net.add_component("Discard1", Discard)
    net.add_component("Discard2", Discard)

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
    net.connect("Generate.OUT[1]", "Discard1.IN")
    assert names(gen.outports) == ['OUT[1]']
    assert gen.ports['OUT'][1].is_connected() is True

    # uses first unused index (index 0)
    net.connect("Generate.OUT", "Discard2.IN")
    assert names(gen.outports) == ['OUT[0]', 'OUT[1]']
    assert gen.ports['OUT'][0].is_connected() is True

    assert type(gen.ports['OUT']) is OutputArray
    assert type(gen.ports['OUT'][0]) is OutputPort
    assert type(dis1.ports['IN']) is InputPort
    # assert gen.ports['OUT'].is_connected() is False
    assert dis1.ports['IN'].is_connected() is True


def test_required_port_error(net, discard):
    net.add_component("Generate", GenerateFixedSizeArray)
    net.add_component("Discard1", discard)
    with pytest.raises(FlowError):
        net.go()


def test_fixed_size_array(net, discard):
    net.add_component("Generate", GenerateFixedSizeArray)
    dis1 = net.add_component("Discard1", discard)
    dis2 = net.add_component("Discard2", discard)
    net.connect("Generate.OUT[0]", "Discard1.IN")
    net.connect("Generate.OUT[1]", "Discard2.IN")
    net.initialize(5, "Generate.COUNT")
    net.go()
    assert dis1.values == ['000005', '000004', '000003', '000002', '000001']
    assert dis2.values == ['000005', '000004', '000003', '000002', '000001']


def test_required_array_error():
    """if fixed_size is provided and the array port is required, all elements
    must be connected"""
    net = Network()
    gen = net.add_component("Generate", GenerateFixedSizeArray)
    assert gen.ports['OUT'].required is True
    net.add_component("Discard1", Discard)
    net.connect("Generate.OUT[0]", "Discard1.IN")
    assert names(gen.outports) == ['OUT[0]', 'OUT[1]']

    gen.init()
    with pytest.raises(FlowError):
        gen.ports.OUT.open()


def test_unconnected_output_array_element(net, discard):
    """if an array port does not specify fixed_size, some elements may remain
    unconnected
    """
    net.add_component("generate", GenerateTestData)
    net.add_component("replicate", Replicate)
    net.add_component("discard", discard)
    net.connect("generate.OUT", "replicate.IN")
    net.connect("replicate.OUT[2]", "discard.IN")
    net.initialize(10, "generate.COUNT")
    net.go()


def test_add_component():
    net = Network()
    gen = net.add_component("generate", GenerateTestData, COUNT=10)
    assert type(gen.ports.COUNT) is InputPort
    assert gen.ports.COUNT.is_static()
    assert gen.ports.COUNT._connection._content == 10

    with pytest.raises(FlowError):
        net.add_component("generate", GenerateTestData)

    def foo(): pass

    with pytest.raises(TypeError):
        net.add_component("not_component", foo)


def test_optional_fixed_size_array(net, discard):
    """if an array port specifies fixed_size, some elements may remain
    unconnected if the array port is not required"""
    # fixed_size of 4
    net.add_component("Generate", GenerateOptionalFixedArray)
    dis1 = net.add_component("Discard1", discard)
    dis2 = net.add_component("Discard2", discard)
    # only two connected
    net.connect("Generate.OUT", "Discard1.IN")
    net.connect("Generate.OUT[2]", "Discard2.IN")
    net.initialize(5, "Generate.COUNT")
    net.go()

    assert dis1.values == ['000005', '000004', '000003', '000002', '000001']
    assert dis2.values == ['000005', '000004', '000003', '000002', '000001']


def test_optional_fixed_size_array_error():
    """if an array port specifies fixed_size, all elements must be
    connected if the array port is required"""
    net = Network()
    gen = net.add_component("Generate", GenerateFixedSizeArray)
    assert gen.ports['OUT'].required is True
    gen.init()
    with pytest.raises(FlowError):
        gen.ports.OUT.open()


def test_null_ports(net, tmpdir, discard):
    """null ports ensure proper ordering of components"""
    tempfile = str(tmpdir.join('data.txt'))
    net.add_component("Generate", GenerateTestData, COUNT=5)
    net.add_component("Write", WriteLines, FILEPATH=tempfile)
    net.add_component("Read", ReadLines, FILEPATH=tempfile)
    dis = net.add_component("Discard", discard)

    net.connect("Generate.OUT", "Write.IN")
    net.connect("Write.OUT_NULL", "Read.IN_NULL")
    net.connect("Read.OUT", "Discard.IN")
    net.go()

    assert dis.values == ['000005', '000004', '000003', '000002', '000001']


def test_null_ports2(net, tmpdir, discard):
    """null ports ensure proper ordering of components"""
    # in this case, we give Read an input that it can consume, but the null
    # port should prevent it from happening until Write is done
    net.add_component("Data", GenerateTestData, COUNT=2)
    net.add_component("FileNames", GenerateTestData, COUNT=2)
    net.add_component("Prefix", Prefix, PRE=str(tmpdir.join('file.')))
    net.add_component("Replicate", Replicate)
    net.add_component("Write", Write)
    net.add_component("Read", ReadLines)

    dis = net.add_component("Discard", discard)

    net.connect("Data.OUT", "Write.IN")
    net.connect("FileNames.OUT", "Prefix.IN")
    net.connect("Prefix.OUT", "Replicate.IN")
    net.connect("Replicate.OUT[0]", "Write.FILEPATH")
    net.connect("Replicate.OUT[1]", "Read.FILEPATH")

    # Note that it's probably more correct to use Write.OUT which sends after
    # each file written, instead of Write.NULL which sends after all files are
    # written, but we use the latter because it reveals an interesting deadlock
    # issue with Replicate (see notes in that component for more info)
    net.connect("Write.OUT_NULL", "Read.IN_NULL")
    net.connect("Read.OUT", "Discard.IN")
    net.go()

    assert dis.values == ['000002', '000001']


def test_inport_closed(net, discard):
    net.add_component("Generate", GenerateTestData, COUNT=5)
    net.add_component("First", First)
    dis = net.add_component("Discard", discard)

    net.connect("Generate.OUT", "First.IN")
    net.connect("First.OUT", "Discard.IN")
    net.go()

    assert dis.values == ['000005']


def test_inport_closed_propagation(net, discard):
    """If a downstream inport is closed, the upstream component should shut
    down"""
    net.add_component("Generate", GenerateTestDataDumb, COUNT=5)
    net.add_component("First", First)  # receives one packet then closes IN
    dis = net.add_component("Discard", discard)

    net.connect("Generate.OUT", "First.IN")
    net.connect("First.OUT", "Discard.IN")

    net.go()

    assert dis.values == ['000005']


def test_synced_receive1(net, discard):
    """
    components using `fn.synced` (Group) close all synced inports on the first
    exhausted inport.
    """
    net.add_component("Generate5", GenerateTestData, COUNT=5)
    net.add_component("Generate3", GenerateTestData, COUNT=3)
    # contains the call to synced:
    net.add_component("Merge", Group)
    dis = net.add_component("Discard", discard)

    net.connect("Generate5.OUT", "Merge.IN[0]")
    net.connect("Generate3.OUT", "Merge.IN[1]")
    net.initialize('initial', "Merge.IN[2]")
    net.connect("Merge.OUT", "Discard.IN")
    net.go()

    assert dis.values == [
        ('000005', '000003', 'initial'),
        ('000004', '000002', 'initial'),
        ('000003', '000001', 'initial'),
    ]


def test_synced_receive2(net, discard):
    """
    components using `fn.synced` (Group) close all synced inports on the first
    exhausted inport.
    """
    net.add_component("Generate5", GenerateTestData, COUNT=5)
    net.add_component("Generate3", GenerateTestData, COUNT=3)
    # repeat infinitely:
    net.add_component("Repeat", Repeat, IN='initial')  # FIXME: this fails when COUNT=1
    # contains the call to synced:
    net.add_component("Merge", Group)
    dis = net.add_component("Discard", discard)

    net.connect("Generate5.OUT", "Merge.IN[0]")
    net.connect("Generate3.OUT", "Merge.IN[1]")
    net.connect('Repeat.OUT', "Merge.IN[2]")
    net.connect("Merge.OUT", "Discard.IN")
    net.go()

    assert dis.values == [
        ('000005', '000003', 'initial'),
        ('000004', '000002', 'initial'),
        ('000003', '000001', 'initial')
    ]


def test_synced_receive3(net, discard):
    # contains the call to synced:
    net.add_component("Merge", Group)
    dis = net.add_component("Discard", discard)
    net.connect("Merge.OUT", "Discard.IN")
    net.go()

    assert dis.values == []


def test_merge_sort_drop(net, discard):
    net.add_component("_Generate", GenerateTestData, COUNT=4)
    net.add_component("_Generate2", GenerateTestData, COUNT=4)
    net.add_component("_Sort", Sort)
    dis = net.add_component("_Discard", discard)
    net.add_component("Passthru", Passthru)
    net.add_component("Passthru2", Passthru)
    net.connect("_Generate2.OUT", "Passthru2.IN")
    net.connect("_Generate.OUT", "Passthru.IN")
    net.connect("Passthru2.OUT", "Passthru.IN")
    net.connect("Passthru.OUT", "_Sort.IN")
    net.connect("_Sort.OUT", "_Discard.IN")
    net.go()
    assert dis.values == [
        '000001', '000001', '000002', '000002', '000003', '000003',
        '000004', '000004'
    ]


def test_inport_default():
    net = Network()
    net.add_component("Generate", GenerateTestData)
    dis = net.add_component("Discard", Discard)
    net.connect("Generate.OUT", "Discard.IN")
    net.go()
    assert dis.values == ['000001']


def test_fib(net):
    """
    Make fibonacci sequence from completely reusable parts.
    """
    net.add_component("Add", Add)
    # oscillates between its two inputs:
    net.add_component("Split", RoundRobinSplit)
    # kicks off the initial values:
    net.add_component("Zero", Inject, CONST=0)
    net.add_component("One", Inject, CONST=1)
    # doubles up the streams because they are consumed in pairs by Add:
    #   Split.OUT[0]: 0 1 1 3 3 8 8
    #   Split.OUT[1]: 1 1 2 2 5 5 13
    net.add_component("Repeat1", Repeat, COUNT=2)
    net.add_component("Repeat2", Repeat, COUNT=2)
    # set a max value to the sequence
    net.add_component("Cap", Cap, MAX=30)
    pthru = net.add_component("Passthru", Passthru)

    # need to use inject because you can't mix static value and connection
    net.connect("Zero.OUT", "Add.IN1")
    net.connect("Repeat1.OUT", "Add.IN1")
    net.connect("Split.OUT[0]", "Repeat1.IN")

    # need to use inject because you can't mix static value and connection
    net.connect("One.OUT", "Repeat2.IN")
    net.connect("Split.OUT[1]", "Repeat2.IN")

    net.connect("Repeat2.OUT", "Add.IN2")
    net.connect("Add.OUT", "Cap.IN")
    net.connect("Cap.OUT", "Passthru.IN")
    # complete the loop:
    net.connect("Passthru.OUT", "Split.IN")
    net.go()
    # FIXME: where's the 0?
    assert pthru.values == [1, 2, 3, 5, 8, 13, 21]


def test_readme_example(net, discard):
    net.add_component("LineToWords", LineToWords, IN="HeLLo GoodbYe WOrld")
    net.add_component("StartsWith", StartsWith, TEST='G')
    net.add_component("LowerCase", LowerCase)
    net.add_component("WordsToLine", WordsToLine)
    dis = net.add_component("Discard", discard)

    net.connect("LineToWords.OUT", "StartsWith.IN")
    net.connect("StartsWith.REJ", "LowerCase.IN")
    net.connect("LowerCase.OUT", "WordsToLine.IN")
    net.connect("WordsToLine.OUT", "Discard.IN")
    net.go()
    assert dis.values == ['hello world']


def test_first(net, discard):
    net.add_component("Generate", GenerateTestData, COUNT=4)
    net.add_component("First", First)
    dis = net.add_component("Discard", discard)

    net.connect("Generate.OUT", "First.IN")
    net.connect("First.OUT", "Discard.IN")
    net.go()
    assert dis.values == ['000004']

# def test_self_starting():
#     # creates a cycle
#     self.connect(self.component("Copy", Copy).port("OUT"),
#                  self.component("CopySSt", CopySSt).port("IN"))
#     self.connect(self.component("CopySSt").port("OUT"),
#                  self.component("Copy").port("IN"))

serialized_network_fixture = {
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
            'data': 5,
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
serialized_network_fixture['connections'] = sorted(
    serialized_network_fixture['connections'],
    key=str
)


def test_network_serialization():
    net = Network()

    counter = net.add_component('Counter1', Counter)
    counter.metadata.update({
        'x': 20.0,
        'y': 300.5
    })
    net.add_component('Pass', PassthruNet)
    net.add_component('Discard1', Discard)
    net.add_component('Generate', GenerateArray)
    net.add_component("Merge", Group)
    net.connect('Counter1.OUT', 'Pass.IN')
    net.connect('Pass.OUT', 'Discard1.IN')
    net.connect("Generate.OUT[0]", "Merge.IN[1]")
    net.connect("Generate.OUT[1]", "Merge.IN[2]")

    net.initialize(5, "Counter1.IN")

    assert len(net.get_components().keys()) == 5

    definition = net.to_dict()

    expected = serialized_network_fixture

    # Order of connections array shouldn't matter
    definition['connections'] = sorted(definition['connections'], key=str)

    assert definition == expected


def test_network_deserialization():
    definition = serialized_network_fixture

    net = Network.from_dict(definition, {
        'rill.components.basic/Counter': Counter,
        'rill.components.merge/Group': Group,
        'tests.components/Discard': Discard,
        'tests.subnets/PassthruNet': PassthruNet,
        'tests.components/GenerateArray': GenerateArray
    })

    assert len(net.get_components().keys()) == 5

    Counter1 = net.get_component('Counter1')
    Discard1 = net.get_component('Discard1')
    Pass = net.get_component('Pass')
    Generate = net.get_component('Generate')
    Merge = net.get_component('Merge')

    assert Counter1.ports.OUT._connection.inport.component is Pass
    assert Counter1.metadata == {
        'x': 20.0,
        'y': 300.5
    }
    assert Pass.ports.OUT._connection.inport.component is Discard1
    assert Counter1.ports.IN._connection._content is 5

    assert (
        Generate.ports.OUT.get_element(0)._connection.inport.component is Merge
    )

    assert (
        Generate.ports.OUT.get_element(1)._connection.inport.component is Merge
    )

    assert (
        Generate.ports.OUT.get_element(0)._connection.inport.index is 1
    )

    assert (
        Generate.ports.OUT.get_element(1)._connection.inport.index is 2
    )

    expected = net.to_dict()

    # Order of connections array shouldn't matter
    expected['connections'] = sorted(expected['connections'], key=str)

    assert definition == expected


def test_network_export():
    net = Network()
    passthru = net.add_component("Pass", SlowPass, DELAY=0.1)

    net.export('Pass.OUT', 'OUT')
    net.export('Pass.IN', 'IN')

    assert len(net.inports.keys()) == 1
    assert len(net.outports.keys()) == 1


def test_export_serialization():
    net = Network()

    net.add_component('Head', SlowPass, DELAY=0.01)
    net.add_component('Tail', SlowPass, DELAY=0.01)

    net.connect('Head.OUT', 'Tail.IN')

    net.export('Head.IN', 'IN')
    net.export('Tail.OUT', 'OUT')

    definition = net.to_dict()
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
                'data': 0.01,
                'tgt': {
                    'process': 'Head',
                    'port': 'DELAY'
                }
            },
            {
                'data': 0.01,
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
                'data': 0.01,
                'tgt': {
                    'process': 'Head',
                    'port': 'DELAY'
                }
            },
            {
                'data': 0.01,
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

    net = Network.from_dict(definition, {
        'rill.components.timing/SlowPass': SlowPass
    })

    Head = net.get_component('Head')
    Tail = net.get_component('Tail')

    assert Head.ports.OUT._connection.inport.component is Tail

    expected = net.to_dict()

    # Order of connections array shouldn't matter
    definition['connections'] = sorted(definition['connections'], key=str)
    expected['connections'] = sorted(expected['connections'], key=str)

    assert definition == expected


def test_network_apply():
    net = Network()
    net.add_component('Add1', Add)
    net.add_component('Add2', Add)

    net.connect('Add1.OUT', 'Add2.IN1')

    net.export('Add1.IN1', 'IN1')
    net.export('Add1.IN2', 'IN2')
    net.export('Add2.IN2', 'IN3')
    net.export('Add2.OUT', 'OUT')

    outputs = apply_network(net, {
        'IN1': 1,
        'IN2': 3,
        'IN3': 6
    })

    assert outputs['OUT'] == 10


def test_network_apply_with_outputs():
    net = Network()
    net.add_component('Add1', Add)
    net.add_component('Add2', Add)
    net.add_component('Kick', Kick)

    net.connect('Add1.OUT', 'Add2.IN1')

    net.export('Add1.IN1', 'IN1')
    net.export('Add1.IN2', 'IN2')
    net.export('Add2.IN2', 'IN3')
    net.export('Add2.OUT', 'OUT')
    net.export('Kick.OUT', 'Kick_OUT')

    outputs = apply_network(net, {
        'IN1': 1,
        'IN2': 3,
        'IN3': 6
    }, ['OUT'])

    assert outputs == {'OUT': 10}

