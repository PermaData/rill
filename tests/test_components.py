import pytest

from rill.engine.exceptions import FlowError
from rill.engine.network import Network
from rill.engine.outputport import OutputPort, OutputArray
from rill.engine.inputport import InputPort, InputArray

from tests.components import *
from tests.subnets import PassthruNet

from rill.components.basic import Counter, Sort, Inject, Repeat, Cap
from rill.components.filters import First
from rill.components.merge import Group
from rill.components.split import RoundRobinSplit, Replicate
from rill.components.math import Add
from rill.components.files import ReadLines, WriteLines, Write
from rill.components.text import Prefix, LineToWords, LowerCase, StartsWith, WordsToLine

from rill.engine.component import Component
from rill.engine.decorators import inport, outport, component


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
    return [x.name for x in ports.ports()]


@component(pass_context=True)
@inport("IN")
@outport("OUT")
def Passthru(self, IN, OUT):
    """Pass a stream of packets to an output stream"""
    self.values = []
    for p in IN.iter_packets():
        self.values.append(p.get_contents())
        OUT.send(p)


@inport("IN", description="Stream of packets to be discarded")
class Discard(Component):
    def execute(self):
        p = self.inports.IN.receive()
        self.packets.append(p)
        self.values.append(p.get_contents())
        self.drop(p)

    def init(self):
        super(Discard, self).init()
        self.values = []
        self.packets = []


@inport("IN", description="Stream of packets to be discarded")
class DiscardLooper(Component):
    def execute(self):
        for p in self.inports.IN:
            self.packets.append(p)
            self.values.append(p.get_contents())
            self.drop(p)

    def init(self):
        super(DiscardLooper, self).init()
        self.values = []
        self.packets = []


@pytest.fixture(params=[dict(default_capacity=1),
                        dict(default_capacity=2)],
                ids=['net.capacity=1', 'net.capacity=2'])
def net(request):
    return Network(**request.param)


@pytest.fixture(params=['looper', 'nonlooper'])
def discard(request):
    return DiscardLooper if request.param == 'looper' else Discard


def test_aggregation(net, discard):
    net.add_component("Generate", GenerateTestData)
    dis = net.add_component("Discard", discard)
    net.add_component("Counter", Counter)

    net.connect("Generate.OUT", "Counter.IN")
    net.connect("Counter.COUNT", "Discard.IN")
    net.initialize(5, "Generate.COUNT")
    net.go()
    assert dis.values == [5]


def test_static_type_validation():
    net = Network()

    with pytest.raises(FlowError):
        net.add_component("Repeat", Repeat, COUNT='foo')


@pytest.mark.xfail(reason='order is ACB instead of ABC')
def test_multiple_inputs(net):
    net.add_component("GenerateA", GenerateTestData, COUNT=5)
    net.add_component("PrefixA", Prefix, PRE='A')
    net.add_component("GenerateB", GenerateTestData, COUNT=5)
    net.add_component("PrefixB", Prefix, PRE='B')
    net.add_component("GenerateC", GenerateTestData, COUNT=5)
    net.add_component("PrefixC", Prefix, PRE='C')
    dis = net.add_component("Discard", Discard)

    net.connect("GenerateA.OUT", "PrefixA.IN")
    net.connect("GenerateB.OUT", "PrefixB.IN")
    net.connect("GenerateC.OUT", "PrefixC.IN")
    net.connect("PrefixA.OUT", "Discard.IN")
    net.connect("PrefixB.OUT", "Discard.IN")
    net.connect("PrefixC.OUT", "Discard.IN")
    net.go()
    assert dis.values == [
        'A000005', 'A000004', 'A000003', 'A000002', 'A000001',
        'B000005', 'B000004', 'B000003', 'B000002', 'B000001',
        'C000005', 'C000004', 'C000003', 'C000002', 'C000001',
    ]


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
    net.go()
    assert dis.values == ['000005', '000004', '000003', '000002', '000001']


def test_basic_connections():
    net = Network()
    count = net.add_component("Count", Counter)
    dis = net.add_component("Discard1", Discard)
    net.add_component("Discard2", Discard)

    # ports are stored in the order they are declared
    assert names(count.outports) == ['OUT', 'COUNT', 'NULL']
    assert list(count.outports._ports.keys()) == ['OUT', 'COUNT', 'NULL']

    # nothing is connected yet
    assert count.outports['OUT'].is_connected() is False
    assert dis.inports['IN'].is_connected() is False

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

    assert type(count.outports['OUT']) is OutputPort
    assert type(dis.inports['IN']) is InputPort
    assert count.outports['OUT'].is_connected() is True
    assert dis.inports['IN'].is_connected() is True


def test_fixed_array_connections():
    net = Network()
    gen = net.add_component("Generate", GenerateFixedSizeArray)
    dis1 = net.add_component("Discard1", Discard)
    net.add_component("Discard2", Discard)

    assert gen.outports['OUT'].get_full_name() == 'Generate.OUT'

    # fixed array ports create their ports immediately
    assert gen.outports.OUT.fixed_size == 2
    assert names(gen.outports) == ['OUT[0]', 'OUT[1]', 'NULL']
    assert list(gen.outports._ports.keys()) == ['OUT', 'NULL']
    assert type(gen.outports['OUT']) is OutputArray
    assert type(dis1.inports['IN']) is InputPort

    # nothing is connected yet
    assert gen.outports['OUT'].is_connected() is False
    assert dis1.inports['IN'].is_connected() is False

    # make a connection
    net.connect("Generate.OUT[1]", "Discard1.IN")
    assert names(gen.outports) == ['OUT[0]', 'OUT[1]', 'NULL']
    assert gen.outports['OUT'][1].is_connected() is True

    # uses first unconnected index (index 0)
    net.connect("Generate.OUT", "Discard2.IN")
    assert gen.outports['OUT'][0].is_connected() is True

    with pytest.raises(FlowError):
        # outports can only have one connection
        net.connect("Generate.OUT[1]", "Discard2.IN")

    with pytest.raises(FlowError):
        # cannot connect outside the fixed range
        net.connect("Generate.OUT[2]", "Discard2.IN")

    assert type(gen.outports['OUT']) is OutputArray
    assert type(gen.outports['OUT'][0]) is OutputPort
    assert type(dis1.inports['IN']) is InputPort
    assert gen.outports['OUT'].is_connected() is False
    assert dis1.inports['IN'].is_connected() is True


def test_array_connections():
    net = Network()
    gen = net.add_component("Generate", GenerateArray)
    dis1 = net.add_component("Discard1", Discard)
    net.add_component("Discard2", Discard)

    assert gen.outports['OUT'].get_full_name() == 'Generate.OUT'

    # non-fixed array ports delay element creation
    assert gen.outports.OUT.fixed_size is None
    assert names(gen.outports) == ['NULL']
    assert list(gen.outports._ports.keys()) == ['OUT', 'NULL']
    assert type(gen.outports['OUT']) is OutputArray
    assert type(dis1.inports['IN']) is InputPort

    # nothing is connected yet
    assert gen.outports['OUT'].is_connected() is False
    assert dis1.inports['IN'].is_connected() is False

    # make a connection
    net.connect("Generate.OUT[1]", "Discard1.IN")
    assert names(gen.outports) == ['OUT[1]', 'NULL']
    assert gen.outports['OUT'][1].is_connected() is True

    # uses first unused index (index 0)
    net.connect("Generate.OUT", "Discard2.IN")
    assert names(gen.outports) == ['OUT[0]', 'OUT[1]', 'NULL']
    assert gen.outports['OUT'][0].is_connected() is True

    assert type(gen.outports['OUT']) is OutputArray
    assert type(gen.outports['OUT'][0]) is OutputPort
    assert type(dis1.inports['IN']) is InputPort
    assert gen.outports['OUT'].is_connected() is False
    assert dis1.inports['IN'].is_connected() is True


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
    assert gen.outports['OUT'].optional is False
    net.add_component("Discard1", Discard)
    net.connect("Generate.OUT[0]", "Discard1.IN")
    assert names(gen.outports) == ['OUT[0]', 'OUT[1]', 'NULL']

    gen.init()
    with pytest.raises(FlowError):
        gen.outports.OUT.open()


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
    assert type(gen.inports.COUNT) is InputPort
    assert gen.inports.COUNT.is_static()
    assert gen.inports.COUNT._connection._content == 10

    with pytest.raises(FlowError):
        net.add_component("generate", GenerateTestData)

    def foo(): pass

    with pytest.raises(TypeError):
        net.add_component("not_component", foo)


def test_optional_fixed_size_array(net, discard):
    """if an array port specifies fixed_size, some elements may remain
    unconnected if the array port is optional (not required)"""
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
    assert gen.outports['OUT'].optional is False
    gen.init()
    with pytest.raises(FlowError):
        gen.outports.OUT.open()


def test_null_ports(net, tmpdir, discard):
    """null ports ensure proper ordering of components"""
    tempfile = str(tmpdir.join('data.txt'))
    net.add_component("Generate", GenerateTestData, COUNT=5)
    net.add_component("Write", WriteLines, FILEPATH=tempfile)
    net.add_component("Read", ReadLines, FILEPATH=tempfile)
    dis = net.add_component("Discard", discard)

    net.connect("Generate.OUT", "Write.IN")
    net.connect("Write.NULL", "Read.NULL")
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
    net.connect("Write.NULL", "Read.NULL")
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


@pytest.mark.xfail(
    reason='the error should not prevent the network from completing...')
def test_inport_closed_error(net, discard):
    net.add_component("Generate", GenerateTestDataDumb, COUNT=5)
    net.add_component("First", First)
    dis = net.add_component("Discard", discard)

    net.connect("Generate.OUT", "First.IN")
    net.connect("First.OUT", "Discard.IN")

    with pytest.raises(FlowError):
        net.go()

    assert dis.values == ['000005']


def test_subnet_with_substreams(net, discard):
    # tracing = True
    net.add_component("Generate", GenSS, COUNT=15)
    dis = net.add_component("Discard", discard)
    net.add_component("Subnet", PassthruNet)

    net.connect("Generate.OUT", "Subnet.IN")
    net.connect("Subnet.OUT", "Discard.IN")

    # FIXME: need a separate test for the NULL port behavior
    # net.connect("Subnet.*SUBEND", "WTC.IN")
    net.go()

    assert dis.values == [
        '', '000015', '000014', '000013', '000012', '000011', '',
        '', '000010', '000009', '000008', '000007', '000006', '',
        '', '000005', '000004', '000003', '000002', '000001', ''
    ]


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
