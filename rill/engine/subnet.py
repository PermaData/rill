from abc import ABCMeta
from collections import OrderedDict
from rill.utils import abstractclassmethod

from future.utils import with_metaclass
from typing import Type

from rill.engine.component import Component, inport, outport, logger
from rill.engine.portdef import InputPortDefinition, OutputPortDefinition
from rill.engine.network import Network, Graph
from rill.engine.status import StatusValues
from rill.engine.inputport import InitializationConnection
from rill.engine.exceptions import FlowError
from rill.engine.packet import Packet
from rill.utils import classproperty


def merge_portdefs(exported, inherited):
    """
    merge inherited ports (defined on parent classes or using @inport)
    with exported ports.
    """
    # Inherited ports take precedence, because they belong to the SubGraph.
    # For example, the description of a port defined on a SubGraph using
    # @inport can be customized to describe the port's purposed within the
    # SubGraph as a whole, whereas the description of exported port concerns
    # only the behavior of its component.
    # FIXME: check compatibility of things like array, required...
    exported.update(inherited)
    return exported


# using a component to proxy each exported port has a few side effects:
# - additional logging output for the extra components
# - additional threads which aren't necessary
# - components in the graph which need to be ignored
# - packets are temporarily owned by the SubGraph
# - dynamically adding/removing inports/outports requires extra book-keeping
#   to ensure the ExportComponents are properly created and connected
# but it plays well with the rest of the system in a way that is
# difficult to achieve otherwise. For example, simply exposing the
# internal ports doesn't work because the port's component attr is not
# the SubGraph, and so the SubGraph is never activated by OutputPort.send()

@inport("PROXIED")
class ExportComponent(Component):
    """
    Component that exports to a parent network
    """
    hidden = True

    def internal_port(self):
        """
        Get the port on the SubGraph that this Component proxies.
        """
        return self.ports.PROXIED.receive_once()


@outport("OUT")
class SubIn(ExportComponent):
    """
    Acts as a proxy between an input port on the SubGraph and an input port on
    one of its internal components.
    """
    def execute(self):
        inport = self.internal_port()
        if inport is None or self.ports.OUT.is_closed():
            return

        self.logger.debug("Accessing input port: {}".format(inport))

        old_receiver = inport.component
        if inport.is_static():
            iic = InitializationConnection(inport._connection._content, inport)

            iic.open()
            p = iic.receive()
            p.set_owner(self)
            self.ports.OUT.send(p)
            iic.close()
        else:
            inport.component = self

            for p in inport:
                p.set_owner(self)
                self.ports.OUT.send(p)

        # inport.close()
        self.logger.debug("Releasing input port: {}".format(inport))

        inport.component = old_receiver


@outport("OUT")
class SubInSS(ExportComponent):
    def execute(self):
        inport = self.internal_port()
        if inport is None or self.ports.OUT.is_closed():
            return

        self.logger.debug("Accessing input port: {}".format(inport))

        old_receiver = inport.component
        if inport.is_static():
            raise FlowError("SubinSS cannot support IIP - use Subin")

        inport.component = self
        level = 0
        for p in inport:
            p.set_owner(self)
            if p.get_type() == Packet.Type.OPEN:
                if level > 0:
                    self.ports.OUT.send(p)
                else:
                    self.drop(p)
                    self.logger.debug("open bracket detected")
                level += 1
            elif p.get_type() == Packet.Type.CLOSE:
                if level > 1:
                    # pass on nested brackets
                    self.ports.OUT.send(p)
                    level -= 1
                else:
                    self.drop(p)
                    self.logger.debug("close bracket detected")
                    break
            else:
                self.ports.OUT.send(p)

        self.logger.debug("Releasing input port: {}".format(inport))
        # inport.set_receiver(old_receiver)
        inport.component = old_receiver


@inport("IN")
class SubOut(ExportComponent):
    """
    Acts as a proxy between an output port on the SubGraph and an output port on
    one of its internal components.
    """
    def execute(self):
        outport = self.internal_port()
        if outport is None:
            return

        self.logger.debug("Accessing output port: {}".format(outport))
        outport.component = self

        for p in self.ports.IN:
            outport.send(p)

        self.logger.debug("Releasing output port: {}".format(outport))


@inport("IN")
class SubOutSS(ExportComponent):
    """
    Look after output from subnet

    Differs from SubOut in that it adds an open bracket at the beginning, and a
    close bracket at the end
    """

    def execute(self):
        outport = self.internal_port()
        if outport is None:
            return

        self.logger.debug("Accessing output port: {}".format(outport))
        outport.component = self

        p = self.create(Packet.Type.OPEN)
        outport.send(p)

        for p in self.ports.IN:
            outport.send(p)

        p = self.create(Packet.Type.CLOSE)
        outport.send(p)

        self.logger.debug("Releasing output port: {}".format(outport))


class SubGraph(with_metaclass(ABCMeta, Component)):
    """
    A component that defines and executes a sub-graph of components
    """
    # type: rill.engine.network.Graph
    subgraph = None

    @abstractclassmethod
    def define(cls, graph):
        """
        Define the network that will be executed by this SubGraph.

        Parameters
        ----------
        graph : ``rill.engine.Graph``
            an empty network to setup
        """
        raise NotImplementedError

    def _init(self):
        if self.subgraph is None:
            self._init_graph()

        # we set these after define because we're paranoid:
        self.subgraph.name = self._name
        super(SubGraph, self)._init()

    @classproperty
    def inport_definitions(cls):
        if cls.subgraph is None:
            cls._init_graph()
        exported = OrderedDict(
            [(name, InputPortDefinition.from_port(port, name=name))
             for name, port in cls.subgraph.inports.items()])

        inherited = super(SubGraph, cls).inport_definitions
        return merge_portdefs(exported, inherited)

    @classproperty
    def outport_definitions(cls):
        if cls.subgraph is None:
            cls._init_graph()
        exported = OrderedDict(
            [(name, OutputPortDefinition.from_port(port, name=name))
             for name, port in cls.subgraph.outports.items()])
        inherited = super(SubGraph, cls).outport_definitions
        return merge_portdefs(exported, inherited)

    @classmethod
    def _init_graph(cls):
        """
        Initialize the graph.

        This is a classmethod so that exported port definitions can be
        inspected without instantiating the SubGraph, which is a requirement
        for all Components.
        """
        cls.subgraph = Graph()
        cls.define(cls.subgraph)
        assert cls.subgraph is not None

    def _build_network(self):
        # the graph is a class attribute, so we have to make a deep copy to
        # avoid side-effects
        graph = self.subgraph.copy()
        for (name, internal_port) in graph.inports.items():
            subcomp = graph.add_component('_' + name, SubIn,
                                          PROXIED=self.ports[name])
            graph.connect(subcomp.ports.OUT, internal_port)

        for (name, internal_port) in graph.outports.items():
            subcomp = graph.add_component('_' + name, SubOut,
                                          PROXIED=self.ports[name])
            graph.connect(internal_port, subcomp.ports.IN)

        # don't do deadlock testing in sub graphs - you need to consider
        # the whole graph!
        network = Network(graph, deadlock_test_interval=None)
        # set the network parent. this allows runners within the network to
        # walk up the network parents to the root
        network.parent_network = self._runner.parent_network
        return network

    def execute(self):
        # self.get_components().clear()
        # FIXME: are these supposed to be the same as null ports?
        # sub_end_port = None  # self.outports.get("*SUBEND")
        # sub_in_port = None  # self.inports.get("*CONTROL")
        # if sub_in_port is not None:
        #     p = sub_in_port.receive()
        #     if p is not None:
        #         self.drop(p)

        # use fields instead!
        # tracing = parent.tracing
        # trace_file_list = parent.trace_file_list

        # FIXME: warn if any external ports have not been connected

        # if not all(c._check_required_ports() for c in
        #            self.get_components().values()):
        #     raise FlowError(
        #         "One or more mandatory connections have been left unconnected: " + self.get_name())

        # FIXME: handle resume!

        network = self._build_network()
        network.initiate()
        network.wait_for_all()

        # FIXME: not sure what purpose this serves
        for inp in self.inports:
            if inp.is_static() and not inp.is_null():
                inp.close()

        # Iterator allout = (outports.values()).iterator()
        # while (allout.has_next()):
        # OutputPort op = (OutputPort) allout.next() op.close()

        # status = StatusValues.TERMINATED # will not be set if never activated
        # parent.indicate_terminated(self)
        # if sub_end_port is not None:
        #     sub_end_port.send(Packet(None, self))

    def get_children(self):
        return list(self.subgraph.get_components().values())

    # def signal_error(self, e):
    #     if self.status != StatusValues.ERROR:
    #         self.parent.signal_error(e)
    #         self.terminate(StatusValues.ERROR)


# @inport("NAME")
# @inport("IN")
# @outport("OUT")
# class SubOI(Component):
#     """Look after (synchronous) output/input from/to subnet.
#
#     This component sends a single packet out to the (external) output port,
#     and then immediately does a receive from the corresponding (external) input
#     port. This process repeats until a None is received on the input port.
#     """
#
#     def execute(self):
#         np = self.nameport.receive()
#         if np is None:
#             return
#         self.nameport.close()
#         pname = np.get_contents()
#         self.drop(np)
#
#         i = pname.index_of(":")
#         oname = pname.substring(0, i)
#         iname = pname.substring(i + 1)
#         extoutport = self.parent.get_outports().get(oname)
#         self.parent.trace_funcs(
#             self.get_name() + ": Accessing output port: " + extoutport.get_name())
#
#         old_sender = extoutport.get_sender()
#         extoutport.set_sender(self)
#
#         extinport = self.parent.get_inports().get(iname)
#         self.parent.trace_funcs(
#             self.get_name() + ": Accessing input port: " + extinport.get_name())
#         old_receiver = extinport.get_receiver()
#         extinport.set_receiver(self)
#
#         for p in self.inport.receive():
#             self.extoutport.send(p)
#
#             p = extinport.receive()
#             p.set_owner(self)
#             self.outport.send(p)
#
#         self.parent.trace_funcs(
#             self.get_name() + ": Releasing input port: " + extinport.get_name())
#         extinport.set_receiver(old_receiver)
#
#         self.parent.trace_funcs(
#             self.get_name() + ": Releasing output port: " + extoutport.get_name())
#         extoutport.set_sender(old_sender)
#
#     def open_ports(self):
#         self.nameport = self.open_input("NAME")
#         self.inport = self.open_input("IN")
#         self.outport = self.open_output("OUT")


def make_subgraph(name, graph):
    """
    Make a ``SubGraph`` component class from a ``Graph`` instance.

    Parameters
    ----------
    name : str
    graph : ``rill.engine.network.Graph``

    Returns
    -------
    Type[``SubGraph``]
    """
    def define(cls, _):
        cls.subgraph = graph

    attrs = {
        'name': name,
        'define': classmethod(define)
    }
    return type(name, (SubGraph,), attrs)
