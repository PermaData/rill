from abc import ABCMeta
from rill.utils import abstractclassmethod

from future.utils import with_metaclass
from typing import Type

from rill.engine.component import Component, inport, outport, logger
from rill.engine.portdef import InputPortDefinition, OutputPortDefinition
from rill.engine.network import Network
from rill.engine.status import StatusValues
from rill.engine.inputport import InitializationConnection
from rill.engine.exceptions import FlowError
from rill.engine.packet import Packet


# using a component to proxy each exported port has a few side effects:
# - additional logging output for the extra components
# - additional threads which aren't necessary
# - components in the graph which need to be ignored
# - packets are temporarily owned by the SubNet
# - dynamically adding/removing inports/outports requires extra book-keeping
#   to ensure the ExportComponents are properly created and connected
# but it plays well with the rest of the system in a way that is
# difficult to achieve otherwise. For example, simply exposing the
# internal ports doesn't work because the port's component attr is not
# the SubNet, and so the SubNet is never activated by OutputPort.send()

@inport("NAME", type=str)
@inport("INDEX", type=int)
class ExportComponent(Component):
    """
    Component that exports to a parent network
    """
    hidden = True

    @property
    def subnet(self):
        """

        Returns
        -------
        ``rill.engine.subnet.SubNet``
        """
        return self.parent._component

    def internal_port(self):
        pname = self.ports.NAME.receive_once()
        if pname is None:
            return

        port = self.subnet.ports[pname]
        index = self.ports.INDEX.receive_once()
        if index is not None:
            port = port[index]
        return port


@outport("OUT")
class SubIn(ExportComponent):
    """
    Acts as a proxy between an input port on the SubNet and an input port on
    one of its internal components.
    """
    def execute(self):
        inport = self.internal_port()
        if inport is None or self.ports.OUT.is_closed():
            return

        self.logger.debug("Accessing input port: {}".format(inport))

        if inport.is_static():
            old_receiver = inport.component
            iic = InitializationConnection(inport._connection._content, inport)

            iic.open()
            p = iic.receive()
            p.set_owner(self)
            self.ports.OUT.send(p)
            iic.close()
        else:
            old_receiver = inport.component
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
    Acts as a proxy between an output port on the SubNet and an output port on
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


class SubNet(with_metaclass(ABCMeta, Component)):
    """
    A component that defines and executes a sub-network of components
    """
    sub_network = None

    @abstractclassmethod
    def define(cls, net):
        """
        Define the network that will be executed by this SubNet.

        Parameters
        ----------
        net : ``rill.engine.Network``
            an empty network to setup
        """
        raise NotImplementedError

    def _init(self):
        if self.sub_network is None:
            self._init_network()

        # we set these after define because we're paranoid:
        # don't do deadlock testing in subnets - you need to consider
        # the whole net!
        self.sub_network.deadlock_test_interval = None
        self.sub_network.parent = self.parent
        self.sub_network.name = self._name

        # FIXME: a hack to provide access to this class in ExportComponent
        self.sub_network._component = self

        super(SubNet, self)._init()

    @classmethod
    def port_definitions(cls):
        if cls.sub_network is None:
            cls._init_network()
        super(SubNet, cls).port_definitions()

    @classmethod
    def _init_network(cls):
        """
        Initialize the network.

        This is a classmethod so that exported port definitions can be
        inspected without instantiating the SubNet, which is a requirement
        for all Components.
        """
        cls.sub_network = Network()
        cls.define(cls.sub_network)
        assert cls.sub_network is not None

        # FIXME: ideally exported ports are recalculated on-the-fly within
        # port_definitions so that we don't have to do extra work in
        # add_inport / add_outport to keep the exports on the SubNet Component
        # in sync with its Network (i.e cls.sub_network).  we can't do that as
        # long as we need to connect ExportComponents within the network,
        # because that must be done exactly once for all SubNet instances.
        for (name, internal_port) in cls.sub_network.inports.items():
            cls.add_inport(name, internal_port)

        for (name, internal_port) in cls.sub_network.outports.items():
            cls.add_outport(name, internal_port)

    @classmethod
    def add_inport(cls, name, internal_port):
        """
        Expose an InputPort from within the network as a port on this SubNet
        Component.

        Parameters
        ----------
        name : str
            name of external SubNet port to create
        internal_port : ``rill.engine.inputport.InputPort``
            internal port
        """
        portdef = InputPortDefinition.from_port(internal_port)
        portdef.name = name
        cls._inport_definitions.append(portdef)
        subcomp = cls.sub_network.add_component('_' + name, SubIn,
                                                NAME=name)
        cls.sub_network.connect(subcomp.ports.OUT, internal_port)

        # in case this has been called outside of _init_network:
        if name not in cls.sub_network.inports:
            cls.sub_network.inports[name] = internal_port

    @classmethod
    def remove_inport(cls, name):
        cls._inport_definitions = [p for p in cls._inport_definitions
                                   if p.name != name]
        cls.sub_network.remove_component('_' + name)

    @classmethod
    def add_outport(cls, name, internal_port):
        """
        Expose an OutputPort from within the network as a port on this SubNet
        Component.

        Parameters
        ----------
        name : str
            name of external SubNet port to create
        internal_port : ``rill.engine.outputport.OutputPort``
            internal port
        """
        portdef = OutputPortDefinition.from_port(internal_port)
        portdef.name = name
        cls._outport_definitions.append(portdef)
        subcomp = cls.sub_network.add_component('_' + name, SubOut,
                                                NAME=name)
        cls.sub_network.connect(internal_port, subcomp.ports.IN)

        # in case this has been called outside of _init_network:
        if name not in cls.sub_network.outports:
            cls.sub_network.outports[name] = internal_port

    @classmethod
    def remove_outport(cls, name):
        cls._outport_definitions = [p for p in cls._outport_definitions
                                    if p.name != name]
        cls.sub_network.remove_component('_' + name)

    def execute(self):
        # self.get_components().clear()
        # FIXME: are these supposed to be the same as null ports?
        sub_end_port = None  # self.outports.get("*SUBEND")
        sub_in_port = None  # self.inports.get("*CONTROL")
        if sub_in_port is not None:
            p = sub_in_port.receive()
            if p is not None:
                self.drop(p)

        # use fields instead!
        # tracing = parent.tracing
        # trace_file_list = parent.trace_file_list

        # FIXME: warn if any external ports have not been connected

        # if not all(c._check_required_ports() for c in
        #            self.get_components().values()):
        #     raise FlowError(
        #         "One or more mandatory connections have been left unconnected: " + self.get_name())

        # FIXME: handle resume!
        self.sub_network.initiate()
        # activate_all()
        self.sub_network.wait_for_all()
        for inp in self.inports:
            if inp.is_static() and not inp.is_null():
                inp.close()

        # Iterator allout = (outports.values()).iterator()
        # while (allout.has_next()):
        # OutputPort op = (OutputPort) allout.next() op.close()

        # status = StatusValues.TERMINATED # will not be set if never activated
        # parent.indicate_terminated(self)
        if sub_end_port is not None:
            sub_end_port.send(Packet(None, self))

    def get_children(self):
        return list(self.sub_network.get_components().values())

    def signal_error(self, e):
        if self.status != StatusValues.ERROR:
            self.parent.signal_error(e)
            self.terminate(StatusValues.ERROR)


@inport("NAME")
@inport("IN")
@outport("OUT")
class SubOI(Component):
    """Look after (synchronous) output/input from/to subnet.

    This component sends a single packet out to the (external) output port,
    and then immediately does a receive from the corresponding (external) input
    port. This process repeats until a None is received on the input port.
    """

    def execute(self):
        np = self.nameport.receive()
        if np is None:
            return
        self.nameport.close()
        pname = np.get_contents()
        self.drop(np)

        i = pname.index_of(":")
        oname = pname.substring(0, i)
        iname = pname.substring(i + 1)
        extoutport = self.parent.get_outports().get(oname)
        self.parent.trace_funcs(
            self.get_name() + ": Accessing output port: " + extoutport.get_name())

        old_sender = extoutport.get_sender()
        extoutport.set_sender(self)

        extinport = self.parent.get_inports().get(iname)
        self.parent.trace_funcs(
            self.get_name() + ": Accessing input port: " + extinport.get_name())
        old_receiver = extinport.get_receiver()
        extinport.set_receiver(self)

        for p in self.inport.receive():
            self.extoutport.send(p)

            p = extinport.receive()
            p.set_owner(self)
            self.outport.send(p)

        self.parent.trace_funcs(
            self.get_name() + ": Releasing input port: " + extinport.get_name())
        extinport.set_receiver(old_receiver)

        self.parent.trace_funcs(
            self.get_name() + ": Releasing output port: " + extoutport.get_name())
        extoutport.set_sender(old_sender)

    def open_ports(self):
        self.nameport = self.open_input("NAME")
        self.inport = self.open_input("IN")
        self.outport = self.open_output("OUT")


def make_subnet(name, net):
    """
    Make a ``SubNet`` class from a ``Network`` instance.

    Parameters
    ----------
    name : str
    net : ``rill.engine.network.Network``

    Returns
    -------
    Type[``SubNet``]
    """
    def define(cls, _):
        cls.sub_network = net

    attrs = {
        'name': name,
        'define': classmethod(define)
    }
    return type(name, (SubNet,), attrs)
