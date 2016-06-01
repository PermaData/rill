from abc import ABCMeta, abstractmethod

from future.utils import with_metaclass
from past.builtins import basestring

from rill.engine.component import Component, inport, outport, logger
from rill.engine.network import Network
from rill.engine.status import StatusValues
from rill.engine.inputport import InputPort, BaseInputCollection, InitializationConnection
from rill.engine.outputport import OutputPort, BaseOutputCollection
from rill.engine.exceptions import FlowError
from rill.engine.packet import Packet


class ExportComponent(Component):
    """
    Component that exports to a parent network
    """
    is_export = True
    subnet = None # Parent subnet component


@inport("NAME", type=str)
@inport("INDEX", type=int)
@outport("OUT")
class SubIn(ExportComponent):
    """
    Acts as a proxy between an input port on the SubNet and an input port on
    one of its internal components.
    """
    def execute(self):
        pname = self.ports.NAME.receive_once()
        if pname is None:
            return

        if self.ports.OUT.is_closed():
            return

        inport = self.subnet.ports[pname]
        index = self.ports.INDEX.receive_once()
        if index is not None:
            inport = inport[index]

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


@inport("NAME", type=str)
@inport("INDEX", type=int)
@outport("OUT")
class SubInSS(ExportComponent):
    def execute(self):
        pname = self.ports.NAME.receive_once()

        if self.ports.OUT.is_closed():
            return

        inport = self.subnet.ports[pname]
        index = self.ports.INDEX.receive_once()
        if index is not None:
            inport = inport[index]

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
@inport("NAME", type=str)
@inport("INDEX", type=int)
class SubOut(ExportComponent):
    """
    Acts as a proxy between an output port on the SubNet and an output port on
    one of its internal components.
    """
    def execute(self):
        pname = self.ports.NAME.receive_once()
        if pname is None:
            return

        outport = self.subnet.ports[pname]
        index = self.ports.INDEX.receive_once()
        if index is not None:
            outport = outport[index]

        self.logger.debug("Accessing output port: {}".format(outport))
        outport.component = self

        for p in self.ports.IN:
            outport.send(p)

        self.logger.debug("Releasing output port: {}".format(outport))


@inport("IN")
@inport("NAME", type=str)
@inport("INDEX", type=int)
class SubOutSS(ExportComponent):
    """
    Look after output from subnet

    Differs from SubOut in that it adds an open bracket at the beginning, and a
    close bracket at the end
    """

    def execute(self):
        pname = self.ports.NAME.receive_once()
        if pname is None:
            return

        outport = self.subnet.ports[pname]
        index = self.ports.INDEX.receive_once()
        if index is not None:
            outport = outport[index]

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
    sub_network = None

    def __init__(self, name, parent, **kwargs):
        Component.__init__(self, name, parent)
        # don't do deadlock testing in subnets - you need to consider
        # the whole net!
        kwargs['deadlock_test_interval'] = None

        if not self.sub_network:
            self.sub_network = Network(**kwargs)

    @abstractmethod
    def define(self, net):
        raise NotImplementedError

    def _init(self):
        super(SubNet, self)._init()
        self.define(self.sub_network)

        for (name, internal_port) in self.sub_network.inports.items():
            self.export(internal_port, name, True)

        for (name, internal_port) in self.sub_network.outports.items():
            self.export(internal_port, name, True)

    def export(self, internal_port, external_port, create=False):
        """
        Expose a port.

        Parameters
        ----------
        external_port : ``rill.engine.outputport.OutputPort`` or
                        ``rill.engine.inputport.InputPort`` or str
            external SubNet port
        internal_port : ``rill.engine.outputport.OutputPort`` or
                        ``rill.engine.inputport.InputPort`` or str
            internal port
        """
        # using a component to proxy each exported port has a few side effects:
        # - additional logging output for the extra components
        # - additional threads which aren't necessary
        # - packets are temporarily owned by the SubNet
        # but it plays well with the rest of the system in a way that is
        # difficult to achieve otherwise. For example, simply exposing the
        # internal ports doesn't work because the port's component attr is not
        # the SubNet, and so the SubNet is never activated by OutputPort.send()

        # FIXME: support exporting an array port to array port. currently,
        # if you try to do that you'll end up exporting the first element
        if create:
            # create a new port on the SubNet based on the exported port
            def_cls = inport if internal_port.kind == 'in' else outport
            external_port_name = external_port
            assert isinstance(external_port_name, basestring)
            portdef = def_cls.from_port(internal_port, external_port_name)
            external_port = portdef.create_port(self)
            self.ports[external_port_name] = external_port
        else:
            external_port = self.port(external_port, kind=internal_port.kind)
        if external_port.is_array():
            external_port = external_port.get_element(create=True)

        if isinstance(external_port, InputPort):
            subcomp = self.sub_network.add_component('_' + external_port.name, SubIn,
                                         NAME=external_port._name)
            if external_port.index is not None:
                self.initialize(external_port.index, subcomp.ports.INDEX)
            self.sub_network.connect(subcomp.ports.OUT, internal_port)
        elif isinstance(external_port, OutputPort):
            subcomp = self.sub_network.add_component('_' + external_port.name, SubOut,
                                         NAME=external_port._name)
            if external_port.index is not None:
                self.initialize(external_port.index, subcomp.ports.INDEX)
            self.sub_network.connect(internal_port, subcomp.ports.IN)

        subcomp.subnet = self


    def execute(self):
        # self.get_components().clear()
        # FIXME:
        sub_end_port = None  # self.outports.get("*SUBEND")
        sub_in_port = None  # self.inports.get("*CONTROL")
        if sub_in_port is not None:
            p = sub_in_port.receive()
            if p is not None:
                self.drop(p)

        # use fields instead!
        # tracing = parent.tracing
        # trace_file_list = parent.trace_file_list

        # FIXME: run define() as part of init. if we allow dynamic port
        # creation in define(), which would be great, then it needs to be run
        # during the net creation phase to know what the ports are. or provide
        # variable to control when it gets run...
        # self.define()
        # FIXME: warn if any external ports have not been connected

        # if not all(c._check_required_ports() for c in
        #            self.get_components().values()):
        #     raise FlowError(
        #         "One or more mandatory connections have been left unconnected: " + self.get_name())

        # FIXME: handle resume!
        self.sub_network.initiate()
        # activate_all()
        self.sub_network.wait_for_all()
        for ip in self.inports:
            if ip.is_static():
                ip.close()

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

