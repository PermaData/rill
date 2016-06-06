import collections
import logging
import re
from abc import ABCMeta, abstractmethod

from future.utils import with_metaclass

from rill.engine.port import PortCollection, flatten_arrays, is_null_port
from rill.engine.packet import Packet, Chain
from rill.engine.exceptions import FlowError, ComponentException
from rill.engine.utils import LogFormatter
from rill.decorators import inport, outport


PORT_NAME_REG = r"^([a-zA-Z][_a-zA-Z0-9]*)(?:\[(\d+)\])?$"

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)
logger = LogFormatter(_logger, {})


@inport('IN_NULL')
@outport('OUT_NULL')
class Component(with_metaclass(ABCMeta, object)):
    # list[rill.engine.portdef.InputPortDefinition]:
    inport_definitions = []
    # list[rill.engine.portdef.OutputPortDefinition]:
    outport_definitions = []
    _self_starting = False
    _must_run = False
    hidden = False

    # same as module-level logger, but provided here for convenience
    logger = logger

    def __init__(self, name, parent):
        assert name is not None
        self._name = name

        # cached values
        self._full_name = None
        self._parents = None

        # the component's immediate network parent: used for subnet support
        self.parent = parent
        # the root network
        # FIXME: this is set by the Network, but should be set by the component using get_parents()
        self.network = None

        # set by the network
        self._runner = None

        # All the input ports are stored here, keyed by name.
        self.ports = None
        self.metadata = {}

        # a stack available to each component
        self._stack = collections.deque()

        # count of packets owned by this component.
        # Whenever the component deactivates, the count must be zero.
        self._packet_count = 0

    def __str__(self):
        return self.get_full_name()

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.get_full_name())

    def __getstate__(self):
        data = self.__dict__.copy()
        for k in ('_runner',):
            data.pop(k)
        return data

    def __setstate__(self, data):
        for key, value in data.items():
            self.__dict__[key] = value
        self.logger = logger

    def _init(self):
        """
        Initialize internal attributes.
        """
        ports = []
        ports.extend(inport.get_inherited(self.__class__))
        ports.extend(outport.get_inherited(self.__class__))
        self.ports = PortCollection(
            self, [p.create_port(self) for p in ports])

    def get_parents(self):
        """
        Returns
        -------
        list of ``rill.engine.network.SubNet``
        """
        from rill.engine.subnet import SubNet
        if self._parents is None:
            parent = self.parent
            parents = []
            while True:
                if parent is None:
                    break
                if not isinstance(parent, SubNet):
                    break
                parents.append(parent)
                parent = parent.parent
            parents.reverse()
            self._parents = parents
        return self._parents

    def get_children(self):
        """
        Returns
        -------
        list of ``Component``
        """
        return []

    # FIXME: property
    def get_name(self):
        """
        Name of the component.

        Returns
        -------
        str
        """
        return self._name

    # FIXME: cached property
    def get_full_name(self):
        """
        Name of the component, including all parent components.

        Returns
        -------
        str
        """
        if self._full_name is None:
            self._full_name = '.'.join(
                [x.get_name() for x in self.get_parents()] + [self.get_name()])
        return self._full_name

    @classmethod
    def get_spec(cls):
        """
        Get a fbp-protocol-compatible component spec

        Returns
        -------
        dict
        """
        import textwrap
        from rill.engine.subnet import SubNet

        return {
            'name': '{0}/{1}'.format(cls.__module__, cls.__name__),
            'description': textwrap.dedent(cls.__doc__ or '').strip(),
            #'icon': '',
            'subgraph': issubclass(cls, SubNet),
            'inPorts': [
                indef.get_spec()
                for indef in cls.inport_definitions
            ],
            'outPorts': [
                outdef.get_spec()
                for outdef in cls.outport_definitions
            ]
        }

    def init(self):
        pass

    @abstractmethod
    def execute(self):
        """Main run method

        Must be implemented by all sub-classes.
        """
        raise NotImplementedError

    @property
    def inports(self):
        """
        Iterate over this component's input ports.

        Yields
        -------
        ``rill.engine.inputport.InputPort``
        """
        for port in flatten_arrays(self.ports):
            if port.kind == 'in' and not is_null_port(port):
                yield port

    @property
    def outports(self):
        """
        Iterate over this component's output ports.

        Yields
        -------
        ``rill.engine.inputport.OutputPort``
        """
        for port in flatten_arrays(self.ports):
            if port.kind == 'out' and not is_null_port(port):
                yield port

    def port(self, port_name, kind=None):
        """
        Get a port on the component.

        Parameters
        ----------
        port_name : str
        kind : {'in', 'out'} or None

        Returns
        -------
        port : ``rill.engine.outputport.OutputPort`` or
               ``rill.engine.outputport.OutputArray`` or
               ``rill.engine.inputport.InputPort`` or
               ``rill.engine.inputport.InputArray``
        """
        reg = re.compile(PORT_NAME_REG)
        m = reg.match(port_name)

        if not m:
            raise FlowError("Invalid port name: " + port_name)

        port_name, index = m.groups()
        if index is not None:
            index = int(index)

        try:
            port = self.ports[port_name]
        except KeyError as err:
            raise FlowError(str(err))

        if kind is not None and port.kind != kind:
            raise FlowError("Expected {}port: got {}".format(kind, type(port)))

        if index is not None:
            if not port.is_array():
                raise FlowError("Element {} specified for non-array "
                                "port {}".format(index, port))
            port = port.get_element(index, create=True)

        return port

    def error(self, msg, errtype=FlowError):
        raise errtype("{}: {}".format(self, msg))

    # Packets --

    def validate_packet(self, packet):
        if not isinstance(packet, Packet):
            raise ComponentException(
                "Expected a Packet instance, got {}".format(type(packet)))

        if self is not packet.owner:
            raise ComponentException(
                "Packet not owned by current component, "
                "or component has terminated (owner is %s)" % packet.owner)

    def create(self, contents):
        """
        Create a Packet and set its owner to this component.

        Returns
        -------
        packet : ``rill.engine.packet.Packet``
        """
        self.logger.debug("Creating packet: " + repr(contents))
        self.network.creates += 1
        # FIXME: this could be nicer
        if contents in (Packet.Type.OPEN, Packet.Type.CLOSE):
            type = contents
            contents = ""
        else:
            type = Packet.Type.NORMAL
        return Packet(contents, self, type)

    def drop(self, packet):
        """
        Drop packet and clear owner reference.

        Parameters
        ----------
        packet : ``rill.engine.packet.Packet``

        Returns
        -------
        contents : object
            packet contents
        """
        self.logger.debug("Dropping packet: " + str(packet))
        self.network.drops += 1
        self.validate_packet(packet)
        packet.clear_owner()
        return packet.get_contents()

    def get_packet_count(self):
        return self._packet_count

    # Packet stack --

    def push(self, packet):
        """
        Push onto the stack and clear owner reference.

        Parameters
        ----------
        packet : ``rill.engine.packet.Packet``
        """
        self.validate_packet(packet)
        self._stack.push(packet)
        packet.clear_owner()

    def pop(self):
        """
        Pop a Packet off the stack or return None if empty.

        Returns
        -------
        ``rill.engine.packet.Packet``
        """
        if len(self._stack) == 0:
            return None

        packet = self._stack.pop()
        packet.set_owner(self)
        return packet

    def stack_size(self):
        """
        Return current size of the stack.

        Returns
        -------
        int
        """
        return len(self._stack)

    # Packet chains --

    def attach(self, packet, name, subpacket):
        """
        Attach a Packet to a named Chain of Packets.

        A Packet may have multiple named Chains attached to it.
        Since Packets are attached to Chains, as well as Chains to Packets,
        this results in an alternation of Packets and Chains, creating a tree structure.

        Parameters
        ----------
        packet : ``rill.engine.packet.Packet``
            the packet to which to attach `subpacket`
        name : str
            name of chain
        subpacket : ``rill.engine.packet.Packet``
        """
        self.validate_packet(subpacket)

        p = packet
        while p.isinstance(p.owner, Packet):
            if p == subpacket:
                self.error("Loop in packet tree structure")
            p = p.owner

        if p == subpacket:
            self.error("Loop in packet tree structure")

        if p.owner is not self:
            self.error("Packet not owned (directly or indirectly) "
                              "by current component")

        chain = packet.chains.get(name)
        if chain is None:
            chain = Chain(name)
            packet.chains[name] = chain

        subpacket.set_owner(packet)
        chain.members.append(subpacket)

    def detach(self, packet, name, subpacket):
        """
        Detach a packet from named chain.

        Parameters
        ----------
        packet : ``rill.engine.packet.Packet``
        name : str
          name of Chain
        subpacket : ``rill.engine.packet.Packet``
        """
        if not isinstance(subpacket, Packet):
            self.error("Expected a Packet instance, got {}".format(
                type(subpacket)))

        root = packet.get_root()
        # FIXME: get_root returns a Component, but this code seems to expect a Packet
        if root.owner is not self:
            self.error(
                "Packet not owned (directly or indirectly) "
                "by current component")

        if packet.chains is None or packet.chains.get(name) is None:
            self.error(
                "Named chain does not exist: {}".format(name))

        chain = packet.chains.get(name)
        if not chain.members.remove(subpacket):
            self.error("Object not found on chain {}".format(name))
        subpacket.set_owner(self)
        return

    # Globals --

    def put_global(self, key, value):
        """
        Set a global value.

        Use this carefully as global data creates a fat coupling.

        Parameters
        ----------
        key : hashable
        value : object

        Returns
        -------
        prev_value : object or None
            previous value
        """
        # FIXME: lock here?
        res = self.network.globals.get(key)
        self.network.globals[key] = value
        return res

    def get_global(self, key):
        """
        Get a global value.

        Parameters
        ----------
        key : hashable

        Returns
        -------
        value : object or None
        """
        # FIXME: lock here?
        return self.network.globals.get(key)

    @classmethod
    def get_type(cls):
        """
        Get component type for serialization
        """
        return '{0}/{1}'.format(cls.__module__,
                                cls.__name__)


class _FunctionComponent(with_metaclass(ABCMeta, Component)):
    """
    Base class for components created from functions via
    ``rill.decorators.component``
    """
    _pass_context = False

    @abstractmethod
    def _execute(self, *args):
        raise NotImplementedError

    def execute(self):
        # FIXME: use named arguments to allow user to change order? could
        # inspect the names and if they're all accounted for, use names,
        # otherwise, fall back to ordered args.
        args = []
        if self._pass_context:
            args.append(self)
        for port in self.ports:
            if is_null_port(port):
                continue
            # currently inports are listed first regardless of decorator order,
            # but we may eventually respect decorator order.
            # note that the in-then-out order is determined in Component._init.
            # FIXME: order may still be a concern for dynamically created ports
            if port.kind == 'in' and port.auto_receive:
                args.append(port.receive_once())
            else:
                args.append(port)
        self._execute(*args)
