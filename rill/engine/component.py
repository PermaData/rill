from collections import OrderedDict, deque
import logging
import re
from abc import ABCMeta, abstractmethod

from typing import Any, List, Iterable

from rill.engine.port import (PortCollection, flatten_arrays, is_null_port,
                              IN_NULL, OUT_NULL)
from rill.engine.packet import Packet, Chain
from rill.engine.exceptions import FlowError, ComponentError
from rill.engine.utils import LogFormatter
from rill.utils import cache, classproperty
from rill.decorators import inport, outport
from rill.compat import *


PORT_NAME_REG = r"^([a-zA-Z][_a-zA-Z0-9]*)(?:\[(\d+)\])?$"

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)
logger = LogFormatter(_logger, {})


@inport(IN_NULL)
@outport(OUT_NULL)
@add_metaclass(ABCMeta)
class Component(object):
    # type: List[rill.engine.portdef.InputPortDefinition]
    _inport_definitions = []
    # type: List[rill.engine.portdef.OutputPortDefinition]
    _outport_definitions = []
    _self_starting = False
    _must_run = False
    type_name = None
    hidden = False

    # same as module-level logger, but provided here for convenience
    logger = logger

    def __init__(self, name):
        """

        Parameters
        ----------
        name : str
            Unique identifier of this component within its graph.
        """
        assert name is not None
        self._name = name

        # set by the network
        # type: rill.engine.runner.ComponentRunner
        self._runner = None

        # All the input ports are stored here, keyed by name.
        # type: PortCollection
        self.ports = None
        self.metadata = {}

        # a stack available to each component
        self._stack = deque()

        # count of packets owned by this component.
        # Whenever the component deactivates, the count must be zero.
        self._packet_count = 0

    def __str__(self):
        return self.get_full_name()

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.get_full_name())

    def __getstate__(self):
        data = self.__dict__.copy()
        data['_runner'] = None
        return data

    def __setstate__(self, data):
        for key, value in data.items():
            self.__dict__[key] = value
        self.logger = logger

    def _init(self):
        """
        Initialize internal attributes.
        """
        # NOTE: Not called? at least not here
        self.ports = PortCollection(
            self, [p.create_port(self) for p in self.port_definitions().values()])

    # FIXME: rename to root_network
    @property
    def network(self):
        """
        The root network.

        Returns
        -------
        ``rill.engine.network.Network``
            The network this component is running within.
        """
        return self._runner.network

    def get_parents(self):
        """
        Returns
        -------
        List[``rill.engine.network.Graph``]
            A list of all the graphs that this component belongs to.
        """
        if self._runner is None:
            return []
        return [p.graph for p in self._runner.get_parents()]

    def get_children(self):
        """
        Returns
        -------
        List[``Component``]
            A list of all child Components owned by this one.
        """
        # NOTE: Components cannot have children?
        return []

    # FIXME: remove this in favor of name property
    def get_name(self):
        """
        Name of the component.

        Returns
        -------
        str
        """
        return self._name

    @property
    def name(self):
        """
        Name of the component.

        Returns
        -------
        str
            The name of this component, as set in __init__.
        """
        return self._name

    # FIXME: we can't cache this because get_parents() may be empty the first
    # time it is called if there is no runner yet.  Look into a way to return
    # a non-cached result.
    # @cache
    def get_full_name(self):
        """
        Name of the component, including all parent components.

        Returns
        -------
        str
            A dot-separated list of parent graphs, with this Component's name
            on the end.
        """
        parts = [x.name for x in self.get_parents() if x.name is not None]
        return '.'.join(parts + [self.get_name()])

    @classmethod
    def get_type(cls):
        """
        Get component type for serialization

        Returns
        -------
        str
            Path to this class definition
        """
        name = cls.type_name or cls.__name__
        return '{0}/{1}'.format(cls.__module__, name)

    @classmethod
    def get_spec(cls):
        """
        Get a fbp-protocol-compatible component spec

        Returns
        -------
        dict
            A packet according to the specification here:
            https://flowbased.github.io/fbp-protocol/#component
        """
        import textwrap
        from rill.engine.subnet import SubGraph

        return {
            'name': cls.get_type(),
            'description': textwrap.dedent(cls.__doc__ or '').strip(),
            #'icon': '',
            'subgraph': issubclass(cls, SubGraph),
            'inPorts': [
                indef.get_spec()
                for indef in cls.inport_definitions.values()
            ],
            'outPorts': [
                outdef.get_spec()
                for outdef in cls.outport_definitions.values()
            ]
        }

    def init(self):
        """
        Override to perform custom initialization at the beginning of
            execution.
        """
        pass

    @abstractmethod
    def execute(self):
        """
        Main run method

        Must be implemented by all sub-classes.
        """
        raise NotImplementedError

    # FIXME: make this a function with a include_null option. return a list.
    @property
    def inports(self):
        """
        Iterate over this component's input ports.

        Returns
        -------
        Iterable[``rill.engine.inputport.InputPort``]
        """
        for port in flatten_arrays(self.ports):
            if port.kind == 'in':
                yield port

    @property
    def outports(self):
        """
        Iterate over this component's output ports.

        Returns
        -------
        Iterable[``rill.engine.inputport.OutputPort``]
        """
        for port in flatten_arrays(self.ports):
            if port.kind == 'out':
                yield port

    def port(self, port_name, kind=None):
        """
        Get a port on the component.

        Handles names with array indices (e.g. 'IN[0]')

        Parameters
        ----------
        port_name : str
        kind : {'in', 'out'} or None
            assert that the retrieved port is of the given type

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
            raise FlowError("Expected {} port: got {}".format(kind, type(port)))

        if index is not None:
            if not port.is_array():
                raise FlowError("Element {} specified for non-array "
                                "port {}".format(index, port))
            port = port.get_element(index, create=True)

        return port

    @classproperty
    def inport_definitions(cls):
        """
        Returns
        -------
        OrderedDict
        """
        return OrderedDict((p.name, p) for p in inport.get_inherited(cls))

    @classproperty
    def outport_definitions(cls):
        """
        Returns
        -------
        OrderedDict
        """
        return OrderedDict((p.name, p) for p in outport.get_inherited(cls))

    @classmethod
    def port_definitions(cls):
        # FIXME: make this better.
        # - use PortCollection?
        # - don't use annotation classes to do the inheritance work (use super)
        return OrderedDict(list(cls.inport_definitions.items()) +
                           list(cls.outport_definitions.items()))

    def error(self, msg, errtype=FlowError):
        raise errtype("{}: {}".format(self, msg))

    # Packets --

    def validate_packet(self, packet):
        if not isinstance(packet, Packet):
            raise ComponentError(
                "Expected a Packet instance, got {}".format(type(packet)))

        if self is not packet.owner:
            raise ComponentError(
                "Packet not owned by current component, "
                "or component has terminated (owner is %s)" % packet.owner)

    def create(self, contents):
        """
        Create a Packet and set its owner to this component.

        Parameters
        ----------
        contents : Any

        Returns
        -------
        ``rill.engine.packet.Packet``
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
        Any
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
        value : Any

        Returns
        -------
        Any
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
        Any
        """
        # FIXME: lock here?
        return self.network.globals.get(key)


class _FunctionComponent(Component):
    """
    Base class for components created from functions via
    ``rill.decorators.component``
    """
    _pass_context = False

    def _execute(self, *args):
        raise NotImplementedError

    def get_args(self):
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
        return args

    def execute(self):
        self._execute(*self.get_args())
