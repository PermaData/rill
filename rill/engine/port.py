from abc import ABCMeta, abstractmethod
from collections import OrderedDict, Counter

from typing import Any, Union, Iterable, List

from rill.engine.portdef import IN_NULL, OUT_NULL
from rill.engine.exceptions import FlowError, PacketValidationError
from rill.compat import *


def is_null_port(port):
    if port.kind == 'in' and port.name == IN_NULL:
        return True
    elif port.kind == 'out' and port.name == OUT_NULL:
        return True
    return False


def flatten_collections(ports):
    for port in ports:
        if isinstance(port, PortCollection):
            for p in port.ports():
                yield p
        else:
            yield port


def flatten_arrays(ports):
    for port in ports:
        if port.is_array():
            for elem in port.ports():
                yield elem
        else:
            yield port


@add_metaclass(ABCMeta)
class PortInterface(object):
    """
    Enforces an interface for a basic port.

    These can be either "real" ports, like ``InputPort`` and ``OutputPort``
    or "virtual" ports that implement ``OutputPortInterface`` or
    ``InputPortInterface`` for sending or receiving.  Only "real" ports
    (sublcasses of ``BasePort``) can be opened.
    """
    kind = None

    @abstractmethod
    def close(self):
        """
        Close the port.

        Components downstream that attempt to receive from a closed
        ``InputPort`` receive a `None` packet.  Once all of the input ports are
        closed and their connections are drained of packets, the component will
        be automatically terminated.
        """
        raise NotImplementedError

    @abstractmethod
    def is_closed(self):
        """
        Return whether the port is closed for sending/receiving packets.

        Returns
        -------
        bool
        """
        raise NotImplementedError


@add_metaclass(ABCMeta)
class BasePort(object):
    """
    Base class for all ports
    """

    def __init__(self, component, name, index=None, required=False, type=None,
                 description=None):
        """
        Parameters
        ----------
        component : ``rill.engine.component.Component``
        name : str
        index : Optional[int]
        required : bool
        type : ``rill.engine.types.TypeHandler``
        description : Optional[str]
        """
        assert name is not None
        assert not isinstance(component, str)
        self.component = component
        self._name = name
        self.type = type
        self.index = index
        self.required = required
        self.description = description

    def __str__(self):
        return self.get_full_name()

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.get_full_name())

    @property
    def name(self):
        """
        Returns
        -------
        str
        """
        name = self._name
        if self.index is not None:
            name += '[{}]'.format(self.index)
        return name

    def get_full_name(self):
        """
        Returns
        -------
        str
        """
        return '{}.{}'.format(self.component, self.name)

    def is_element(self):
        """
        Return whether the port is a member of an array.

        Returns
        -------
        bool
        """
        return self.index is not None

    @abstractmethod
    def validate(self):
        """
        Runs prior to opening a port to validate that the port can be opened.
        """
        raise NotImplementedError

    @abstractmethod
    def open(self):
        """
        Open the port.

        Internal use only
        """
        raise NotImplementedError

    @abstractmethod
    def is_array(self):
        """
        Return whether the port is an array.

        Array ports contain sparse indices to their element ports.

        Returns
        -------
        bool
        """
        raise NotImplementedError


class Port(BasePort):
    """
    Base class for all singular (non-array) ports.
    """

    def validate_packet_contents(self, packet_content):
        """
        Validate packet data.

        Parameters
        ----------
        packet_content : Any

        Returns
        -------
        Any
            original content, or content conformed based on the ``TypeHandler``
        """
        if self.type is not None:
            try:
                conformed = self.type.validate(packet_content)
            except PacketValidationError as err:
                # catch and re-raise to provide port name in error message
                raise FlowError(
                    "{} found invalid type: {}".format(self, err))
            else:
                return conformed if conformed is not None else packet_content
        return packet_content

    def validate(self):
        """
        Runs prior to opening a port to validate that the port can be opened.
        """
        if not self.is_connected() and self.required:
            raise FlowError(
                "{} port is required, but not connected".format(self))

    def is_array(self):
        return False

    @abstractmethod
    def is_connected(self):
        """
        Return whether the port is connected

        Returns
        -------
        bool
        """
        raise NotImplementedError

    @abstractmethod
    def is_null(self):
        raise NotImplementedError


@add_metaclass(ABCMeta)
class PortContainerMixin(object):
    """
    Provides functionality common to classes which are containers for ports.
    """

    _valid_classes = None

    def _check_port_types(self):
        offenders = [p for p in self.ports()
                     if not isinstance(p, self._valid_classes)]
        if offenders:
            raise ValueError(
                "{}: ports must all be instances of {}: {}".format(
                    self.__class__.__name__, self._valid_classes,
                    ', '.join(repr(o) for o in offenders)))

    @abstractmethod
    def iter_ports(self):
        """
        Iterate over all ports held by this container.

        Returns
        -------
        Iterable[Union[``rill.engine.inputport.InputPort``, ``rill.engine.outputport.OutputPort``]]
        """
        raise NotImplementedError

    def ports(self):
        """
        Return all the ports held by this container.

        This excludes ``ArrayPort``s but includes their members.

        Returns
        -------
        List[Union[``rill.engine.inputport.InputPort``, ``rill.engine.outputport.OutputPort``]]
        """
        return list(self.iter_ports())

    def close(self):
        """
        Close all the ports held by this container
        """
        for port in self.ports():
            port.close()

    def is_closed(self):
        """
        Return whether all child ports are closed.

        Returns
        -------
        bool
        """
        return all(p.is_closed() for p in self.ports())


class ArrayPort(BasePort, PortContainerMixin):
    """
    Base class for array ports, which hold a sparse array of ports, each
    identified by an index.
    """
    port_class = None

    def __init__(self, component, name, fixed_size=None, **kwargs):
        super(ArrayPort, self).__init__(component, name, **kwargs)
        self._elements = {}
        self.fixed_size = None
        if fixed_size:
            for i in range(0, fixed_size):
                self.create_element(i)
        self.fixed_size = fixed_size

    def iter_ports(self):
        """
        Iterate over element ports.

        Returns
        -------
        Iterable[Union[``rill.engine.inputport.InputPort``, ``rill.engine.outputport.OutputPort``]]
        """
        for index in sorted(self._elements.keys()):
            yield self._elements[index]

    __iter__ = iter_ports

    def __getitem__(self, index):
        """
        Returns
        -------
        Union[``rill.engine.inputport.InputPort``, ``rill.engine.outputport.OutputPort``]
        """
        # FIXME: if the component is in an uninitialized state have this
        # default to get_element(index, create=True).  useful while building
        # networks. e.g.  mycomp.port('IN')[2]
        return self._elements[index]

    def __setitem__(self, index, port):
        self._elements[index] = port

    def __len__(self):
        """
        Returns
        -------
        int
        """
        return len(self._elements)

    def next_available_index(self):
        """
        Get the lowest unused index in the array.

        Returns
        -------
        int
            port index
        """
        if not self._elements:
            return 0
        for i, (index, port) in enumerate(sorted(self._elements.items())):
            if i != index or not port.is_connected():
                return i
        return index + 1

    def get_element(self, index=None, create=False):
        """
        Get an element port within the array.

        Parameters
        ----------
        index : int
            index of element. If None, the next available index is used
        create : bool
            whether to create the element if it does not exist

        Returns
        -------
        Union[``rill.engine.inputport.InputPort``, ``rill.engine.outputport.OutputPort``]
        """
        if index is None:
            index = self.next_available_index()
        result = self._elements.get(index)
        if result is None and create:
            result = self.create_element(index)
        return result

    def _create_element(self, index):
        return self.port_class(self.component, self.name, index=index,
                               type=self.type, required=self.required)

    def create_element(self, index=None):
        """
        Create an element port within the array.

        Parameters
        ----------
        index : Optional[int]
            index of element. If None, the next available index is used

        Returns
        -------
        Union[``rill.engine.inputport.InputPort``, ``rill.engine.outputport.OutputPort``]
        """
        if self.fixed_size is not None:
            raise FlowError(
                "New elements cannot be added to array ports with fixed size "
                "after instantiation")
        if index is None:
            index = self.next_available_index()
        comp = self._create_element(index)
        self._elements[index] = comp
        return comp

    def validate(self):
        self._check_port_types()

        if not self.ports() and self.required:
            raise FlowError(
                "Required {} {} has no members".format(
                    self.port_class.__name__, self))

        if self.fixed_size is not None and self.required:
            missing = []
            for port in self.ports():
                if not port.is_connected():
                    missing.append(str(port))
            if missing:
                raise FlowError(
                    "Required {} {} has missing elements: {}".format(
                        self.port_class.__name__, self, ', '.join(missing)))

    def open(self):
        """
        Open the element ports within the array.

        Prepares ports to receive data.
        """
        for port in self.ports():
            port.open()

    def is_array(self):
        return True


class BasePortCollection(PortContainerMixin):
    """
    Holds a collection of ports and provides iteration and lookup by
    name.
    """
    # NOTE: we have to be careful about the attributes and methods we add to
    # this class and its parent because PortCollection provides attribute-based
    # lookup of ports, which could clash with actual attributes and methods.
    _valid_classes = (BasePort,)

    def __init__(self, component, ports):
        """
        Parameters
        ----------
        component : ``rill.engine.component.Component``
        ports : Iterable[``BasePort``]
        """
        self.component = component
        self._ports = OrderedDict((p.name, p) for p in self._prep_args(ports))
        self._check_port_types()

    def _prep_args(self, ports):
        return flatten_arrays(flatten_collections(ports))

    def iter_ports(self):
        """
        Iterate over all ports in the container.

        Returns
        -------
        Iterable[Union[``rill.engine.inputport.InputPort``, ``rill.engine.outputport.OutputPort``]]
        """
        for port in self._ports.values():
            yield port

    def __getitem__(self, item):
        """
        Returns
        -------
        Union[``rill.engine.inputport.InputPort``, ``rill.engine.outputport.OutputPort``]
        """
        try:
            return self._ports[item]
        except KeyError as e:
            raise_from(KeyError("Port {}.{} does not exist".format(
                self.component, item)), e)

    # def _pop_null_port(self):
    #     # special handling of NULL port. this port is removed from the port map
    #     # before a component's execute method is run, so they are not publicly
    #     # available
    #     port = self._ports.pop('NULL')
    #     if port.is_connected():
    #         return port


class PortCollection(BasePortCollection):
    """
    Acts as a Mapping Container for ports, as well as provides attribute-based
    lookups.
    """
    def __init__(self, component, ports):
        super(PortCollection, self).__init__(component, ports)
        for pname, port in self._ports.items():
            setattr(self, pname, port)

    def _prep_args(self, ports):
        # unlike BasePortCollection, we don't flatten arrays
        ports = list(flatten_collections(ports))
        # Enforce unique names between input and output ports. This ensures
        # that _FunctionComponent functions can receive a named argument per
        # port
        names = [p.name for p in ports]
        dupes = [n for n, count in Counter(names).items() if count > 1]
        if dupes:
            raise FlowError("{}: Duplicate port names: {}".format(
                self.component, ', '.join(dupes)))
        return ports

    def __iter__(self):
        return self.iter_ports()

    def __len__(self):
        return len(self._ports)

    def __setitem__(self, item, port):
        self._ports[item] = port
        setattr(self, item, port)
