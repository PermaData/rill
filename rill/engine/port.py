from abc import ABCMeta, abstractmethod
from collections import OrderedDict

from future.utils import with_metaclass
from future.utils import raise_from

from rill.engine.types import get_type_handler
from rill.engine.exceptions import FlowError


def flatten_collections(ports):
    for port in ports:
        if isinstance(port, PortCollection):
            for p in port.port():
                yield p
        else:
            yield port


class PortInterface(with_metaclass(ABCMeta, object)):
    """
    Enforces an interface for a basic port that can be opened and closed.

    Used by ``ArrayPort``, ``OutputPortInterface`` and ``InputPortInterface``
    """

    @abstractmethod
    def is_connected(self):
        raise NotImplementedError

    @abstractmethod
    def open(self):
        """
        Open the port.

        Internal use only
        """
        raise NotImplementedError

    @abstractmethod
    def close(self):
        """
        Close the port.
        """
        raise NotImplementedError

    @abstractmethod
    def is_closed(self):
        raise NotImplementedError


class BasePort(with_metaclass(ABCMeta, object)):
    """
    Base class for all ports
    """

    def __init__(self, component, name, index=None, optional=True, type=None,
                 description=None):
        assert name is not None
        assert not isinstance(component, str)
        self.component = component
        self._name = name
        self.type = get_type_handler(type) if type is not None else None
        self.index = index
        self.optional = optional
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
    def is_array(self):
        raise NotImplementedError


class Port(BasePort):
    """
    Base class for all singular (non-array) ports.
    """

    def __init__(self, *args, **kwargs):
        super(Port, self).__init__(*args, **kwargs)
        self._connection = None

    def validate_packet_contents(self, packet_content):
        """
        Validate packet data.

        Parameters
        ----------
        packet_content : object

        Returns
        -------
        packet_content : object
            original content, or content conformed based on the ``TypeHandler``
        """
        if self.type is not None:
            try:
                conformed = self.type.validate(packet_content)
            except Exception as err:
                raise FlowError(
                    "{} found invalid type: {}".format(self, err))
            else:
                return conformed if conformed is not None else packet_content
        return packet_content

    def open(self):
        if not self.is_connected() and not self.optional:
            raise FlowError(
                "{} port is required, but not connected".format(self))

    def is_array(self):
        return False

    def is_connected(self):
        """
        Return whether the output port is connected

        Returns
        -------
        bool
        """
        return self._connection is not None


class PortContainerMixin(with_metaclass(ABCMeta, object)):
    """
    Provides functionality common to classes which are containers for ports.
    """

    valid_classes = None

    def check_port_types(self):
        offenders = [p for p in self.ports()
                     if not isinstance(p, self.valid_classes)]
        if offenders:
            raise ValueError(
                "{}: ports must all be instances of {}: {}".format(
                    self.__class__.__name__, self.valid_classes,
                    ', '.join(repr(o) for o in offenders)))

    @abstractmethod
    def iter_ports(self):
        """
        Iterate over all ports held by this container.

        This excludes ``ArrayPort``s but includes their members.

        Yields
        -------
        ``rill.engine.inputport.InputPort`` or
        ``rill.engine.outputport.OutputPort``
        """
        raise NotImplementedError

    def ports(self):
        """
        Return all the ports held by this container.

        This excludes ``ArrayPort``s but includes their members.

        Returns
        -------
        list of ``rill.engine.inputport.InputPort`` or
        list of ``rill.engine.outputport.OutputPort``
        """
        return list(self.iter_ports())

    def open(self):
        """
        Open all the ports held by this container
        """
        for port in self.ports():
            port.open()

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

    def is_connected(self):
        """
        Return whether any child ports are connected.

        Returns
        -------
        bool
        """
        return any(p.is_connected() for p in self.ports())


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

        Yields
        -------
        ``rill.engine.inputport.InputPort`` or
        ``rill.engine.outputport.OutputPort``
        """
        for index in sorted(self._elements.keys()):
            yield self._elements[index]

    __iter__ = iter_ports

    def __getitem__(self, index):
        # FIXME: if the component is in an uninitialized state have this
        # default to get_element(index, create=True).  useful while building
        # networks. e.g.  mycomp.port('IN')[2]
        return self._elements[index]

    def __setitem__(self, index, port):
        self._elements[index] = port

    def __len__(self):
        return len(self._elements)

    def next_available_index(self):
        """
        Get the lowest unused index in the array.

        Returns
        -------
        index : int
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
        ``rill.engine.inputport.InputPort`` or
        ``rill.engine.outputport.OutputPort``
        """
        if index is None:
            index = self.next_available_index()
        result = self._elements.get(index)
        if result is None and create:
            result = self.create_element(index)
        return result

    def _create_element(self, index):
        return self.port_class(self.component, self.name, index=index,
                               type=self.type, optional=self.optional)

    def create_element(self, index=None):
        """
        Create an element port within the array.

        Parameters
        ----------
        index : int
            index of element. If None, the next available index is used

        Returns
        -------
        ``rill.engine.inputport.InputPort`` or
        ``rill.engine.outputport.OutputPort``
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

    def open(self):
        """
        Open the element ports within the array.

        Prepares ports to receive data.
        """
        self.check_port_types()

        if not self.ports() and not self.optional:
            raise FlowError(
                "Required {} {} has no members".format(
                    self.port_class.__name__, self))

        if self.fixed_size is not None and not self.optional:
            missing = []
            for port in self.ports():
                if not port.is_connected():
                    missing.append(str(port))
            if missing:
                raise FlowError(
                    "Required {} {} has missing elements: {}".format(
                        self.port_class.__name__, self, ', '.join(missing)))

        super(ArrayPort, self).open()

    def is_connected(self):
        return False

    def is_array(self):
        return True


class PortCollection(PortContainerMixin):
    """
    Holds a collection of ports and provides iteration and lookup by
    name.

    For variants which do additional type checking see ``InputCollection` and
    ``OutputCollection`.
    """

    def __init__(self, component, ports):
        self.component = component
        self._ports = OrderedDict((p.name, p) for p
                                  in flatten_collections(ports))
        self.check_port_types()

    def root_ports(self, include_null=False):
        """
        Return the root ports, including array ports but not their children.

        Yields
        -------
        ``Port`` or ``ArrayPort``
        """
        # cast to list for python 3 compat
        ports = list(self._ports.values())
        if not include_null:
            ports = [p for p in ports if p.name != 'NULL']
        return ports

    def iter_ports(self):
        """
        Iterate over all ports and their children.

        Yields
        -------
        ``rill.engine.inputport.InputPort`` or
        ``rill.engine.outputport.OutputPort``
        """
        for port in self.root_ports():
            if port.is_array():
                for elem in port.ports():
                    yield elem
            else:
                yield port

    def __getattr__(self, item):
        try:
            return self._ports[item]
        except KeyError:
            raise AttributeError(item)

    def __getitem__(self, item):
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
