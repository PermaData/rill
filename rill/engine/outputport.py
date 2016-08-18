from abc import ABCMeta, abstractmethod

from typing import List, Union, Any

from rill.engine.port import (Port, ArrayPort, BasePortCollection,
                              PortInterface, OUT_NULL)
from rill.engine.packet import Packet
from rill.compat import *


@add_metaclass(ABCMeta)
class OutputInterface(PortInterface):
    """
    Enforces a common interface for all classes which send packets
    """
    kind = 'out'

    @property
    def sender(self):
        """
        Returns
        -------
        ``rill.engine.runner.ComponentRunner``
        """
        return self.component._runner

    @abstractmethod
    def send(self, packet):
        raise NotImplementedError


class OutputPort(Port, OutputInterface):
    """
    An ``OutputPort`` sends packets via a ``BaseConnection``.
    """

    def __init__(self, component, name, **kwargs):
        super(OutputPort, self).__init__(component, name, **kwargs)
        # _sender_count is how many connections this port passes into.
        self._sender_count = 0
        # type: List[rill.engine.inputport.Connection]
        self._connections = []

    def open(self):
        """Registers this port as open with all associated connections."""
        for connection in self._connections:
            connection._sender_count += 1
        self._sender_count = len(self._connections)

    def close(self):
        """
        Close this OutputPort.

        This is a signal that no further packets will be
        sent via self OutputPort. Since more than one OutputPort may feed a
        given Connection, this does not necessarily close the Connection.
        """
        self.sender.logger.debug("Closing", port=self)
        if self.is_connected() and not self.is_closed():
            self._sender_count = 0
            # -- synchronized (self._connections):
            for connection in self._connections:
                if not connection.is_closed():
                    # indicate that one sender has terminated
                    connection.indicate_sender_closed()
            # -- end
        self.sender.logger.debug("Close finished", port=self)

    def is_closed(self):
        """
        Return whether the output port is closed

        Returns
        -------
        bool
        """
        return self._sender_count == 0

    def is_connected(self):
        """Returns whether this port is joined to at least one connection."""
        return bool(self._connections)

    def is_null(self):
        return self.name == OUT_NULL

    def send(self, packet):
        """
        Send a packet to this Port.

        The thread is suspended if no capacity is currently available.

        Do not reference the packet after sending - another component may be
        modifying it.

        Parameters
        ----------
        packet : Union[``rill.engine.packet.Packet``, Any]
            the packet to send

        Returns
        -------
        bool
            Whether the send was successful
        """
        if not isinstance(packet, Packet):
            packet = self.component.create(packet)

        # FIXME: Added this check, but it changes behavior slightly from before:  owner check occurs before is_connected
        # NOTE: Checks type of data against expected
        self.sender.component.validate_packet(packet)

        # if packet is None:
        #     raise FlowError(
        #         "{}: Null packet referenced in 'send' method call: " + self.sender.get_name())

        if not self.is_connected():
            self.component.drop(packet)
            return False

        # if self.sender != packet.owner:
        #     # the owner is set to the current component in Connection.receive()
        #     # and Component.create() so that's a pretty good guarantee that
        #     # this will never happen
        #     raise FlowError(
        #         "Packet being sent not owned by current component: " + self.sender.get_name())

        if self.is_closed():
            self.sender.logger.debug("Send: Output closed. Failed to deliver "
                                     "packet {}".format(packet),
                                     port=self)
            # FIXME: raise error here? drop the packet?
            self.component.drop(packet)
            return False

        self.validate_packet_contents(packet.get_contents())
        self.sender.logger.debug("Sending packet: {}".format(packet),
                                 port=self)

        do_clone = len(self._connections) > 1
        for connection in self._connections:
            # Send packet along each connection
            p = packet.clone() if do_clone else packet
            if not connection.send(p, self):
                # FIXME: would be good to check if all outports are closed and
                # terminate the component. otherwise, every component must be
                # written to check is_closed() and break
                self.component.drop(p)
                if not connection.is_closed():
                    # indicate that one sender has terminated
                    connection.indicate_sender_closed()
                self._sender_count -= 1
                # raise FlowError("{}: Could not deliver packet to {}".format(
                #     self._connection.get_name(), self.get_name()))
            self.sender.logger.debug("Packet sent to {}", port=self,
                                     args=[connection.inport])
        return True

    def downstream_count(self):
        """
        Get the downstream packet count.

        Returns
        -------
        int
            How many packets are queued in all joined connections
        """
        return sum([c.count() for c in self._connections])


class OutputArray(ArrayPort, PortInterface):
    _valid_classes = (OutputInterface,)
    port_class = OutputPort
    kind = 'out'


class BaseOutputCollection(BasePortCollection, OutputInterface):
    """Base class for output port collections"""
    _valid_classes = (OutputInterface, OutputArray)


# class OutputCollection(BaseOuputCollection, PortInterface):
#     def __iter__(self):
#         return self.iter_ports()


class LoadBalancedOutputCollection(BaseOutputCollection):
    """
    Provides methods for sending to the optimal port within the collection.
    """

    def next_port(self):
        """
        Find the port with the fewest number of downstream packets.

        Returns
        -------
        ``OutputPort``
        """
        result = None
        backlog = None
        for port in self.ports():
            count = port.downstream_count()
            if backlog is None or count <= backlog:
                backlog = count
                result = port
        return result

    def send(self, packet):
        """
        Send `packet` to the port with the fewest number of downstream packets.
        """
        return self.next_port().send(packet)


import gevent.pool


class ForkedOutputCollection(BaseOutputCollection):
    def send(self, packet):
        """Returns whether any of the sends failed
        """
        # get results in parallel
        group = gevent.pool.Group()
        failed = list(group.imap_unordered(lambda x: x.send(packet.clone()),
                                           self.ports()))
        return any(failed)
