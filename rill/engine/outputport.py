from abc import ABCMeta, abstractmethod

from future.utils import with_metaclass
from typing import List, Union, Any

from rill.engine.port import (Port, ArrayPort, BasePortCollection,
                              PortInterface, OUT_NULL)
from rill.engine.packet import Packet


class OutputInterface(with_metaclass(ABCMeta, PortInterface)):
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
        self._is_closed = True

    def open(self):
        self._is_closed = False
        if self.is_connected():
            self._connection._sender_count += 1

    def close(self):
        """
        Close this OutputPort.

        This is a signal that no further packets will be
        sent via self OutputPort. Since more than one OutputPort may feed a
        given Connection, this does not necessarily close the Connection.
        """
        self.sender.logger.debug("Closing", port=self)
        if self.is_connected() and not self.is_closed():
            self._is_closed = True
            # -- synchronized (self._connection):
            if not self._connection.is_closed():
                # indicate that one sender has terminated
                self._connection.indicate_sender_closed()
                # -- end
        self.sender.logger.debug("Close finished", port=self)

    def is_closed(self):
        """
        Return whether the output port is closed

        Returns
        -------
        bool
        """
        return self._is_closed

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
        if not self._connection.send(packet, self):
            # FIXME: would be good to check if all outports are closed and
            # terminate the component. otherwise, every component must be
            # written to check is_closed() and break
            self.component.drop(packet)
            self.close()
            # raise FlowError("{}: Could not deliver packet to {}".format(
            #     self._connection.get_name(), self.get_name()))
        self.sender.logger.debug("Packet sent to {}", port=self,
                                 args=[self._connection.inport])
        return True

    def downstream_count(self):
        """
        Get the downstream packet count.

        Returns
        -------
        int
        """
        if self.is_connected():
            return self._connection.count()
        else:
            return 0


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
        """
        """
        # get results in parallel
        group = gevent.pool.Group()
        failed = list(group.imap_unordered(lambda x: x.send(packet.clone()),
                                           self.ports()))
        return any(failed)
