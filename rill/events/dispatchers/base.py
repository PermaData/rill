from abc import ABCMeta, abstractmethod
from rill.compat import *


@add_metaclass(ABCMeta)
class GraphDispatcher(object):
    """
    Propagates updates to a graph, usually emanating from a listener, to a
    destination graph store, such as a database, socket connection, or
    in-memory representation.
    """

    @abstractmethod
    def new_graph(self, payload):
        """
        Create a new graph.
        """

    @abstractmethod
    def set_graph_metadata(self, payload):
        """
        Set Graph Metadata
        """

    @abstractmethod
    def rename_graph(self, payload):
        """
        Rename Graph
        """

    @abstractmethod
    def add_node(self, payload):
        """
        Add a component instance.
        """

    @abstractmethod
    def remove_node(self, payload):
        """
        Destroy component instance.
        """

    @abstractmethod
    def rename_node(self, payload):
        """
        Rename component instance.
        """

    @abstractmethod
    def set_node_metadata(self, payload):
        """
        Sends changenode event
        """

    @abstractmethod
    def add_edge(self, payload):
        """
        Connect ports between components.
        """

    @abstractmethod
    def remove_edge(self, payload):
        """
        Disconnect ports between components.
        """

    @abstractmethod
    def set_edge_metadata(self, payload):
        """
        Send changeedge event'
        """

    @abstractmethod
    def initialize_port(self, payload):
        """
        Set the inital packet for a component inport.
        """

    @abstractmethod
    def uninitialize_port(self, payload):
        """
        Remove the initial packet for a component inport.
        """

    @abstractmethod
    def add_inport(self, payload):
        """
        Add inport to graph
        """

    @abstractmethod
    def remove_inport(self, payload):
        """
        Remove inport from graph
        """
        self._remove_inport(payload['graph_id'], payload['public'])

    @abstractmethod
    def set_inport_metadata(self, payload):
        """
        Send the metadata on an exported inport
        """

    @abstractmethod
    def add_outport(self, payload):
        """
        Add outport to graph
        """

    @abstractmethod
    def remove_outport(self, payload):
        """
        Remove outport from graph
        """

    @abstractmethod
    def set_outport_metadata(self, payload):
        """
        Send the metadata on an exported inport
        """
