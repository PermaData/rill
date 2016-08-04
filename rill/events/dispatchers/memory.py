from rill.engine.network import Graph
from rill.engine.types import FBP_TYPES
from rill.engine.exceptions import FlowError
from rill.events.dispatchers.base import GraphDispatcher

from typing import Dict


class InMemoryGraphDispatcher(GraphDispatcher):
    """
    Propagate updates to a set of in-memory Graph objects
    """
    def __init__(self):
        # type: Dict[str, Graph]
        self._graphs = {}  # Graph instances, keyed by graph ID

    @staticmethod
    def _get_port(graph, data, kind):
        return graph.get_component_port((data['node'], data['port']),
                                        index=data.get('index'),
                                        kind=kind)

    def get_graph(self, graph_id):
        """
        Parameters
        ----------
        graph_id : str
            unique identifier for the graph to create or get

        Returns
        -------
        graph : ``rill.engine.network.Graph``
            the graph object.
        """
        try:
            return self._graphs[graph_id]
        except KeyError:
            raise FlowError('Requested graph not found: {}'.format(graph_id))

    # -- overrides

    def new_graph(self, payload):
        """
        Create a new graph.
        """
        return self._new_graph(payload['id'])

    def add_node(self, payload):
        """
        Add a component instance.
        """
        self._add_node(payload['graph_id'], payload['id'],
                       payload['component'],
                       payload.get('metadata', {}))

    def remove_node(self, payload):
        """
        Destroy component instance.
        """
        self._remove_node(payload['graph_id'], payload['id'])

    def rename_node(self, payload):
        """
        Rename component instance.
        """
        self._rename_node(payload['graph_id'], payload['from'],
                          payload['to'])

    def set_node_metadata(self, payload):
        """
        Sends changenode event
        """
        self._set_node_metadata(payload['graph_id'],
                                payload['id'],
                                payload['metadata'])

    def add_edge(self, payload):
        """
        Connect ports between components.
        """
        self._add_edge(payload['graph_id'], payload['src'],
                       payload['tgt'],
                       payload.get('metadata', {}))

    def remove_edge(self, payload):
        """
        Disconnect ports between components.
        """
        self._remove_edge(payload['graph_id'], payload['src'],
                          payload['tgt'])

    def set_edge_metadata(self, payload):
        """
        Send changeedge event'
        """
        self._set_edge_metadata(payload['graph_id'],
                                payload['src'],
                                payload['tgt'],
                                payload['metadata'])

    def initialize_port(self, payload):
        """
        Set the inital packet for a component inport.
        """
        self._initialize_port(payload['graph_id'], payload['tgt'],
                              payload['src']['data'])

    def uninitialize_port(self, payload):
        """
        Remove the initial packet for a component inport.
        """
        self._uninitialize_port(payload['graph_id'],
                                payload['tgt'])

    def add_inport(self, payload):
        """
        Add inport to graph
        """
        self._add_export(payload['graph_id'], payload['node'],
                         payload['port'], payload['public'],
                         payload['metadata'])

    def remove_inport(self, payload):
        """
        Remove inport from graph
        """
        self._remove_inport(payload['graph_id'], payload['public'])

    def set_inport_metadata(self, payload):
        """
        Send the metadata on an exported inport
        """
        self._change_inport(payload['graph_id'], payload['public'],
                            payload['metadata'])

    def add_outport(self, payload):
        """
        Add outport to graph
        """
        self._add_export(payload['graph_id'], payload['node'],
                         payload['port'], payload['public'],
                         payload['metadata'])

    def remove_outport(self, payload):
        """
        Remove outport from graph
        """
        self._remove_outport(payload['graph_id'], payload['public'])

    def set_outport_metadata(self, payload):
        """
        Send the metadata on an exported inport
        """
        self._change_outport(payload['graph_id'], payload['public'],
                             payload['metadata'])

    # -- implementation

    def _get_graph(self, graph_id):
        """
        Parameters
        ----------
        graph_id : str
            unique identifier for the graph to create or get

        Returns
        -------
        graph : ``rill.engine.network.Graph``
            the graph object.
        """
        try:
            return self._graphs[graph_id]
        except KeyError:
            raise FlowError('Requested graph not found')

    def _new_graph(self, graph_id):
        """
        Create a new graph.
        """
        self.logger.debug('Graph {}: Initializing'.format(graph_id))
        # FIXME: set graph name to graph_id?
        graph = Graph()
        self.add_graph(graph_id, graph)
        return graph

    def _add_graph(self, graph_id, graph):
        """
        Parameters
        ----------
        graph_id : str
        graph : ``rill.engine.network.Graph``
        """
        self._graphs[graph_id] = graph

    def _add_node(self, graph_id, node_id, component_id, metadata=None):
        """
        Add a component instance.
        """
        self.logger.debug('Graph {}: Adding node {}({})'.format(
            graph_id, component_id, node_id))

        graph = self.get_graph(graph_id)

        component_class = self._component_types[component_id]['class']
        component = graph.add_component(node_id, component_class)
        component.metadata.update(metadata or {})

    def _remove_node(self, graph_id, node_id):
        """
        Destroy component instance.
        """
        self.logger.debug('Graph {}: Removing node {}'.format(
            graph_id, node_id))

        graph = self.get_graph(graph_id)
        graph.remove_component(node_id)

    def _rename_node(self, graph_id, orig_node_id, new_node_id):
        """
        Rename component instance.
        """
        self.logger.debug('Graph {}: Renaming node {} to {}'.format(
            graph_id, orig_node_id, new_node_id))

        graph = self.get_graph(graph_id)
        graph.rename_component(orig_node_id, new_node_id)

    def _set_node_metadata(self, graph_id, node_id, metadata=None):
        metadata = metadata or {}
        graph = self.get_graph(graph_id)
        component = graph.component(node_id)
        for key, value in metadata.items():
            if value is None:
                metadata.pop(key)
                component.metadata.pop(key, None)
        component.metadata.update(metadata)
        return component.metadata

    def _add_edge(self, graph_id, src, tgt, metadata=None):
        """
        Connect ports between components.
        """
        self.logger.debug('Graph {}: Connecting ports: {} -> {}'.format(
            graph_id, src, tgt))

        metadata = metadata or {}

        graph = self.get_graph(graph_id)
        outport = self._get_port(graph, src, kind='out')
        inport = self._get_port(graph, tgt, kind='in')
        graph.connect(outport, inport)

        edge_metadata = inport._connection.metadata.setdefault(outport, {})
        metadata.setdefault('route', FBP_TYPES[inport.type.get_spec()['type']]['color_id'])
        edge_metadata.update(metadata)

        return edge_metadata

    def _remove_edge(self, graph_id, src, tgt):
        """
        Disconnect ports between components.
        """
        self.logger.debug('Graph {}: Disconnecting ports: {} -> {}'.format(
            graph_id, src, tgt))

        graph = self.get_graph(graph_id)
        graph.disconnect(self._get_port(graph, src, kind='out'),
                         self._get_port(graph, tgt, kind='in'))

    def _set_edge_metadata(self, graph_id, src, tgt, metadata=None):
        graph = self.get_graph(graph_id)
        outport = self._get_port(graph, src, kind='out')
        inport = self._get_port(graph, tgt, kind='in')
        edge_metadata = inport._connection.metadata.setdefault(outport, {})

        metadata = metadata or {}
        for key, value in metadata.items():
            if value is None:
                metadata.pop(key)
                edge_metadata.pop(key, None)
        edge_metadata.update(metadata)
        return edge_metadata

    def _initialize_port(self, graph_id, tgt, data):
        """
        Set the inital packet for a component inport.
        """
        self.logger.info('Graph {}: Setting IIP to {!r} on port {}'.format(
            graph_id, data, tgt))

        # FIXME: noflo-ui is sending an 'addinitial foo.IN []' even when
        # the inport is connected
        if data == []:
            return

        graph = self.get_graph(graph_id)

        target_port = self._get_port(graph, tgt, kind='in')
        # if target_port.is_connected():
        #     graph.disconnect(target_port)

        # FIXME: handle deserialization?
        graph.initialize(data, target_port)

    def _uninitialize_port(self, graph_id, tgt):
        """
        Remove the initial packet for a component inport.
        """
        self.logger.debug('Graph {}: Removing IIP from port {}'.format(
            graph_id, tgt))

        graph = self.get_graph(graph_id)

        target_port = self._get_port(graph, tgt, kind='in')
        if target_port.is_initialized():
            # FIXME: so far the case where an uninitialized port receives a uninitialize_port
            # message is when noflo initializes the inport to [] (see initialize_port as well)
            return graph.uninitialize(target_port)._content

    def _add_inport(self, graph_id, node, port, public, metadata=None):
        """
        Add inport to graph
        """
        graph = self.get_graph(graph_id)
        graph.export("{}.{}".format(node, port), public, metadata or {})

    def _remove_inport(self, graph_id, public):
        """
        Remove inport from graph
        """
        graph = self.get_graph(graph_id)
        graph.remove_inport(public)

    def _set_inport_metadata(self, graph_id, public, metadata):
        """
        Change inport metadata
        """
        graph = self.get_graph(graph_id)
        graph.inport_metadata[public] = metadata

    def _add_outport(self, graph_id, node, port, public, metadata=None):
        """
        Add  outport to graph
        """
        graph = self.get_graph(graph_id)
        graph.export("{}.{}".format(node, port), public, metadata or {})

    def _remove_outport(self, graph_id, public):
        """
        Remove outport from graph
        """
        graph = self.get_graph(graph_id)
        graph.remove_outport(public)

    def _set_outport_metadata(self, graph_id, public, metadata):
        """
        Change inport metadata
        """
        graph = self.get_graph(graph_id)
        graph.outport_metadata[public] = metadata


