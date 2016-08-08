from rill.events.listeners.base import GraphListener
from rill.engine.types import serialize
from rill.engine.inputport import InitializationConnection, InputPort

from typing import List


def fbp_component(component, graph_id=None):
    node = {
        'graph': graph_id,
        'id': component.get_name(),
        "component": component.get_type(),
        "metadata": component.metadata,
    }
    if graph_id is not None:
        node['graph'] = graph_id
    return node


def fbp_port(port, node_label='node'):
    """
    Get a Flowbase Protocol compoatible port definition

    Parameters
    ----------
    port: BasePort

    Returns
    -------
    dict
    """
    doc = {
        node_label: port.component.get_name(),
        # FIXME: it should be easier to get this value from BasePort
        'port': port._name if port.is_element() else port.name
    }
    if port.is_element():
        doc['index'] = port.index
    return doc


def fbp_edge(outport, inport, graph_id, node_label='node'):
    connection = {
        'src': fbp_port(outport, node_label),
        'tgt': fbp_port(inport, node_label)
    }
    conn = inport._connection
    # keyed on outport
    metadata = conn.metadata.get(outport, None)
    if metadata:
        connection['metadata'] = metadata
    if graph_id is not None:
        connection['graph'] = graph_id
    return connection


def fbp_iip(inport, graph_id=None, node_label='node'):
    conn = inport._connection
    content = conn._content
    # FIXME: we need to determine the most reliable way to
    # serialize `content`.
    # - inport.type is only good when type is not "any".
    # - serialize() must store extra info about rich types,
    #   which could confuse a client
    # - another option is to store the type on the Packet
    # content = inport.type.to_primitive(content)
    if inport.auto_receive:
        content = content[0]
    connection = {
        'src': {'data': serialize(content)},
        'tgt': fbp_port(inport, node_label)
    }
    if conn.metadata:
        connection['metadata'] = conn.metadata
    if graph_id is not None:
        connection['graph'] = graph_id
    return connection


def get_graph_messages_old(graph, graph_id):
    """
    Create graph protocol messages to build graph for receiver

    Params
    ------
    graph : ``rill.engine.network.Graph``
    graph_id : str
        id of graph to serialize

    Returns
    -------
    Iterator[Dict[str, Any]]
        graph protocol messages that reproduce graph
    """
    # FIXME: reverse this: use get_graph_messages to produce Graph.to_dict().
    # for large graphs, this function could benefit from yielding messages, but
    # most of the work is currently done in Graph.to_dict() in a blocking way
    definition = graph.to_dict()

    yield ('clear', {
        'id': graph_id,
        'name': graph.name
    })

    for node_id, node in definition['processes'].items():
        payload = {
            'graph': graph_id,
            'id': node_id
        }
        payload.update(node)
        yield ('addnode', payload)
    for edge in definition['connections']:
        payload = {
            'graph': graph_id
        }
        if 'data' in edge['src']:
            command = 'addinitial'
        else:
            command = 'addedge'

        payload.update(edge)

        yield (command, payload)

    for public_port, inner_port in definition['inports'].items():
        yield ('addinport', {
            'graph': graph_id,
            'public': public_port,
            'node': inner_port['process'],
            'port': inner_port['port'],
            'metadata': inner_port['metadata']
        })

    for public_port, inner_port in definition['outports'].items():
        yield ('addoutport', {
            'graph': graph_id,
            'public': public_port,
            'node': inner_port['process'],
            'port': inner_port['port'],
            'metadata': inner_port['metadata']
        })


def get_graph_messages(graph, graph_id):
    """
    Create graph protocol messages to build graph for receiver

    Params
    ------
    graph : ``rill.engine.network.Graph``
    graph_id : str
        id of graph to serialize

    Returns
    -------
    Iterator[Dict[str, Any]]
        graph protocol messages that reproduce graph
    """

    yield ('clear', {
        'id': graph_id,
        'name': graph.name
    })

    connections = []
    for (comp_name, component) in graph.get_components().items():
        if component.hidden:
            continue

        yield ('addnode', fbp_component(component, graph_id))

        for inport in component.inports:
            if not inport.is_connected():
                continue

            conn = inport._connection
            if isinstance(conn, InitializationConnection):
                connections.append(('addinitial',
                                    fbp_iip(inport, graph_id)))
            else:
                for outport in conn.outports:
                    if outport.component.hidden:
                        continue
                    connections.append(('addedge',
                                        fbp_edge(outport, inport, graph_id)))

    for connection in connections:
        yield connection

    for public_port, inport in graph.inports.items():
        yield ('addinport', {
            'graph': graph_id,
            'public': public_port,
            'node': inport.component.get_name(),
            'port': inport.name,
            'metadata': graph.inport_metadata.get(public_port, {})
        })

    for public_port, outport in graph.outports.items():
        yield ('addoutport', {
            'graph': graph_id,
            'public': public_port,
            'node': outport.component.get_name(),
            'port': outport.name,
            'metadata': graph.outport_metadata.get(public_port, {})
        })


class InMemoryNetworkListner():
    pass


class InMemoryGraphListener(GraphListener):
    """
    Listen for changes to a Graph and send them as FBP messages to one or
    more dispatchers
    """
    def __init__(self, graph, dispatchers):
        """
        Parameters
        ----------
        graph : rill.engine.network.Graph
        dispatchers : List[rill.events.dispatchers.GraphDispatcher]
        """
        super(InMemoryGraphListener, self).__init__(dispatchers)
        assert graph.name is not None
        self.graph = graph
        self.add_listeners()

    def add_listeners(self):
        self.graph.set_metadata.event.listen(self.set_graph_metadata)
        self.graph.rename.event.listen(self.rename_graph)
        self.graph.remove_component.event.listen(self.remove_component)
        self.graph.rename_component.event.listen(self.rename_component)
        self.graph.put_component.event.listen(self.put_component)
        self.graph.export.event.listen(self.export)
        self.graph.remove_inport.event.listen(self.remove_inport)
        self.graph.remove_outport.event.listen(self.remove_outport)
        self.graph.set_inport_metadata.event.listen(self.set_inport_metadata)
        self.graph.set_outport_metadata.event.listen(self.set_outport_metadata)
        self.graph.connect.event.listen(self.connect)
        self.graph.disconnect.event.listen(self.disconnect)
        self.graph.set_node_metadata.event.listen(self.set_node_metadata)
        self.graph.set_edge_metadata.event.listen(self.set_edge_metadata)
        self.graph.initialize.event.listen(self.initialize)
        self.graph.uninitialize.event.listen(self.uninitialize)

    def remove_listeners(self):
        # TODO
        pass

    def snapshot(self):
        """
        Send messages to dispatchers providing the current state of the Graph.

        This should be called after subscribing to an existing graph.
        """
        for command, payload in get_graph_messages(self.graph, self.graph.name):
            self.handle(command, payload)

    def set_graph_metadata(self, metadata):
        self.handle('changegraph', {
            'graph': self.graph.name,
            'metadata': metadata
        })

    def rename_graph(self, from_id, to_id):
        self.handle('renamegraph', {
            'from': from_id,
            'to': to_id
        })

    def remove_component(self, component):
        self.handle('removenode', {
            'graph': self.graph.name,
            'id': component.get_name(),
        })

    def rename_component(self, orig_name, new_name, component):
        self.handle('renamenode', {
            'graph': self.graph.name,
            'from': orig_name,
            'to': new_name,
        })

    def put_component(self, name, comp):
        self.handle('addnode', fbp_component(comp, self.graph.name))

    def set_node_metadata(self, component, metadata):
        self.handle('changenode', {
            'graph': self.graph.name,
            'id': component.get_name(),
            'metadata': metadata
        })

    def export(self, internal_port, external_port_name, metadata=None):
        if isinstance(internal_port, InputPort):
            command = 'addinport'
        else:
            command = 'addoutport'

        self.handle(command, {
            'graph': self.graph.name,
            'node': internal_port.component.get_name(),
            'port': internal_port.name,
            'public': external_port_name,
            'metadata': metadata
        })

    def remove_inport(self, external_port_name):
        self.handle('removeinport', {
            'graph': self.graph.name,
            'public': external_port_name
        })

    def set_inport_metadata(self, external_port_name, metadata):
        self.handle('changeinport', {
            'graph': self.graph.name,
            'public': external_port_name,
            'metadata': metadata
        })

    def remove_outport(self, external_port_name):
        self.handle('removeoutport', {
            'graph': self.graph.name,
            'public': external_port_name
        })

    def set_outport_metadata(self, external_port_name, metadata):
        self.handle('changeoutport', {
            'graph': self.graph.name,
            'public': external_port_name,
            'metadata': metadata
        })

    def connect(self, outport, inport, connection_capacity=None, metadata=None):
        self.handle('addedge', fbp_edge(outport, inport, self.graph.name))

    def disconnect(self, outport, inport):
        self.handle('removeedge', {
            'graph': self.graph.name,
            'src': fbp_port(outport),
            'tgt': fbp_port(inport)
        })

    def set_edge_metadata(self, outport, inport, metadata):
        self.handle('changeedge', {
            'graph': self.graph.name,
            'src': fbp_port(outport),
            'tgt': fbp_port(inport),
            'metadata': metadata
        })

    def initialize(self, inport, content):
        self.handle('addinitial', fbp_iip(inport, self.graph.name))

    def uninitialize(self, inport):
        self.handle('removeinitial', {
            'graph': self.graph.name,
            'tgt': fbp_port(inport)
        })
