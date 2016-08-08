import json
from websocket import create_connection

from rill.events.dispatchers.base import GraphDispatcher


class SocketGraphDispatcher(GraphDispatcher):
    """
    Propagate updates to a socket-like object.
    """
    def __init__(self, socket):
        self.socket = socket
        socket.subscriptions.append(self)

    def send(self, protocol, command, payload):
        if not self.socket:
            raise RuntimeError('No socket to send on')

        self.socket.send(json.dumps({
            'protocol': protocol,
            'command': command,
            'payload': payload
        }))

    def send_graph(self, command, payload):
        self.send('graph', command, payload)

    # -- overrides

    def new_graph(self, payload):
        """
        Create a new graph.
        """
        self.send('graph', 'clear', payload)

    def add_node(self, payload):
        """
        Add a component instance.
        """
        self.send('graph', 'addnode', payload)

    def remove_node(self, payload):
        """
        Destroy component instance.
        """
        self.send('graph', 'removenode', payload)

    def rename_node(self, payload):
        """
        Rename component instance.
        """
        self.send('graph', 'renamenode', payload)

    def set_node_metadata(self, payload):
        """
        Sends changenode event
        """
        self.send('graph', 'changenode', payload)

    def add_edge(self, payload):
        """
        Connect ports between components.
        """
        self.send('graph', 'addedge', payload)

    def remove_edge(self, payload):
        """
        Disconnect ports between components.
        """
        self.send('graph', 'removeedge', payload)

    def set_edge_metadata(self, payload):
        """
        Send changeedge event'
        """
        self.send('graph', 'changeedge', payload)

    def initialize_port(self, payload):
        """
        Set the inital packet for a component inport.
        """
        self.send('graph', 'addinitial', payload)

    def uninitialize_port(self, payload):
        """
        Remove the initial packet for a component inport.
        """
        self.send('graph', 'removeinitial', payload)

    def add_inport(self, payload):
        """
        Add inport to graph
        """
        self.send('graph', 'addinport', payload)

    def remove_inport(self, payload):
        """
        Remove inport from graph
        """
        self.send('graph', 'removeinport', payload)

    def set_inport_metadata(self, payload):
        """
        Send the metadata on an exported inport
        """
        self.send('graph', 'removeinport', payload)

    def add_outport(self, payload):
        """
        Add outport to graph
        """
        self.send('graph', 'addoutport', payload)

    def remove_outport(self, payload):
        """
        Remove outport from graph
        """
        self.send('graph', 'removeoutport', payload)

    def set_outport_metadata(self, payload):
        """
        Send the metadata on an exported outport
        """
        self.send('graph', 'removeoutport', payload)

    # def new_graph(self, graph_id):
    #     """
    #     Create a new graph.
    #     """
    #     self.send('graph', 'clear', {
    #         'id': graph_id
    #     })
    # def add_node(self, graph_id, node_id, component_id, metadata=None):
    #     """
    #     Add a component instance.
    #     """
    #     self.send('graph', 'addnode', {
    #         'graph': graph_id,
    #         'id': node_id,
    #         'component': component_id,
    #         'metadata': metadata or {}
    #     })
    #
    # def remove_node(self, graph_id, node_id):
    #     """
    #     Destroy component instance.
    #     """
    #     self.send('graph', 'removenode', {
    #         'graph': graph_id,
    #         'id': node_id
    #     })
    #
    # def rename_node(self, graph_id, orig_node_id, new_node_id):
    #     """
    #     Rename component instance.
    #     """
    #     self.send('graph', 'renamenode', {
    #         'graph': graph_id,
    #         'from': orig_node_id,
    #         'to': new_node_id
    #     })
    #
    # def set_node_metadata(self, graph_id, node_id, metadata=None):
    #     """
    #     Sends changenode event
    #     """
    #     self.send('graph', 'changenode', {
    #         'graph': graph_id,
    #         'id': node_id,
    #         'metadata': metadata or {}
    #     })
    #
    # def add_edge(self, graph_id, src, tgt, metadata=None):
    #     """
    #     Connect ports between components.
    #     """
    #     self.send('graph', 'addedge', {
    #         'graph': graph_id,
    #         'src': src,
    #         'tgt': tgt,
    #         'metadata': metadata or {}
    #     })
    #
    # def remove_edge(self, graph_id, src, tgt):
    #     """
    #     Disconnect ports between components.
    #     """
    #     self.send('graph', 'removeedge', {
    #         'graph': graph_id,
    #         'src': src,
    #         'tgt': tgt
    #     })
    #
    # def set_edge_metadata(self, graph_id, src, tgt, metadata=None):
    #     """
    #     Send changeedge event'
    #     """
    #     self.send('graph', 'changeedge', {
    #         'graph': graph_id,
    #         'src': src,
    #         'tgt': tgt,
    #         'metadata': metadata or {}
    #     })
    #
    # def initialize_port(self, graph_id, tgt, data):
    #     """
    #     Set the inital packet for a component inport.
    #     """
    #     self.send('graph', 'addinitial', {
    #         'graph': graph_id,
    #         'src': {'data': data},
    #         'tgt': tgt
    #     })
    #
    # def uninitialize_port(self, graph_id, tgt):
    #     """
    #     Remove the initial packet for a component inport.
    #     """
    #     self.send('graph', 'removeinitial', {
    #         'graph': graph_id,
    #         'tgt': tgt
    #     })
    #
    # def add_inport(self, graph_id, node, port, public, metadata=None):
    #     """
    #     Add inport to graph
    #     """
    #     self.send('graph', 'addinport', {
    #         'graph': graph_id,
    #         'node': node,
    #         'port': port,
    #         'public': public,
    #         'metadata': metadata or {}
    #     })
    #
    # def remove_inport(self, graph_id, public):
    #     """
    #     Remove inport from graph
    #     """
    #     self.send('graph', 'removeinport', {
    #         'graph': graph_id,
    #         'public': public
    #     })
    #
    # def add_outport(self, graph_id, node, port, public, metadata=None):
    #     """
    #     Add outport to graph
    #     """
    #     self.send('graph', 'addoutport', {
    #         'graph': graph_id,
    #         'node': node,
    #         'port': port,
    #         'public': public,
    #         'metadata': metadata or {}
    #     })
    #
    # def remove_outport(self, graph_id, public):
    #     """
    #     Remove outport from graph
    #     """
    #     self.send('graph', 'removeoutport', {
    #         'graph': graph_id,
    #         'public': public
    #     })
    #


class WebSocketClientGraphDispatcher(SocketGraphDispatcher):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.socket = None

    def connect(self):
        self.socket = create_connection('ws://{}:{}/'.format(self.host, self.port))

    def disconnect(self):
        self.socket.close()
        self.socket = None
