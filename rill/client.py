from websocket import create_connection
import json

class RillRuntimeClient(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.ws = None

    def connect(self):
        self.ws = create_connection('ws://{}:{}/'.format(self.host, self.port))

    def disconnect(self):
        self.ws.close()
        self.ws = None

    def send(self, protocol, command, payload):
        if not self.ws:
            raise 'Go fuck yourself'

        self.ws.send(json.dumps({
            'protocol': protocol,
            'command': command,
            'payload': payload
        }))
        self.ws.recv()

    def send_graph(self, command, payload):
        self.send('graph', command, payload)

    def new_graph(self, graph_id):
        """
        Create a new graph.
        """
        self.send_graph('clear', {
            'id': graph_id
        })

    def add_node(self, graph_id, node_id, component_id, metadata={}):
        """
        Add a component instance.
        """
        self.send_graph('addnode', {
            'graph': graph_id,
            'id': node_id,
            'component': component_id,
            'metadata': metadata
        })

    def remove_node(self, graph_id, node_id):
        """
        Destroy component instance.
        """
        self.send_graph('removenode', {
            'graph': graph_id,
            'id': node_id
        })

    def rename_node(self, graph_id, orig_node_id, new_node_id):
        """
        Rename component instance.
        """
        self.send_graph('renamenode', {
            'graph': graph_id,
            'from': orig_node_id,
            'to': new_node_id
        })

    def set_node_metadata(self, graph_id, node_id, metadata):
        """
        Sends changenode event
        """
        self.send_graph('changenode', {
            'graph': graph_id,
            'id': node_id,
            'metadata': metadata
        })

    def add_edge(self, graph_id, src, tgt, metadata={}):
        """
        Connect ports between components.
        """
        self.send_graph('addedge', {
            'graph': graph_id,
            'src': src,
            'tgt': tgt,
            'metadata': metadata
        })

    def remove_edge(self, graph_id, src, tgt):
        """
        Disconnect ports between components.
        """
        self.send_graph('removeedge', {
            'graph': graph_id,
            'src': src,
            'tgt': tgt
        })

    def set_edge_metadata(self, graph_id, src, tgt, metadata):
        """
        Send changeedge event'
        """
        self.send_graph('changeedge', {
            'graph': graph_id,
            'src': src,
            'tgt': tgt,
            'metadata': metadata
        })

    def initialize_port(self, graph_id, tgt, data):
        """
        Set the inital packet for a component inport.
        """
        self.send_graph('addinitial', {
            'graph': graph_id,
            'src': {'data': data},
            'tgt': tgt
        })

    def uninitialize_port(self, graph_id, tgt):
        """
        Remove the initial packet for a component inport.
        """
        self.send_graph('removeinitial', {
            'graph': graph_id,
            'tgt': tgt
        })

    def add_inport(self, graph_id, node, port, public, metadata={}):
        """
        Add inport to graph
        """
        self.send_graph('addinport', {
            'graph': graph_id,
            'node': node,
            'port': port,
            'public': public,
            'metadata': metadata
        })

    def add_outport(self, graph_id, node, port, public, metadata={}):
        """
        Add outport to graph
        """
        self.send_graph('addoutport', {
            'graph': graph_id,
            'node': node,
            'port': port,
            'public': public,
            'metadata': metadata
        })

    def remove_inport(self, graph_id, public):
        """
        Remove inport from graph
        """
        self.send_graph('removeinport', {
            'graph': graph_id,
            'public': public
        })

    def remove_outport(self, graph_id, public):
        """
        Remove outport from graph
        """
        self.send_graph('removeoutport', {
            'graph': graph_id,
            'public': public
        })

