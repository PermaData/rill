from rill.events.listeners.base import GraphHandler
from typing import List


class InMemoryGraphHandler(GraphHandler):
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
        super(InMemoryGraphHandler, self).__init__(dispatchers)
        self.graph = graph
        self.add_listeners()

    def add_listeners(self):
        self.graph.remove_component.event.listen(self.remove_component)
        self.graph.rename_component.event.listen(self.rename_component)
        self.graph.put_component.event.listen(self.put_component)
        self.graph.export.event.listen(self.export)
        self.graph.remove_inport.event.listen(self.remove_inport)
        self.graph.remove_outport.event.listen(self.remove_outport)
        self.graph.connect.event.listen(self.connect)
        self.graph.disconnect.event.listen(self.disconnect)
        self.graph.initialize.event.listen(self.initialize)
        self.graph.uninitialize.event.listen(self.uninitialize)

    def remove_listeners(self):
        # TODO
        pass

    def remove_component(self, name):
        self.handle('removenode', {})

    def rename_component(self, orig_name, new_name, component):
        self.handle('renamenode', {})

    def put_component(self, name, comp):
        self.handle('addnode', {})

    def export(self, internal_port, external_port_name, metadata=None):
        self.handle('renamenode', {})

    def remove_inport(self, external_port_name):
        self.handle('removeinport', {})

    def remove_outport(self, external_port_name):
        self.handle('removeoutport', {})

    def connect(self, outport, inport, connection_capacity):
        self.handle('addedge', {})

    def disconnect(self, outport, inport):
        self.handle('removeedge', {})

    def initialize(self, receiver, content):
        self.handle('addinitial', {})

    def uninitialize(self, receiver):
        self.handle('removeinitial', {})
