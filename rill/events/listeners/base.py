from rill.engine.exceptions import FlowError

# FIXME: this is still a work in progress. need to determine how much

class GraphListener(object):
    """
    Simple passtrhough base class, mapping FBP graph sub-commands to
    dispatcher methods.
    """
    GRAPH_COMMAND_TO_METHOD = {
        'clear': 'new_graph',
        'changegraph': 'set_graph_metadata',
        'renamegraph': 'rename_graph',
        # Nodes
        'addnode': 'add_node',
        'removenode': 'remove_node',
        'renamenode': 'rename_node',
        'changenode': 'set_node_metadata',
        # Edges/connections
        'addedge': 'add_edge',
        'removeedge': 'remove_edge',
        'changeedge': 'set_edge_metadata',
        # IIP / literals
        'addinitial': 'initialize_port',
        'removeinitial': 'uninitialize_port',
        # Exported ports
        'addinport': 'add_inport',
        'addoutport': 'add_outport',
        'removeinport': 'remove_inport',
        'removeoutport': 'remove_outport',
        'changeinport': 'set_inport_metadata',
        'changeoutport': 'set_outport_metadata',
    }

    def __init__(self, dispatchers):
        self.dispatchers = dispatchers

    def handle(self, command, payload):
        """
        Deliver a FBP graph message to dispatchers

        Parameters
        ----------
        command : str
        payload : dict
        """
        # FIXME: add (optional?) jsonschema validation of payload
        try:
            method_name = self.GRAPH_COMMAND_TO_METHOD[command]
        except KeyError:
            raise FlowError("Unknown command '%s' for protocol '%s'" %
                            (command, 'graph'))
        for dispatcher in self.dispatchers:
            method = getattr(dispatcher, method_name)
            method(payload)
