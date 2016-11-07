import os
import pydoc
import logging
import json
from collections import OrderedDict
import inspect
import functools
import traceback
import datetime
import weakref
from functools import wraps
import uuid

import gevent
import geventwebsocket

from rill.engine.component import Component
from rill.engine.inputport import Connection
from rill.engine.outputport import OutputPort
from rill.engine.network import Graph, Network
from rill.engine.subnet import SubGraph, make_subgraph
from rill.engine.types import FBP_TYPES, Stream
from rill.engine.exceptions import FlowError
from rill.compat import *

from typing import Union, Any, Iterator, Dict


logger = logging.getLogger(__name__)
_initialized = False

# here as a constant so they can be used in both serve_runtime and the CLI
DEFAULTS = {
    'host': 'localhost',
    'port': 3569,
    'registry_host': 'localhost',
    'registry_port': 8080
}


class RillRuntimeError(FlowError):
    pass


def short_class_name(klass):
    return klass.__name__


def add_callback(f, callback):
    """
    Wrap the callable ``f`` to first execute callable ``callback``
    """
    if not hasattr(f, '_callbacks'):
        # undecorated function: decorate it
        callbacks = {}#weakref.WeakValueDictionary()
        callbacks[0] = callback
        @wraps(f)
        def wrapper(*args, **kwargs):
            for k in sorted(callbacks.keys()):
                cb = callbacks[k]
                cb(*args, **kwargs)
            return f(*args, **kwargs)
        wrapper._callbacks = callbacks
        return wrapper
    else:
        # already decorated function: add the callback function
        keys = f._callbacks.keys()
        key = max(keys) if keys else 0
        f._callbacks[key] = callback
        return f


def expandpath(p):
    return os.path.abspath(os.path.expanduser(os.path.expandvars(p)))


def _itermodules():
    import imp
    import sys
    suffixes = tuple([x[0] for x in imp.get_suffixes()])
    # # locate all modules that look like `rill_cmds.<mypackage>`
    # try:
    #     mod = __import__(COMMAND_MODULE, None, None)
    # except ImportError as err:
    #     if err.message != 'No module named {}'.format(COMMAND_MODULE):
    #         raise
    # else:
    #     for path in mod.__path__:
    #         for filename in os.listdir(expandpath(path)):
    #             if filename.endswith(suffixes) \
    #                     and not filename.startswith('_'):
    #                 name = os.path.splitext(filename)[0]
    #                 name = name.encode('ascii', 'replace')
    #                 modname = COMMAND_MODULE + '.' + name
    #                 yield modname

    # locate all modules that look like `rill_<mypackage>`
    for path in sys.path:
        path = expandpath(path)
        try:
            for filename in os.listdir(path):
                if filename.endswith(suffixes) \
                        and filename.startswith('rill_'):
                    name = os.path.splitext(filename)[0]
                    name = name.encode('ascii', 'replace')
                    yield name
        except OSError:
            pass

    modnames = os.environ.get('RILL_MODULES', None)
    if modnames:
        for modname in modnames.split(os.path.pathsep):
            yield modname


def _import_component_modules():
    """
    Import component collection modules.
    Modules are discovered in the following ways:
    - Modules named `rill_cmds.<mycollection>`
    - Modules named `rill_<mycollection>`
    - Modules listed in `RILL_MODULES` environment variable
    Additional, you can specify `RILL_PYTHONPATH` environment variable to
    temporariliy extend the python search path (`sys.path`) during the
    search for the above modules.
    """
    import imp
    import sys
    suffixes = tuple([x[0] for x in imp.get_suffixes()])
    # import pydevd; pydevd.settrace();

    names = set()

    def _uniquify(name):
        # uniqify the name
        orig = name
        suffix = 1
        while name in names:
            name = orig + str(suffix)
            suffix += 1
        return name

    paths = os.environ.get('RILL_PYTHONPATH', None)
    # separate files from folders on the path:
    files = []
    dirs = []
    if paths:
        paths = paths.split(':')
        for p in paths:
            if p.endswith(suffixes):
                path, name = os.path.split(os.path.splitext(p)[0])
                name = _uniquify(name)
                names.add(name)
                files.append((name, p))
            else:
                dirs.append(p)

        if dirs:
            # put folders on sys.path so that component modules within them are
            # found by __import__ below
            orig_sys_path = list(sys.path)
            sys.path.extend(dirs)

    try:
        for modname in _itermodules():
            try:
                # logger.debug('Importing %s' % modname)
                __import__(modname, None, None, [])
            except ImportError:
                import traceback
                traceback.print_exc()

        # load files that were explicitly added to RILL_PATH
        for name, filename in files:
            imp.load_source(name, filename)

    finally:
        if dirs:
            # restore
            sys.path = orig_sys_path


def init():
    """
    Initialize rill.
    """
    global _initialized
    if not _initialized:
        _import_component_modules()
    _initialized = True


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
    definition = graph.to_dict()

    yield ('clear', {
        'id': graph_id,
        'name': graph.name,
        'description': graph.description,
        'metadata': graph.metadata
    })

    def port_msg(p):
        p['node'] = p.pop('process')

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
        port_msg(edge['tgt'])
        if 'data' in edge['src']:
            command = 'addinitial'
        else:
            port_msg(edge['src'])
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


class Runtime(object):
    """
    Rill runtime for python
    A ``Runtime`` instance holds many ``rill.engine.network.Graph`` instances
    each running on their own greelent.
    """
    PROTOCOL_VERSION = '0.5'

    def __init__(self):
        self.logger = logging.getLogger('%s.%s' % (self.__class__.__module__,
                                                   self.__class__.__name__))

        self._component_types = {}  # Component metadata, keyed by component name
        # type: Dict[str, Graph]
        self._graphs = {}  # Graph instances, keyed by graph ID
        # type: Dict[str, Tuple[Greenlet, Network]]
        self._executors = {}  # GraphExecutor instances, keyed by graph ID

        self.logger.debug('Initialized runtime!')

    def get_runtime_meta(self):
        # Supported protocol capabilities
        capabilities = [
            # expose the ports of its main graph using the Runtime protocol
            # and transmit packet information to/from them
            'protocol:runtime',

            # modify its graphs using the Graph protocol
            'protocol:graph',

            # list and modify its components using the Component protocol
            'protocol:component',

            # control and introspect its running networks using the Network
            # protocol
            'protocol:network',

            # compile and run custom components sent as source code strings
            #'component:setsource',

            # read and send component source code back to client
            'component:getsource',

            # "flash" a running graph setup into itself, making it persistent
            # across reboots
            'network:persist',

            # build graph on ui using graph protocol messages
            'graph:getgraph'
        ]

        all_capabilities = capabilities

        return {
            # FIXME: there's a bug in fbp-protocol
            'label': 'rill python runtime',
            'type': 'rill',
            'version': self.PROTOCOL_VERSION,
            'capabilities': capabilities,
            'allCapabilities': all_capabilities
        }

    # Components --

    def get_subnet_component(self, graph_id):
        graph = self.get_graph(graph_id)
        Sub = make_subgraph(str(graph_id), graph)
        return Sub

    def register_subnet(self, graph_id):
        subnet = self.get_subnet_component(graph_id)
        self.register_component(subnet, True)
        return subnet.get_spec()

    def get_all_component_specs(self):
        """
        Returns
        -------
        List[Dict[str, Any]]
        """
        return [data['spec'] for data in self._component_types.values()]

    def register_component(self, component_class, overwrite=False):
        """
        Register a component class.
        Parameters
        ----------
        component_class : Type[``rill.enginge.component.Component``]
            the Component class to register.
        overwrite : bool
            whether the component be overwritten if it already exists.
            if False and the component already exists, a ValueError will be
            raised
        """
        if not issubclass(component_class, Component):
            raise ValueError('component_class must be a class that inherits '
                             'from Component')

        spec = component_class.get_spec()
        name = spec['name']

        if name in self._component_types and not overwrite:
            raise ValueError("Component {0} already registered".format(name))

        self.logger.debug('Registering component: {0}'.format(name))

        self._component_types[name] = {
            'class': component_class,
            'spec': spec
        }

    def register_module(self, module, overwrite=False):
        """
        Register all component classes within a module.
        Parameters
        ----------
        module : Union[str, `ModuleType`]
        overwrite : bool
        """
        if isinstance(module, basestring):
            module = pydoc.locate(module)

        if not inspect.ismodule(module):
            raise ValueError('module must be either a module or the name of a '
                             'module')

        self.logger.info('Registering components in module: {}'.format(
            module.__name__))

        registered = 0
        for obj_name, class_obj in inspect.getmembers(module):
            if (inspect.isclass(class_obj) and
                    class_obj is not Component and
                    not inspect.isabstract(class_obj) and
                    not issubclass(class_obj, SubGraph) and
                    issubclass(class_obj, Component)):
                self.register_component(class_obj, overwrite)
                registered += 1

        if registered == 0:
            self.logger.warn('No components were found in module: {}'.format(
                module.__name__))

    def get_source_code(self, component_name):
        # FIXME:
        component = None
        for graph in self._graphs.values():
            component = self._find_component_by_name(graph, component_name)
            if component is not None:
                break

        if component is None:
            raise ValueError('No component named {}'.format(component_name))

        return inspect.getsource(component.__class__)

    # Network --

    def get_status(self, graph_id):
        if graph_id not in self._graphs:
            self.new_graph(graph_id)
        started = graph_id in self._executors
        running = started and not self._executors[graph_id][0].ready()
        print("get_status.  started {}, running {}".format(started, running))
        return started, running

    def start(self, graph_id, done_callback):
        """
        Execute a graph.
        """
        self.logger.debug('Graph {}: Starting execution'.format(graph_id))

        graph = self.get_graph(graph_id)

        network = Network(graph)
        executor = gevent.Greenlet(network.go)
        # FIXME: should we delete the executor from self._executors on finish?
        # this has an impact on the result returned from get_status().  Leaving
        # it means that after completion it will be started:True, running:False
        # until stop() is triggered, at which point it will be started:False,
        # running:False
        executor.link(lambda g: done_callback())
        self._executors[graph_id] = (executor, network)
        executor.start()
        # if executor.is_running():
        #     raise ValueError('Graph {} is already started'.format(graph_id))

    def stop(self, graph_id):
        """
        Stop executing a graph.
        """
        self.logger.debug('Graph {}: Stopping execution'.format(graph_id))
        if graph_id not in self._executors:
            raise ValueError('Invalid graph: {}'.format(graph_id))

        executor, network = self._executors[graph_id]
        network.terminate()
        executor.join()
        del self._executors[graph_id]

    def set_debug(self, graph_id, debug):
        # FIXME: noflo-ui sends the network:debug command before creating a
        # graph
        # self.get_graph(graph_id).debug = debug
        pass

    # Graphs --

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
            raise RillRuntimeError('Requested graph not found')

    def new_graph(self, graph_id, description=None, metadata=None):
        """
        Create a new graph.
        """
        self.logger.debug('Graph {}: Initializing'.format(graph_id))
        self.add_graph(graph_id, Graph(
            name=graph_id,
            description=description,
            metadata=metadata
        ))

    def add_graph(self, graph_id, graph):
        """
        Parameters
        ----------
        graph_id : str
        graph : ``rill.engine.network.Graph``
        """
        self._graphs[graph_id] = graph

    def add_node(self, graph_id, node_id, component_id, metadata):
        """
        Add a component instance.
        """
        self.logger.debug('Graph {}: Adding node {}({})'.format(
            graph_id, component_id, node_id))

        graph = self.get_graph(graph_id)

        component_class = self._component_types[component_id]['class']
        component = graph.add_component(node_id, component_class)
        component.metadata.update(metadata)

    def remove_node(self, graph_id, node_id):
        """
        Destroy component instance.
        """
        self.logger.debug('Graph {}: Removing node {}'.format(
            graph_id, node_id))

        graph = self.get_graph(graph_id)
        graph.remove_component(node_id)

    def rename_node(self, graph_id, orig_node_id, new_node_id):
        """
        Rename component instance.
        """
        self.logger.debug('Graph {}: Renaming node {} to {}'.format(
            graph_id, orig_node_id, new_node_id))

        graph = self.get_graph(graph_id)
        graph.rename_component(orig_node_id, new_node_id)

    def set_node_metadata(self, graph_id, node_id, metadata):
        graph = self.get_graph(graph_id)
        component = graph.component(node_id)
        for key, value in metadata.items():
            if value is None:
                metadata.pop(key)
                component.metadata.pop(key, None)
        component.metadata.update(metadata)
        return component.metadata

    def add_edge(self, graph_id, src, tgt, metadata):
        """
        Connect ports between components.
        """
        self.logger.debug('Graph {}: Connecting ports: {} -> {}'.format(
            graph_id, src, tgt))

        graph = self.get_graph(graph_id)
        outport = self._get_port(graph, src, kind='out')
        inport = self._get_port(graph, tgt, kind='in')
        graph.connect(outport, inport)

        edge_metadata = inport._connection.metadata.setdefault(outport, {})
        metadata.setdefault('route', FBP_TYPES[inport.type.get_spec()['type']]['color_id'])
        edge_metadata.update(metadata)

        return edge_metadata

    def remove_edge(self, graph_id, src, tgt):
        """
        Disconnect ports between components.
        """
        self.logger.debug('Graph {}: Disconnecting ports: {} -> {}'.format(
            graph_id, src, tgt))

        graph = self.get_graph(graph_id)
        graph.disconnect(self._get_port(graph, src, kind='out'),
                         self._get_port(graph, tgt, kind='in'))

    def set_edge_metadata(self, graph_id, src, tgt, metadata):
        graph = self.get_graph(graph_id)
        outport = self._get_port(graph, src, kind='out')
        inport = self._get_port(graph, tgt, kind='in')
        edge_metadata = inport._connection.metadata.setdefault(outport, {})

        for key, value in metadata.items():
            if value is None:
                metadata.pop(key)
                edge_metadata.pop(key, None)
        edge_metadata.update(metadata)
        return edge_metadata

    def initialize_port(self, graph_id, tgt, data):
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
        if not target_port.auto_receive:
            data = Stream(data)
        graph.initialize(data, target_port)

    def uninitialize_port(self, graph_id, tgt):
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

    def add_export(self, graph_id, node, port, public, metadata={}):
        """
        Add inport or outport to graph
        """
        graph = self.get_graph(graph_id)
        graph.export("{}.{}".format(node, port), public, metadata)

    def remove_inport(self, graph_id, public):
        """
        Remove inport from graph
        """
        graph = self.get_graph(graph_id)
        graph.remove_inport(public)

    def remove_outport(self, graph_id, public):
        """
        Remove outport from graph
        """
        graph = self.get_graph(graph_id)
        graph.remove_outport(public)

    def change_inport(self, graph_id, public, metadata):
        """
        Change inport metadata
        """
        graph = self.get_graph(graph_id)
        graph.inport_metadata[public] = metadata

    def change_outport(self, graph_id, public, metadata):
        """
        Change inport metadata
        """
        graph = self.get_graph(graph_id)
        graph.outport_metadata[public] = metadata

    def change_graph(self, graph_id, description=None, metadata={}):
        """
        Change graph attributes
        """

        graph = self.get_graph(graph_id)

        if description:
            graph.description = description

        if metadata:
            graph.metadata.update(metadata)

    def rename_graph(self, old_id, new_id):
        """
        Change graph name
        """

        graph = self.get_graph(old_id)
        graph.name = new_id

        del self._graphs[old_id]
        self._graphs[new_id] = graph


clients = {}
class WebSocketRuntimeApplication(geventwebsocket.WebSocketApplication):
    """
    Web socket application that hosts a single ``Runtime`` instance.
    An instance of this class receives messages over a websocket, delegates
    message payloads to the appropriate ``Runtime`` methods, and sends
    responses where applicable.
    Message structures are defined by the FBP Protocol.
    """
    runtimes = {}

    def __init__(self, ws):
        super(WebSocketRuntimeApplication, self).__init__(ws)

        self.logger = logging.getLogger('{}.{}'.format(
            self.__class__.__module__, self.__class__.__name__))
        self.runtime = self.runtimes[int(ws.environ['SERVER_PORT'])]

        # FIXME: move to on_open?
        # insert a listener
        Connection.send = add_callback(Connection.send,
                                       self.send_connection_data)

    # WebSocketApplication overrides --

    @staticmethod
    def protocol_name():
        """
        WebSocket sub-protocol
        """
        return 'noflo'

    def on_open(self):
        self.client_id = uuid.uuid4()
        print('connected: {}'.format(str(self.client_id)))
        clients[self.client_id] = self
        self.logger.info("Connection opened")

    def on_close(self, reason):
        del clients[self.client_id]
        print('disconnected: {}'.format(str(self.client_id)))
        self.client_id = None
        self.logger.info("Connection closed. Reason: {}".format(reason))

    def on_message(self, message, **kwargs):
        self.logger.debug('MESSAGE: {}'.format(message))

        if not message:
            self.logger.warn('Got empty message')
            return

        m = json.loads(message)
        dispatch = {
            'runtime': self.handle_runtime,
            'component': self.handle_component,
            'graph': self.handle_graph,
            'network': self.handle_network
        }
        import pprint
        print("--IN--")
        pprint.pprint(m)

        try:
            protocol = m['protocol']
            command = m['command']
            payload = m['payload']
            message_id = m.get('id', None)
        except KeyError:
            # FIXME: send error?
            self.logger.warn("Malformed message")
            return

        # FIXME: use the json-schema files from FBP protocol to validate
        # message structure
        try:
            handler = dispatch[protocol]
        except KeyError:
            # FIXME: send error?
            self.logger.warn("Subprotocol '{}' "
                             "not supported".format(protocol))
            return

        try:
            handler(command, payload, message_id)
        except RillRuntimeError as err:
            self.send_error(protocol, str(err))

    # Utilities --

    def send(self, protocol, command, payload, message_id=None):
        """
        Send a message to UI/client
        """
        message = {'protocol': protocol,
                   'command': command,
                   'payload': payload,
                   'id': message_id or str(uuid.uuid4())}
        print("--OUT--")
        import pprint
        pprint.pprint(message)
        # FIXME: what do we do when the socket closes or is dead?
        try:
            self.ws.send(json.dumps(message))
        except geventwebsocket.WebSocketError as err:
            print(err)

    def send_error(self, protocol, message):
        data = {
            'message': message,
            'stack': traceback.format_exc()
        }
        self.send(protocol, 'error', data)

    def send_connection_data(self, connection, packet, outport):
        """
        Setup as a callback for ``rill.engine.inputport.Connection.send``
        so that packets sent by this method are intercepted and reported
        to the UI/client.
        """
        inport = connection.inport
        payload = {
            'id': '{} {} -> {} {}'.format(
                outport.component.get_name(),
                outport._name,
                inport._name,
                inport.component.get_name(),
            ),
            'graph': 'main',  # FIXME
            'src': {
                'node': outport.component.get_name(),
                'port': outport._name
            },
            'tgt': {
                'node': inport.component.get_name(),
                'port': inport._name
            },
            'data': packet.get_contents()
        }
        self.send('network', 'data', payload)

    # Protocol send/responses --

    def handle_runtime(self, command, payload, message_id):
        # tell UI info about runtime and supported capabilities
        if command == 'getruntime':
            payload = self.runtime.get_runtime_meta()
            # self.logger.debug(json.dumps(payload, indent=4))
            self.send('runtime', 'runtime', payload)

        # network:packet, allows sending data in/out to networks in this
        # runtime can be used to represent the runtime as a FBP component
        # in bigger system "remote subgraph"
        elif command == 'packet':
            # We don't actually run anything, just echo input back and
            # pretend it came from "out"
            payload['port'] = 'out'
            self.send('runtime', 'packet', payload)

        else:
            self.logger.warn("Unknown command '%s' for protocol '%s' " %
                             (command, 'runtime'))

    def handle_component(self, command, payload, message_id):
        """
        Provide information about components.
        Parameters
        ----------
        command : str
        payload : dict
        """
        if command == 'list':
            for spec in self.runtime.get_all_component_specs():
                self.send('component', 'component', spec)

            self.send('component', 'componentsready', None)
        # Get source code for component
        elif command == 'getsource':
            raise TypeError("HEREREREREHRERERE")
            component_name = payload['name']
            source_code = self.runtime.get_source_code(component_name)

            library_name, short_component_name = component_name.split('/', 1)

            payload = {
                'name': short_component_name,
                'language': 'python',
                'library': library_name,
                'code': source_code,
                #'tests': ''
                'secret': payload.get('secret')
            }
            self.send('component', 'source', payload)
        else:
            self.logger.warn("Unknown command '%s' for protocol '%s' " %
                             (command, 'component'))

    def handle_graph(self, command, payload, message_id):
        """
        Modify our graph representation to match that of the UI/client
        Parameters
        ----------
        command : str
        payload : dict
        """
        # Note: if it is possible for the graph state to be changed by
        # other things than the client you must send a message on the
        # same format, informing the client about the change
        # Normally done using signals,observer-pattern or similar

        send_ack = True

        def get_graph():
            try:
                return payload['graph']
            except KeyError:
                raise RillRuntimeError('No graph specified')

        def update_subnet(graph_id):
            spec = self.runtime.register_subnet(graph_id)
            self.send(
                'component',
                'component',
                spec
            )

        try:
            # New graph
            if command == 'clear':
                self.runtime.new_graph(
                    payload['id'],
                    payload.get('description', None),
                    payload.get('metadata', None)
                )
            # Nodes
            elif command == 'addnode':
                self.runtime.add_node(get_graph(), payload['id'],
                                      payload['component'],
                                      payload.get('metadata', {}))
            elif command == 'removenode':
                self.runtime.remove_node(get_graph(), payload['id'])
            elif command == 'renamenode':
                self.runtime.rename_node(get_graph(), payload['from'],
                                         payload['to'])
            # Edges/connections
            elif command == 'addedge':
                metadata = self.runtime.add_edge(get_graph(), payload['src'],
                                                 payload['tgt'],
                                                 payload.get('metadata', {}))
                # send an immedate followup to set the color based on type
                send_ack = True
                payload['metadata'] = metadata
                self.send('graph', command, payload)
                self.send('graph', 'changeedge', payload)
            elif command == 'removeedge':
                self.runtime.remove_edge(get_graph(), payload['src'],
                                         payload['tgt'])
            # IIP / literals
            elif command == 'addinitial':
                self.runtime.initialize_port(get_graph(), payload['tgt'],
                                             payload['src']['data'])
            elif command == 'removeinitial':
                iip = self.runtime.uninitialize_port(get_graph(),
                                                     payload['tgt'])
                payload['src'] = {'data': iip}
                # FIXME: hard-wiring metdata here to pass fbp-test
                payload['metadata'] = {}
            # Exported ports
            elif command in ('addinport', 'addoutport'):
                self.runtime.add_export(get_graph(), payload['node'],
                                        payload['port'], payload['public'], payload['metadata'])
                update_subnet(get_graph())
            elif command == 'removeinport':
                self.runtime.remove_inport(get_graph(), payload['public'])
                update_subnet(get_graph())
            elif command == 'removeoutport':
                self.runtime.remove_outport(get_graph(), payload['public'])
                update_subnet(get_graph())
            elif command == 'changeinport':
                self.runtime.change_inport(
                    get_graph(), payload['public'], payload['metadata'])
            elif command == 'changeoutport':
                self.runtime.change_outport(
                    get_graph(), payload['public'], payload['metadata'])
            # Metadata changes
            elif command == 'changenode':
                metadata = self.runtime.set_node_metadata(get_graph(),
                                                          payload['id'],
                                                          payload['metadata'])
                payload['metadata'] = metadata
            elif command == 'changeedge':
                metadata = self.runtime.set_edge_metadata(get_graph(),
                                                          payload['src'],
                                                          payload['tgt'],
                                                          payload['metadata'])
                payload['metadata'] = metadata
            elif command == 'getgraph':
                send_ack = False
                graph_id = payload['id']
                try:
                    graph = self.runtime.get_graph(graph_id)
                    graph_messages = get_graph_messages(
                        graph, graph_id)
                    for command, payload in graph_messages:
                        self.send('graph', command, payload)
                except RillRuntimeError as ex:
                    self.runtime.new_graph(graph_id)

            elif command == 'list':
                send_ack = False
                for graph_id in self.runtime._graphs.keys():
                    self.send('graph', 'graph', {
                        'id': graph_id
                    })

                self.send('graph', 'graphsdone', None)

            elif command == 'changegraph':
                send_ack = True
                self.runtime.change_graph(
                    get_graph(),
                    payload.get('description', None),
                    payload.get('metadata', None)
                )

            elif command == 'renamegraph':
                send_ack = True
                self.runtime.rename_graph(payload['from'], payload['to'])

            else:
                self.logger.warn("Unknown command '%s' for protocol '%s'" %
                                 (command, 'graph'))
                return
        except FlowError as ex:
            self.send_error('graph', str(ex))

        # For any message we respected, send same in return as
        # acknowledgement
        if send_ack:
            self.send('graph', command, payload)
            print("CLIENTS: {}".format(len(clients.items())))
            for client_id, client in clients.items():
                if client_id != self.client_id:
                    client.send('graph', command, payload, message_id)

    def handle_network(self, command, payload, message_id):
        """
        Start / Stop and provide status messages about the network.
        Parameters
        ----------
        command : str
        payload : dict
        """
        def send_status(cmd, g, timestamp=True, broadcast=False):
            started, running = self.runtime.get_status(g)
            data = {
                'graph': g,
                'started': started,
                'running': running,
                # 'debug': True,
            }
            if timestamp:
                data['time'] = datetime.datetime.now().isoformat()

            self.send('network', cmd, data)
            if broadcast:
                for client_id, client in clients.items():
                    if client_id != self.client_id:
                        client.send('network', cmd, data)
            # FIXME: hook up component logger to and output handler
            # self.send('network', 'output', {'message': 'TEST!'})
            # if started and running:
            #     payload = {u'component': u'tests.components/GenerateTestData',
            #   u'graph': u'575ed4de-39c9-3698-a4be-f5395d9eda2f',
            #   u'id': u'tests.components/GenerateTestData_zoa2g',
            #   u'metadata': {u'label': u'GenerateTestData',
            #                 u'x': 334,
            #                 u'y': 100},
            #   u'secret': u'9129923'}
            #     self.send('graph', 'addnode', payload)

        graph_id = payload.get('graph', None)
        if command == 'getstatus':
            send_status('status', graph_id, timestamp=False)
        elif command == 'start':
            callback = functools.partial(
                send_status, 'stopped', graph_id, broadcast=True)
            self.runtime.start(graph_id, callback)
            send_status('started', graph_id, broadcast=True)
        elif command == 'stop':
            self.runtime.stop(graph_id)
            send_status('stopped', graph_id, broadcast=True)
        elif command == 'debug':
            self.runtime.set_debug(graph_id, payload['enable'])
            self.send('network', 'debug', payload)
        else:
            self.logger.warn("Unknown command '%s' for protocol '%s'" %
                             (command, 'network'))


# FIXME: do we need the host?
def serve_runtime(runtime=None, host=DEFAULTS['host'], port=DEFAULTS['port'],
                  registry_host=DEFAULTS['registry_host'],
                  registry_port=DEFAULTS['registry_port']):

    runtime = runtime if runtime is not None else Runtime()
    address = 'ws://{}:{:d}'.format(host, port)

    def runtime_application_task():
        """
        This greenlet runs the websocket server that responds to remote commands
        that inspect/manipulate the Runtime.
        """
        print('Runtime listening at {}'.format(address))
        WebSocketRuntimeApplication.runtimes[port] = runtime
        try:
            r = geventwebsocket.Resource(
                OrderedDict([('/', WebSocketRuntimeApplication)]))
            s = geventwebsocket.WebSocketServer(('', port), r)
            s.serve_forever()
        finally:
            WebSocketRuntimeApplication.runtimes.pop(port)

    def local_registration_task():
        """
        This greenlet will run the rill registry to register the runtime with
        the ui.
        """
        from rill.registry import serve_registry
        serve_registry(registry_host, registry_port, host, port)

    tasks = [runtime_application_task, local_registration_task]

    # Start!
    gevent.wait([gevent.spawn(t) for t in tasks])
