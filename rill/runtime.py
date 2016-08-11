import os
import pydoc
import logging
import json
from collections import OrderedDict
import inspect
import weakref
from functools import wraps

import gevent
import geventwebsocket

from rill.engine.component import Component
from rill.engine.network import Graph, Network
from rill.engine.subnet import SubGraph, make_subgraph
from rill.engine.types import FBP_TYPES, Stream
from rill.engine.exceptions import FlowError
from rill.plumbing import Client, RuntimeServer, Message
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


class WebSocketRuntimeApplication(geventwebsocket.WebSocketApplication):
    """
    Web socket application that hosts a single ``Runtime`` instance.
    An instance of this class receives messages over a websocket, delegates
    message payloads to the appropriate ``Runtime`` methods, and sends
    responses where applicable.
    Message structures are defined by the FBP Protocol.
    """

    def __init__(self, ws):
        super(WebSocketRuntimeApplication, self).__init__(ws)

        self.client = Client(self.on_response)
        self.client.connect("tcp://localhost", 5556)

        self.logger = logging.getLogger('{}.{}'.format(
            self.__class__.__module__, self.__class__.__name__))

    # WebSocketApplication overrides --

    @staticmethod
    def protocol_name():
        """
        WebSocket sub-protocol
        """
        return 'noflo'

    def on_message(self, message, **kwargs):
        self.logger.debug('INCOMING: {}'.format(message))
        self.client.send(Message(**json.loads(message)))

    def on_response(self, msg):
        self.logger.debug("OUTCOMING: %r" % msg)
        self.ws.send(json.dumps(msg.to_dict()))


# FIXME: do we need the host?
def serve_runtime(runtime=None, host=DEFAULTS['host'], port=DEFAULTS['port'],
                  registry_host=DEFAULTS['registry_host'],
                  registry_port=DEFAULTS['registry_port']):

    runtime = runtime if runtime is not None else Runtime()
    address = 'ws://{}:{:d}'.format(host, port)

    def runtime_server_task():
        server = RuntimeServer(runtime)
        server.start()

    def websocket_application_task():
        """
        This greenlet runs the websocket server that responds to remote commands
        that inspect/manipulate the Runtime.
        """
        print('Runtime listening at {}'.format(address))
        r = geventwebsocket.Resource(
            OrderedDict([('/', WebSocketRuntimeApplication)]))
        server = geventwebsocket.WebSocketServer(('', port), r)
        server.serve_forever()

    def local_registration_task():
        """
        This greenlet will run the rill registry to register the runtime with
        the ui.
        """
        from rill.registry import serve_registry
        serve_registry(registry_host, registry_port, host, port)

    tasks = [runtime_server_task, websocket_application_task]

    # Start!
    gevent.wait([gevent.spawn(t) for t in tasks])
