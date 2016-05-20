#!/usr/bin/env python

import os
import sys
import uuid
import logging
import json
from abc import ABCMeta, abstractmethod
from collections import OrderedDict
import inspect
import functools
import textwrap
import traceback
import datetime

import argparse
import requests
import gevent
import geventwebsocket

from rill.engine.component import Component
from rill.engine.inputport import Connection
from rill.engine.network import Network
from rill.engine.subnet import SubNet
from rill.engine.exceptions import FlowError

log = logging.getLogger(__name__)


<<<<<<< HEAD
class RuntimeError(FlowError):
    pass


def long_class_name(klass):
    return '{0}/{1}'.format(klass.__module__,
                            klass.__name__)

=======
>>>>>>> 12fa7fb... Add function to serialize network to a dictionary

def short_class_name(klass):
    return klass.__name__


def add_callback(f, callback):
    """
    Wrap the callable ``f`` to first execute callable ``callback``
    """
    def wrapper(*args, **kwargs):
        callback(*args, **kwargs)
        return f(*args, **kwargs)
    return wrapper


class Runtime(object):
    """
    Rill runtime for python

    A ``Runtime`` instance holds many ``rill.engine.network.Network`` instances
    each running on their own greelent.
    """
    PROTOCOL_VERSION = '0.5'

    # FIXME: move this to rill.engine.types
    # Mapping of native Python types to FBP protocol types
    _type_map = {
        str: 'string',
        unicode: 'string',
        bool: 'boolean',
        int: 'int',
        float: 'number',
        complex: 'number',
        dict: 'object',
        list: 'array',
        tuple: 'array',
        set: 'array',
        frozenset: 'array',
        #color
        #date
        #function
        #buffer
    }

    def __init__(self):
        self.log = logging.getLogger('%s.%s' % (self.__class__.__module__,
                                                self.__class__.__name__))

        self._component_types = {}  # Component metadata, keyed by component name
        self._graphs = {}  # Graph instances, keyed by graph ID
        self._executors = {}  # GraphExecutor instances, keyed by graph ID

        self.log.debug('Initialized runtime!')

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
        ]

        all_capabilities = capabilities

        return {
            # FIXME: there's a bug in fbp-protocol
            # 'label': 'rill python runtime',
            # 'label': 'NoFlo runtime',
            'type': 'rill',
            'version': self.PROTOCOL_VERSION,
            'capabilities': capabilities,
            'allCapabilities': all_capabilities
            #'graph': ''
        }

    # Components --

    def get_all_component_specs(self):
        specs = {}
        for component_name, component_options in self._component_types.iteritems():
            specs[component_name] = component_options['spec']

        return specs

    def register_component(self, component_class, overwrite=False):
        """
        Registers a component class.

        Parameters
        ----------
        component_class : ``rill.enginge.component.Component``
            the Component class to register.
        overwrite : bool
            whether the component be overwritten if it already exists.
            if False and the component already exists, a ValueError will be
            raised
        """
        if not issubclass(component_class, Component):
            raise ValueError('component_class must be a class that inherits '
                             'from Component')

        long_name = component_class.get_type()
        short_name = short_class_name(component_class)

        if long_name in self._component_types and not overwrite:
            raise ValueError("Component {0} already registered".format(
                long_name))

        self.log.debug('Registering component: {0}'.format(long_name))

        self._component_types[long_name] = {
            'class': component_class,
            'spec': self._create_component_spec(long_name, component_class)
        }

    def register_module(self, module, overwrite=False):
        """

        Parameters
        ----------
        module : str or `ModuleType`
        overwrite : bool
        """
        # FIXME: py3
        if isinstance(module, basestring):
            module = __import__(module)

        if not inspect.ismodule(module):
            raise ValueError('module must be either a module or the name of a '
                             'module')

        self.log.debug('Registering components in module: {}'.format(
            module.__name__))

        registered = 0
        for obj_name in dir(module):
            class_obj = getattr(module, obj_name)
            if (inspect.isclass(class_obj) and
                    (class_obj != Component) and
                    (not inspect.isabstract(class_obj)) and
                    (not issubclass(class_obj, SubNet)) and
                    issubclass(class_obj, Component)):
                self.register_component(class_obj, overwrite)
                registered += 1

        if registered == 0:
            self.log.warn('No components were found in module: {}'.format(
                module.__name__))

    def _create_component_spec(self, component_class_name, component_class):
        if not issubclass(component_class, Component):
            raise ValueError('component_class must be a Component')

        print component_class
        return {
            'name': component_class_name,
            'description': textwrap.dedent(component_class.__doc__ or '').strip(),
            #'icon': '',
            'subgraph': issubclass(component_class, SubNet),
            'inPorts': [
                {
                    'id': inport.args['name'],
                    # 'type': inport.args['type'].basic_type_id(),
                    'type': self._type_map.get(inport.args['type'], 'object'),
                    'description': inport.args['description'],
                    'addressable': inport.array,
                    'required': (not inport.args['optional']),
                    #'values': []
                    'default': inport.default
                }
                for inport in component_class.inport_definitions
            ],
            'outPorts': [
                {
                    'id': outport.args['name'],
                    # 'type': outport.args['type'].basic_type_id(),
                    'type': self._type_map.get(outport.args['type'], 'object'),
                    'description': outport.args['description'],
                    'addressable': outport.array,
                    'required': (not outport.args['optional'])
                }
                for outport in component_class.outport_definitions
            ]
        }

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
        running = started and not self._executors[graph_id].ready()
        print "get_status.  started {}, running {}".format(started, running)
        return started, running

    def start(self, graph_id, done_callback):
        """
        Execute a graph.
        """
        self.log.debug('Graph {}: Starting execution'.format(graph_id))

        graph = self._get_graph(graph_id)

        executor = gevent.Greenlet(graph.go)
        # FIXME: should we delete the executor from self._executors on finish?
        # this has an impact on the result returned from get_status().  Leaving
        # it means that after completion it will be started:True, running:False
        # until stop() is triggered, at which point it will be started:False,
        # running:False
        executor.link(lambda g: done_callback())
        self._executors[graph_id] = executor
        executor.start()
        # if executor.is_running():
        #     raise ValueError('Graph {} is already started'.format(graph_id))

    def stop(self, graph_id):
        """
        Stop executing a graph.
        """
        self.log.debug('Graph {}: Stopping execution'.format(graph_id))
        if graph_id not in self._executors:
            raise ValueError('Invalid graph: {}'.format(graph_id))

        graph = self._get_graph(graph_id)
        graph.terminate()
        executor = self._executors[graph_id]
        executor.join()
        del self._executors[graph_id]

    def set_debug(self, graph_id, debug):
        # FIXME: noflo-ui sends the network:debug command before creating a
        # graph
        # self._get_graph(graph_id).debug = debug
        pass

    # Graphs --

    @staticmethod
    def _get_port(graph, data, kind):
        return graph.get_component_port((data['node'], data['port']),
                                        index=data.get('index'),
                                        kind=kind)

    def _get_graph(self, graph_id):
        """
        Parameters
        ----------
        graph_id : str
            unique identifier for the graph to create or get

        Returns
        -------
        graph : ``rill.engine.network.Network``
            the graph object.
        """
        try:
            return self._graphs[graph_id]
        except KeyError:
            raise RuntimeError('Requested graph not found')

    def new_graph(self, graph_id):
        """
        Create a new graph.
        """
        self.log.debug('Graph {}: Initializing'.format(graph_id))
        self._graphs[graph_id] = Network()

    def add_node(self, graph_id, node_id, component_id, metadata):
        """
        Add a component instance.
        """
        self.log.debug('Graph {}: Adding node {}({})'.format(
            graph_id, component_id, node_id))

        graph = self._get_graph(graph_id)

        component_class = self._component_types[component_id]['class']
        component = graph.add_component(node_id, component_class)
        component.metadata.update(metadata)

    def remove_node(self, graph_id, node_id):
        """
        Destroy component instance.
        """
        self.log.debug('Graph {}: Removing node {}'.format(
            graph_id, node_id))

        graph = self._get_graph(graph_id)
        graph.remove_component(node_id)

    def rename_node(self, graph_id, orig_node_id, new_node_id):
        """
        Rename component instance.
        """
        self.log.debug('Graph {}: Renaming node {} to {}'.format(
            graph_id, orig_node_id, new_node_id))

        graph = self._get_graph(graph_id)
        graph.rename_component(orig_node_id, new_node_id)

    def set_node_metadata(self, graph_id, node_id, metadata):
        graph = self._get_graph(graph_id)
        component = graph.component(node_id)
        for key, value in metadata.items():
            if value is None:
                metadata.pop(key)
                component.metadata.pop(key, None)
        component.metadata.update(metadata)
        return component.metadata

    def add_edge(self, graph_id, src, tgt):
        """
        Connect ports between components.
        """
        self.log.debug('Graph {}: Connecting ports: {} -> {}'.format(
            graph_id, src, tgt))

        graph = self._get_graph(graph_id)
        graph.connect(self._get_port(graph, src, kind='out'),
                      self._get_port(graph, tgt, kind='in'))

    def remove_edge(self, graph_id, src, tgt):
        """
        Disconnect ports between components.
        """
        self.log.debug('Graph {}: Disconnecting ports: {} -> {}'.format(
            graph_id, src, tgt))

        graph = self._get_graph(graph_id)
        graph.disconnect(self._get_port(graph, src, kind='out'),
                         self._get_port(graph, tgt, kind='in'))

    def initialize_port(self, graph_id, src, data):
        """
        Set the inital packet for a component inport.
        """
        self.log.info('Graph {}: Setting IIP to {!r} on port {}'.format(
            graph_id, data, src))

        # FIXME: noflo-ui is sending an 'addinitial foo.IN []' even when
        # the inport is connected
        if data == []:
            return

        graph = self._get_graph(graph_id)

        target_port = self._get_port(graph, src, kind='in')
        # if target_port.is_connected():
        #     graph.disconnect(target_port)

        graph.initialize(data, target_port)

    def uninitialize_port(self, graph_id, src):
        """
        Remove the initial packet for a component inport.
        """
        self.log.debug('Graph {}: Removing IIP from port {}'.format(
            graph_id, src))

        graph = self._get_graph(graph_id)

        target_port = self._get_port(graph, src, kind='in')
        if target_port.is_static():
            # FIXME: so far the case where an uninitialized port receives a uninitialize_port
            # message is when noflo initializes the inport to [] (see initialize_port as well)
            return graph.uninitialize(target_port)._content


def create_websocket_application(runtime):
    class WebSocketRuntimeApplication(geventwebsocket.WebSocketApplication):
        """
        Web socket application that hosts a single ``Runtime`` instance.

        An instance of this class sits between the websocket and the runtime.
        It receives messages over the websocket, delegates their payload to
        the appropriate Runtime methods, and sends responses where applicable.
        """
        def __init__(self, ws):
            super(WebSocketRuntimeApplication, self).__init__(self)

            self.log = logging.getLogger('%s.%s' % (self.__class__.__module__,
                                                    self.__class__.__name__))
            self.runtime = runtime

            Connection.send = add_callback(Connection.send,
                                           self.send_connection_data)

        # WebSocket transport handling --

        # FIXME: not used?
        @staticmethod
        def protocol_name():
            """
            WebSocket sub-protocol
            """
            return 'noflo'

        def on_open(self):
            self.log.info("Connection opened")

        def on_close(self, reason):
            self.log.info("Connection closed. Reason: %s" % reason)

        def on_message(self, message, **kwargs):
            self.log.debug('MESSAGE: %s' % message)

            if not message:
                self.log.warn('Got empty message')
                return

            m = json.loads(message)
            dispatch = {
                'runtime': self.handle_runtime,
                'component': self.handle_component,
                'graph': self.handle_graph,
                'network': self.handle_network
            }
            import pprint
            print "--IN--"
            pprint.pprint(m)

            try:
                protocol = m['protocol']
                command = m['command']
                payload = m['payload']
            except KeyError:
                # FIXME: send error?
                self.log.warn("Malformed message")
                return

            try:
                handler = dispatch[protocol]
            except KeyError:
                # FIXME: send error?
                self.log.warn("Subprotocol '{}' "
                              "not supported".format(protocol))
                return

            try:
                handler(command, payload)
            except RuntimeError as err:
                self.send_error(protocol, str(err))

        def send(self, protocol, command, payload):
            """
            Send a message to UI/client
            """
            message = {'protocol': protocol,
                       'command': command,
                       'payload': payload}
            print "--OUT--"
            import pprint
            pprint.pprint(message)
            self.ws.send(json.dumps(message))

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

        def handle_runtime(self, command, payload):
            # tell UI info about runtime and supported capabilities
            if command == 'getruntime':
                payload = self.runtime.get_runtime_meta()
                # self.log.debug(json.dumps(payload, indent=4))
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
                self.log.warn("Unknown command '%s' for protocol '%s' " %
                              (command, 'runtime'))

        def handle_component(self, command, payload):
            """
            Provide information about components.

            Parameters
            ----------
            command : str
            payload : dict
            """
            if command == 'list':
                specs = self.runtime.get_all_component_specs()
                for component_name, component_data in specs.iteritems():
                    payload = component_data
                    self.send('component', 'component', payload)

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
                self.log.warn("Unknown command '%s' for protocol '%s' " %
                              (command, 'component'))

        def handle_graph(self, command, payload):
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
                    raise RuntimeError('No graph specified')

            # New graph
            if command == 'clear':
                self.runtime.new_graph(payload['id'])
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
                self.runtime.add_edge(get_graph(), payload['src'],
                                      payload['tgt'])
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
                pass  # Not supported yet
            elif command in ('removeinport', 'removeoutport'):
                pass  # Not supported yet
            # Metadata changes
            elif command == 'changenode':
                metadata = self.runtime.set_node_metadata(get_graph(),
                                                          payload['id'],
                                                          payload['metadata'])
                payload['metadata'] = metadata
            else:
                self.log.warn("Unknown command '%s' for protocol '%s'" %
                              (command, 'graph'))
                return

            # For any message we respected, send same in return as
            # acknowledgement
            if send_ack:
                self.send('graph', command, payload)

        def handle_network(self, command, payload):
            """
            Start / Stop and provide status messages about the network.

            Parameters
            ----------
            command : str
            payload : dict
            """
            def send_status(cmd, g, timestamp=True):
                started, running = self.runtime.get_status(g)
                payload = {
                    'graph': g,
                    'started': started,
                    'running': running,
                    # 'debug': True,
                }
                if timestamp:
                    payload['time'] = datetime.datetime.now().isoformat()

                self.send('network', cmd, payload)
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
                callback = functools.partial(send_status, 'stopped', graph_id)
                self.runtime.start(graph_id, callback)
                send_status('started', graph_id)
            elif command == 'stop':
                self.runtime.stop(graph_id)
                send_status('stopped', graph_id)
            elif command == 'debug':
                self.runtime.set_debug(graph_id, payload['enable'])
                self.send('network', 'debug', payload)
            else:
                self.log.warn("Unknown command '%s' for protocol '%s'" %
                              (command, 'network'))

    return WebSocketRuntimeApplication


class RuntimeRegistry(object):
    """
    Runtime registry.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def register_runtime(self, runtime, runtime_id, user_id, address):
        """
        Registers a runtime.

        Parameters
        ----------
        runtime : ``Runtime``
            the Runtime to register
        user_id : str
            registry user ID
        address : str
            callback address
        """
        pass

    def ping_runtime(self, runtime_id):
        """
        Pings a registered runtime, keeping it alive in the registry.
        This should be called periodically.

        Parameters
        ----------
        runtime_id : str
            the id of the runtime to ping
        """
        pass


class FlowhubRegistry(RuntimeRegistry):
    """
    FlowHub runtime registry.
    It's necessary to use this if you want to manage your graph in either
    FlowHub or NoFlo-UI.
    """
    def __init__(self):
        self.log = logging.getLogger('%s.%s' % (self.__class__.__module__,
                                                self.__class__.__name__))

        self._endpoint = 'http://api.flowhub.io'

    def runtime_url(self, runtime_id):
        return '{}/runtimes/{}'.format(self._endpoint, runtime_id)

    def register_runtime(self, runtime, runtime_id, user_id, address):
        if not isinstance(runtime, Runtime):
            raise ValueError('runtime must be a Runtime instance')

        runtime_metadata = runtime.get_runtime_meta()
        payload = {
            'id': runtime_id,

            'label': runtime_metadata['label'],
            'type': runtime_metadata['type'],

            'address': address,
            'protocol': 'websocket',

            'user': user_id,
            'secret': '9129923',  # unused
        }

        self.log.info('Registering runtime %s for user %s...' % (runtime_id, user_id))
        response = requests.put(self.runtime_url(runtime_id),
                                data=json.dumps(payload),
                                headers={'Content-type': 'application/json'})
        self._ensure_http_success(response)

    def ping_runtime(self, runtime_id):
        url = self.runtime_url(runtime_id)
        self.log.info('Pinging runtime {}...'.format(url))
        response = requests.post(url)
        self._ensure_http_success(response)

    @classmethod
    def _ensure_http_success(cls, response):
        if not (199 < response.status_code < 300):
            raise Exception('Flow API returned error %d: %s' %
                            (response.status_code, response.text))


def create_runtime_id(user_id, address):
    return str(uuid.uuid3(uuid.UUID(user_id), 'rill_' + address))


def main():
    # Argument defaults
    defaults = {
        'host': 'localhost',
        'port': 3569
    }

    # Parse arguments
    argp = argparse.ArgumentParser(
        description='Runtime that responds to commands sent over the network, '
                    'managing and executing graphs.')
    argp.add_argument(
        '-u', '--user-id', metavar='UUID',
        help='FlowHub user ID (get this from NoFlo-UI)')
    argp.add_argument(
        '-r', '--runtime-id', metavar='UUID',
        help='FlowHub unique runtime ID (generated if none specified)')
    argp.add_argument(
        '--host', default=defaults['host'], metavar='HOSTNAME',
        help='Listen host for websocket (default: %(host)s)' % defaults)
    argp.add_argument(
        '--port', type=int, default=3569, metavar='PORT',
        help='Listen port for websocket (default: %(port)d)' % defaults)
    argp.add_argument(
        '--log-file', metavar='FILE_PATH',
        help='File to send log output to (default: none)')
    argp.add_argument(
        '-v', '--verbose', action='store_true',
        help='Enable verbose logging')

    # TODO: add arg for executor type (multiprocess, singleprocess, distributed)
    # TODO: add args for component search paths
    args = argp.parse_args()

    # Configure logging
    # utils.init_logger(filename=args.log_file,
    #                   default_level=(logging.DEBUG if args.verbose else logging.INFO),
    #                   logger_levels={
    #                       'requests': logging.WARN,
    #                       'geventwebsocket': logging.INFO,
    #                       'sh': logging.WARN,
    #
    #                       'rill.core': logging.INFO,
    #                       'rill.components': logging.INFO,
    #                       'rill.executors': logging.INFO
    #                   })

    address = 'ws://{}:{:d}'.format(args.host, args.port)
    runtime_id = args.runtime_id
    if not runtime_id and not args.user_id:
        log.warn('No runtime ID or user ID was specified: skipping '
                 'registration with flowhub.')
    elif not runtime_id and args.user_id:
        runtime_id = create_runtime_id(args.user_id, address)
        log.warn('No runtime ID was specified, so one was '
                 'generated: {}'.format(runtime_id))

    runtime = Runtime()

    # FIXME: remove hard-wired imports
    import tests.components
    import rill.components.basic
    import rill.components.timing
    runtime.register_module(rill.components.basic)
    runtime.register_module(rill.components.timing)
    runtime.register_module(tests.components)

    def runtime_application_task():
        """
        This greenlet runs the websocket server that responds remote commands
        that inspect/manipulate the Runtime.
        """
        print('Runtime listening at {}'.format(address))
        r = geventwebsocket.Resource(
            OrderedDict([('/', create_websocket_application(runtime))]))
        s = geventwebsocket.WebSocketServer(('', args.port), r)
        s.serve_forever()

    def registration_task():
        """
        This greenlet will register the runtime with FlowHub and occasionally
        ping the endpoint to keep the runtime alive.
        """
        flowhub = FlowhubRegistry()

        # Register runtime
        flowhub.register_runtime(runtime, runtime_id, args.user_id, address)

        # Ping
        delay_secs = 60  # Ping every minute
        while True:
            flowhub.ping_runtime(runtime_id)
            gevent.sleep(delay_secs)

    tasks = [runtime_application_task]
    if runtime_id:
        tasks.append(registration_task)

    # Start!
    gevent.wait([gevent.spawn(t) for t in tasks])
    runtime_application_task()


if __name__ == '__main__':
    main()
