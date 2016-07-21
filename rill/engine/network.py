"""
This module is responsible for applying gevent monkey patching, so it should
be imported before all others if you intend to process a network in the current
python process.
"""
from collections import defaultdict, OrderedDict
from inspect import isclass
import time
import copy

from future.utils import raise_with_traceback
from past.builtins import basestring
from typing import Dict, Union, Tuple, Any, Optional

from rill.engine.exceptions import FlowError, NetworkDeadlock
from rill.engine.runner import ComponentRunner
from rill.engine.component import Component, logger
from rill.engine.status import StatusValues
from rill.engine.outputport import OutputPort, OutputArray
from rill.engine.inputport import Connection, InputPort, InputArray, InitializationConnection
from rill.engine.utils import CountDownLatch


class Graph(object):
    """
    A graph of ``Components``
    """
    def __init__(self, name=None, default_capacity=10):
        self.name = name
        self.default_capacity = default_capacity

        # type: OrderedDict[str, rill.engine.component.Component]
        self._components = OrderedDict()

        # exported inports and outports
        # type: OrderedDict[str, rill.engine.inputport.InputPort]
        self.inports = OrderedDict()
        # type: OrderedDict[str, rill.engine.inputport.OutputPort]
        self.outports = OrderedDict()

    def __repr__(self):
        return '{}(name={!r})'.format(self.__class__.__name__, self.name)

    def __getstate__(self):
        # avoid copying InputPorts and OutputPorts
        data = self.__dict__.copy()
        for k in ('inports', 'outports'):
            data.pop(k)

        inports = []
        for name, port in self.inports.items():
            inports.append((name, port.component.name, port._name, port.index))
        data['inports'] = inports

        outports = []
        for name, port in self.outports.items():
            outports.append((name, port.component.name, port._name, port.index))
        data['outports'] = outports
        return data

    def __setstate__(self, data):
        # avoid copying InputPorts and OutputPorts
        inports = data.pop('inports')
        outports = data.pop('outports')
        self.__dict__.update(data)
        self.inports = OrderedDict()
        for name, comp_name, port_name, port_index in inports:
            self.inports[name] = self.get_component_port(
                (comp_name, port_name), index=port_index)
        self.outports = OrderedDict()
        for name, comp_name, port_name, port_index in outports:
            self.outports[name] = self.get_component_port(
                (comp_name, port_name), index=port_index)

    def copy(self, deep=True):
        """
        Create a deep copy of the graph.

        Returns
        -------
        ``Graph``
        """
        if deep:
            return copy.deepcopy(self)
        else:
            graph = Graph(self.name, self.default_capacity)
            graph.inports = self.inports.copy()
            graph.outports = self.outports.copy()
            graph._components = self._components.copy()
            return graph

    # Components --

    def add_component(self, *args, **initializations):
        """
        Instantiate a component and add it to the network.

        Parameters
        ----------
        name : str
            name of component
        comp_type : Type[``rill.engine.component.Component``]
            component class to instantiate

        Returns
        -------
        ``rill.engine.component.Component``
        """
        if len(args) == 1:
            arg = args[0]
            if issubclass(arg, Component):
                comp = arg
                name = comp.name
            elif isclass(arg) and issubclass(arg, Component):
                comp_type = arg
                name = comp_type.type_name or comp_type.__name__
                basename = name
                i = 0
                while name in self._components:
                    i += 1
                    name = basename + str(i)
                comp = comp_type(name)
            else:
                raise ValueError()
        elif len(args) == 2:
            name, comp_type = args
            if not isclass(comp_type) or not issubclass(comp_type, Component):
                raise TypeError("comp_type must be a sub-class of Component")
            comp = comp_type(name)
        else:
            raise ValueError()

        if name in self._components:
            raise FlowError(
                "Component {} already exists in network".format(name))

        self.put_component(name, comp)

        for name, value in initializations.items():
            receiver = comp.port(name, kind='in')
            if value is None and not receiver.required:
                continue
            self.initialize(value, receiver)
        return comp

    def add_graph(self, name, graph, **initializations):
        """
        Instantiate a component and add it to the network.

        Parameters
        ----------
        name : str
            name of component within the graph
        graph : ``Graph``
            graph to add

        Returns
        -------
        ``rill.engine.component.Component``
        """
        from rill.engine.subnet import make_subgraph
        comp_type = make_subgraph(name, graph)
        return self.add_component(name, comp_type, **initializations)

    def remove_component(self, name):
        # FIXME: this needs some love
        # assert not self.active
        component = self._components.pop(name)
        for inport in component.inports:
            if inport.is_connected() and not inport.is_static():
                for outport in inport._connection.outports:
                    self.disconnect(inport, outport)
        for outport in component.outports:
            if outport.is_connected():
                self.disconnect(outport._connection.inport, outport)
        # FIXME: remove references in self.inports and self.outports

    def rename_component(self, orig_name, new_name):
        # FIXME: this needs some love
        # assert not self.active
        assert new_name not in self._components
        component = self._components.pop(orig_name)
        self._components[new_name] = component

    def component(self, name):
        """
        Parameters
        ----------
        name : str
            name of component

        Returns
        -------
        ``rill.engine.component.Component``

        Raises
        ------
        ``rill.engine.exceptions.FlowError`` : if no component found
        """
        comp = self.get_component(name)
        if comp is None:
            raise FlowError("Reference to unknown component " + name)
        return comp

    def get_components(self):
        """
        Get a dictionary of components in this network

        Returns
        -------
        Dict[str, ``rill.enginge.component.Component``]
        """
        return self._components

    def get_component(self, name):
        """
        Returns the requested component in this network if present or None
        if not present.

        Returns
        -------
        ``rill.enginge.component.Component``
        """
        return self._components.get(name)

    # FIXME: remove this and run/move Component._init to __init__
    def put_component(self, name, comp):
        """
        Adds a component and inits it.

        Parameters
        ----------
        name : str
        comp : ``rill.enginge.component.Component``
        """
        # initialize the component's ports so they can be used for network
        # building
        comp._init()
        self._components[name] = comp

    # Ports --

    def export(self, internal_port, external_port_name):
        """
        Exports component port for connecting to other networks
        as a sub network

        Parameters
        ----------
        internal_port : ``rill.engine.outputport.OutputPort`` or
            ``rill.engine.inputport.InputPort`` or str
        external_port_name : str
            name of port that will be exposed
        """
        internal_port = self.get_component_port(internal_port)

        if isinstance(internal_port, InputPort):
            self.inports[external_port_name] = internal_port

        elif isinstance(internal_port, OutputPort):
            self.outports[external_port_name] = internal_port

    def get_component_port(self, arg, index=None, kind=None):
        """
        Get a port on a component.

        Parameters
        ----------
        arg : Union[``rill.engine.outputport.OutputPort``, ``rill.engine.inputport.InputPort``, str]
        index : Optional[int]
            index of element, if port is an array. If None, the next available
            index is used
        kind : str
            {'in', 'out'}

        Returns
        -------
        Union[``rill.engine.outputport.OutputPort``, ``rill.engine.inputport.InputPort``]
        """
        if isinstance(arg, (OutputPort, OutputArray, InputPort, InputArray)):
            port = arg
            if kind is not None and port.kind != kind:
                raise FlowError(
                    "Expected {}port: got {}".format(kind, type(port)))
        else:
            if isinstance(arg, (tuple, list)):
                comp_name, port_name = arg
            elif isinstance(arg, basestring):
                comp_name, port_name = arg.split('.')
            else:
                raise TypeError(arg)

            comp = self.component(comp_name)
            port = comp.port(port_name, kind=kind)

        if port.is_array() and index is not False:
            port = port.get_element(index, create=True)

        return port

    def connect(self, sender, receiver, connection_capacity=None,
                count_packets=False):
        """
        Connect an output port of one component to an input port of another.

        Parameters
        ----------
        sender : Union[``rill.engine.inputport.InputPort``, str]
        receiver : Union[``rill.engine.outputport.OutputPort``, str]

        Returns
        -------
        ``rill.engine.inputport.InputPort``
        """
        outport = self.get_component_port(sender, kind='out')
        inport = self.get_component_port(receiver, kind='in')
        assert not outport.is_array()
        assert not inport.is_array()

        if connection_capacity is None:
            connection_capacity = self.default_capacity

        if inport._connection is None:
            inport._connection = Connection()
        inport._connection.connect(inport, outport, connection_capacity)
        return inport

    def disconnect(self, sender, receiver):
        """
        Disconnect an output port of one component from an input port of
        another.

        Parameters
        ----------
        sender : Union[``rill.engine.inputport.InputPort``, str]
        receiver : Union[``rill.engine.outputport.OutputPort``, str]
        """
        outport = self.get_component_port(sender, kind='out')
        inport = self.get_component_port(receiver, kind='in')
        outport._connection = None
        inport._connection.outports.remove(outport)
        if not inport._connection.outports:
            inport._connection = None
        else:
            inport._connection.metadata.pop(outport)

    def initialize(self, content, receiver):
        """
        Initialize an inport port with a value

        Parameters
        ----------
        content : Any
        receiver : Union[``rill.engine.inputport.InputPort``, str]
        """
        inport = self.get_component_port(receiver, kind='in')
        inport.initialize(content)

    def uninitialize(self, receiver):
        """
        Remove the initialized value of an inport port.

        Parameters
        ----------
        receiver : Union[``rill.engine.inputport.InputPort``, str]

        Returns
        -------
        ``rill.engine.inputport.InitializationConnection``
            The removed initialization connection
        """
        inport = self.get_component_port(receiver, kind='in')
        return inport.uninitialize()

    def validate(self):
        """
        Validate the graph.
        """
        errors = []
        for component in self._components.values():
            for port in component.ports:
                try:
                    port.validate()
                except FlowError as e:
                    errors.append(str(e))
        if errors:
            for error in errors:
                logger.error(error)
            raise FlowError("Errors opening ports")

    # Serialization --

    def to_dict(self):
        """
        Serialize graph to dictionary

        Returns
        -------
        definition : dict
            json-serializable representation of graph according to fbp json
            standard
        """
        def portdef(cname, p):
            doc = {
                'process': cname,
                # FIXME: it should be easier to get this value from BasePort
                'port': p._name if p.is_element() else p.name
            }
            if p.is_element():
                doc['index'] = p.index
            return doc

        definition = {
            'processes': {},
            'connections': [],
            'inports': {},
            'outports': {}
        }
        for (comp_name, component) in self.get_components().items():
            if component.hidden:
                continue

            definition['processes'][comp_name] = {
                "component": component.get_type(),
                "metadata": component.metadata,
            }

            for inport in component.inports:
                if not inport.is_connected():
                    continue

                conn = inport._connection
                if isinstance(conn, InitializationConnection):
                    content = conn._content
                    content = inport.type.to_primitive(content)
                    connection = {
                        'src': {'data': content},
                        'tgt': portdef(comp_name, inport)
                    }
                    if conn.metadata:
                        connection['metadata'] = conn.metadata
                    definition['connections'].append(connection)
                else:
                    for outport in conn.outports:
                        if outport.component.hidden:
                            continue

                        sender = outport.component
                        connection = {
                            'src': portdef(sender.get_name(), outport),
                            'tgt': portdef(comp_name, inport)
                        }
                        # keyed on outport
                        metadata = conn.metadata.get(outport, None)
                        if metadata:
                            connection['metadata'] = metadata
                        definition['connections'].append(connection)

        for (name, inport) in self.inports.items():
            definition['inports'][name] = {
                'process': inport.component.get_name(),
                'port': inport.name
            }

        for (name, outport) in self.outports.items():
            definition['outports'][name] = {
                'process': outport.component.get_name(),
                'port': outport.name
            }

        return definition

    @classmethod
    def from_dict(cls, definition, component_lookup=None):
        """
        Create network from serialized definition

        Parameters
        ----------
        definition : dict
            network structure according to fbp json standard
        component_lookup : Dict[str, ``rill.enginge.component.Component``]
            maps of component name to component

        Returns
        -------
        ``rill.enginge.network.Graph``
        """
        import pydoc

        def _port(p):
            return graph.get_component_port((p['process'], p['port']),
                                            index=p.get('index', None))

        graph = cls()
        for (name, spec) in definition['processes'].items():
            if component_lookup:
                comp_class = component_lookup[spec['component']]
            else:
                comp_class = pydoc.locate(spec['component'].replace('/', '.'))
                if comp_class is None:
                    raise FlowError("Could not find component {}".format(
                        spec['component']))
            component = graph.add_component(name, comp_class)
            if spec.get('metadata'):
                component.metadata.update(spec['metadata'])

        for connection in definition['connections']:
            tgt = _port(connection['tgt'])
            if 'data' in connection['src']:
                # static initializer
                content = connection['src']['data']
                content = tgt.type.to_native(content)
                graph.initialize(content, tgt)
            else:
                # connection
                src = _port(connection['src'])
                graph.connect(src, tgt)

        for (name, inport) in definition['inports'].items():
            graph.export(_port(inport), name)

        for (name, outport) in definition['outports'].items():
            graph.export(_port(outport), name)

        return graph


class Network(object):
    """
    Responsible for executing a ``Graph`` instance.
    """

    def __init__(self, graph, deadlock_test_interval=1):
        """

        Parameters
        ----------
        graph : ``Graph``
        deadlock_test_interval : int
        """
        # self.logger = logger
        # type: Graph
        self.graph = graph
        # parent Network: set by SubGraph
        # type: Network
        self.parent_network = None
        self.deadlock_test_interval = deadlock_test_interval

        self.active = False  # used for deadlock detection

        # FIXME: not used
        self.timeouts = {}

        # variables with a life-span of one run (set by reset()):

        # FIXME: get rid of this. using gevent.iwait now
        self.cdl = None

        # type: List[ComponentRunner]
        self.runners = None
        # globals is a global synchronized_map intended for real global use.
        # It is not intended for component-to-component communication.
        self.globals = None
        self.deadlock = None

        # holds the first error raised
        self.error = None
        self._abort = None
        # for use by list_comp_status(). here for post-mortem inspection
        self.msgs = None

        # type: Dict[rill.engine.inputports.Connection, int]
        self._packet_counts = None

        # FIXME: these were AtomicInteger instances, with built-in locking.
        # might not be safe to make them regular ints
        self.sends = self.receives = self.creates = self.drops = self.drop_olds = None

    def __getstate__(self):
        data = self.__dict__.copy()
        for k in ('cdl', 'runners', 'msgs'):
            data.pop(k)
        if self.runners is not None:
            data['runners'] = [runner.status for runner in self.runners]
        return data

    def __setstate__(self, data):
        runners = data.pop('runners', None)
        self.__dict__.update(data)
        if runners is not None:
            self._build_runners()
            for runner, status in zip(self.runners, runners):
                runner.status = status

    def reset(self):
        # freq = 0.5 if self.deadlock_test_interval else None
        # self.cdl = CountDownLatch(len(self._components), freq)

        self.globals = {}
        self.deadlock = False
        self.error = None
        self._abort = False
        self.msgs = []
        self._packet_counts = defaultdict(int)

        self.receives = 0
        self.sends = 0
        self.creates = 0
        self.drops = 0
        self.drop_olds = 0

        for name, comp in self.graph._components.items():
            comp.init()

    def go(self, resume=False):
        """
        Execute the network
        """
        import gevent

        now = time.time()

        self.active = True
        deadlock_thread = None

        try:
            if resume:
                self.resume()
            else:
                self.initiate()

            if self.deadlock_test_interval:
                deadlock_thread = gevent.spawn(self._test_deadlocks)

            self.wait_for_all()
        except FlowError as e:
            s = "Flow Error :" + str(e)
            logger.info("Network: " + s)
            raise
        finally:
            self.active = False

        if deadlock_thread:
            deadlock_thread.kill()

        duration = time.time() - now
        logger.info("Run complete.  Time: %.02f seconds" % duration)
        logger.info("Counts:")
        logger.info(" creates:        %d", self.creates)
        logger.info(" drops (manual): %d", self.drops)
        logger.info(" drops (old):    %d", self.drop_olds)
        logger.info(" sends:          %d", self.sends)
        logger.info(" receives:       %d", self.receives)

        if self.error is not None:
            logger.error("re-rasing error")
            # throw the exception which caused the network to stop
            raise_with_traceback(self.error)

    # FIXME: get rid of this:  we don't need the CDL anymore...
    # may be useful if we want to support threading systems other than gevent
    def indicate_terminated(self, comp):
        # -- synchronized (comp)
        comp.status = StatusValues.TERMINATED
        # -- end
        logger.debug("{}: Terminated", args=[comp])
        # self.cdl.count_down()
        # net.interrupt()

    def _build_runners(self):
        """
        Populate `self.runners` with a runner for each component.
        """
        self.runners = []
        for comp in self.graph._components.values():
            runner = ComponentRunner(comp, self)
            comp._runner = runner
            self.runners.append(runner)
            runner.status = StatusValues.NOT_STARTED

    def _open_ports(self):
        self.graph.validate()
        for runner in self.runners:
            runner.open_ports()

    def _close_ports(self):
        for runner in self.runners:
            runner.close_ports()

    def resume(self):
        """
        Resume a graph that has been suspended.
        """
        self_starters = []
        for runner in self.runners:
            for port in runner.component.inports:
                if port.is_connected() and not port.is_static() and \
                        port._connection._queue:
                    logger.info("Existing data in connection buffer: {}",
                                args=[port])

            if runner.status in (StatusValues.TERMINATED, StatusValues.ERROR):
                runner.kill()
                continue

            elif runner.self_starting or \
                    runner.status in (StatusValues.SUSP_RECV,
                                      StatusValues.SUSP_SEND,
                                      StatusValues.DORMANT,
                                      StatusValues.ACTIVE):
                self_starters.append(runner)

        if not self_starters:
            raise FlowError("No self-starters found")

        for runner in self_starters:
            runner.activate()

    def initiate(self):
        """
        Go through components opening ports, and activating those which are
        self-starting (have no input connections)
        """
        self.reset()
        self._build_runners()
        self._open_ports()
        self_starters = [r for r in self.runners if r.self_starting]

        if not self_starters:
            raise FlowError("No self-starters found")

        for runner in self_starters:
            runner.activate()

    def interrupt_all(self):
        """
        Interrupt all components
        """
        logger.warning("*** Crashing whole application!")

        # FIXME: overkill
        sys.exit(0)  # trying this - see if more friendly!

    # FIXME: make private
    def wait_for_all(self):
        """
        Test if network as a whole has terminated
        """
        import gevent

        try:
            for completed in gevent.iwait(self.runners):
                logger.debug("Component completed: {}".format(completed))
                # if an error occurred, skip deadlock testing
                if self.error is not None:
                    break

                # if the network was aborted, skip deadlock testing
                if self._abort:
                    break
        except gevent.hub.LoopExit:
            statuses = []
            if self.list_comp_status(statuses):
                self._signal_deadlock(statuses)

    def _signal_deadlock(self, statuses):
        # FIXME: move error messages into NetworkDeadlock and get rid of this method?
        logger.error("Network has deadlocked")
        for status, objs in statuses:
            logger.error("  {:<13}{{}}".format(status), args=objs)
        raise NetworkDeadlock("Deadlock detected in Network", statuses)

    def _test_deadlocks(self):
        """
        Test the network for deadlocks

        Raises
        ------
        ``rill.exceptions.NetworkDeadlock``
        """
        import gevent
        deadlock_status = None
        while self.active:
            statuses = []
            # if True, it is a potential deadlock
            if self.list_comp_status(statuses):
                if deadlock_status is None:
                    deadlock_status = statuses
                elif statuses == deadlock_status:
                    self.terminate()
                    # same set of statuses two checks in a row: deadlocked
                    self._signal_deadlock(statuses)
                else:
                    deadlock_status = None
            else:
                deadlock_status = None
            gevent.sleep(self.deadlock_test_interval)

    def list_comp_status(self, msgs):
        """
        Queries the status of the subnet's components.

        Parameters
        ----------
        msgs : List[Tuple[``StatusValues``, List[object]]]
            status messages

        Returns
        -------
        bool
            whether the network is deadlocked
        """
        from rill.engine.subnet import SubGraph
        # Messages are added to list, rather than written directly,
        # in case it is not a self.deadlock

        terminated = True
        for runner in self.runners:
            if isinstance(runner, SubGraph):
                # consider components of subnets
                if not runner.sub_network.list_comp_status(msgs):
                    return False
            else:
                status = runner.status
                if status in (StatusValues.ACTIVE, StatusValues.LONG_WAIT):
                    return False

                if status != StatusValues.TERMINATED:
                    terminated = False

                if status == StatusValues.SUSP_RECV:
                    objs = [runner.curr_conn]
                elif status == StatusValues.SUSP_SEND:
                    objs = [runner.curr_outport._connection]
                else:
                    objs = [runner]

                msgs.append((status, objs))

        return not terminated

    def test_timeouts(self, freq):
        for t in self.timeouts.values():
            t.decrement(freq)  # if negative, complain

    def signal_error(self, e):
        """
        Handle errors in the network.

        Records the error and terminates all components.
        """
        # only react to the first error, the others presumably are inherited
        # errors
        if self.error is None:
            assert isinstance(e, Exception)
            # set the error field to let go() raise the exception
            self.error = e
            # terminate the network's components
            for comp in self.runners:
                comp.terminate(StatusValues.ERROR)

    def terminate(self, new_status=StatusValues.TERMINATED):
        """
        Shut down the network
        """
        # prevent deadlock testing, components will be shut down anyway
        self._abort = True
        for comp in self.runners:
            comp.terminate(new_status)

    # FIXME: consider removing this and the next
    # these packet count methods overlap with the creates/sends/receives counts.
    # they're special built for use by one component
    def get_packet_counts(self):
        """
        Get a dictionary of connection to count of packet received

        Returns
        -------
        dict
        """
        return self._packet_counts

    def incr_packet_count(self, connection):
        """
        Increment the packet count for the given connection.

        This is purely for analysis.

        Parameters
        ----------
        connection : ``rill.engine.inputports.Connection``
        """
        self._packet_counts[connection] += 1


def run_graph(graph, initializations=None, capture_results=False):
    """
    Run a graph.

    Parameters
    ----------
    graph : ``rill.engine.network.Graph``
    initializations : Optional[Dict[str, Any]]
        map of exported inport names to initial content
    capture_results : Union[bool, List[str]]
        list of exported outport names whose results should be collected.
        True for all, False for none.

    Returns
    -------
    Dict[str, Any]
        map network outport names to captured values
    """
    from rill.components.basic import Capture

    if capture_results is True:
        outports = graph.outports.keys()
        if not outports:
            raise FlowError("Cannot capture results: graph has no exported "
                            "outports")
    elif capture_results is False:
        outports = []
    else:
        outports = capture_results

    initializations = initializations or {}

    if outports or initializations:
        # use a wrapper so we don't modify the passed graph
        # we could also copy the graph, but seeing as we're working with
        # inports and outports, it might be best to operate on the graph as a
        # SubGraph to ensure consistent behavior
        wrapper = Graph()
        apply = wrapper.add_graph('Apply', graph)

        for (port_name, content) in initializations.items():
            wrapper.initialize(content, apply.port(port_name))

        captures = {}
        for port_name in outports:
            capture_name = 'Capture_{}'.format(port_name)
            capture = wrapper.add_component(capture_name, Capture)
            wrapper.connect(apply.port(port_name), capture.port('IN'))
            captures[port_name] = capture
        graph = wrapper

    Network(graph).go()

    if outports:
        return {name: capture.value for (name, capture) in captures.items()}
