"""
This module is responsible for applying gevent monkey patching, so it should
be imported before all others if you intend to process a network in the current
python process.
"""
from rill.engine.utils import patch
patch()

from collections import defaultdict, OrderedDict
from inspect import isclass
import time

from future.utils import raise_with_traceback
from past.builtins import basestring

from rill.engine.exceptions import FlowError, NetworkDeadlock
from rill.engine.runner import ComponentRunner
from rill.engine.component import Component, logger
from rill.engine.status import StatusValues
from rill.engine.outputport import OutputPort, OutputArray
from rill.engine.inputport import Connection, InputPort, InputArray, InitializationConnection
from rill.engine.utils import CountDownLatch


class Network(object):
    """
    A network of comonents.

    Attributes
    ----------
    _components : dict of (str, ``rill.engine.runner.ComponentRunner``
    """

    def __init__(self, default_capacity=10, deadlock_test_interval=1):
        # FIXME: what should the name be?
        # super(Network, self).__init__(self.__class__.__name__, None)
        # self.logger = logger
        self.default_capacity = default_capacity
        self.deadlock_test_interval = deadlock_test_interval

        self.active = False  # used for deadlock detection

        self._components = OrderedDict()

        self.inports = OrderedDict()
        self.outports = OrderedDict()

        # FIXME: not used
        self.timeouts = {}

        # variables with a life-span of one run (set by reset()):

        # FIXME: get rid of this. using gevent.iwait now
        self.cdl = None

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

        self._packet_counts = None

        # FIXME: these were AtomicInteger instances, with built-in locking.
        # might not be safe to make them regular ints
        self.sends = self.receives = self.creates = self.drops = self.drop_olds = None

    def __getstate__(self):
        data = self.__dict__.copy()
        for k in ('cdl', 'runners', 'msgs'):
            data.pop(k)
        if self.runners is not None:
            runners = {}
            for name, runner in self.runners.items():
                runners[name] = runner.status
            data['runners'] = runners
        return data

    def __setstate__(self, data):
        runners = data.pop('runners', None)
        self.__dict__.update(data)
        if runners is not None:
            self._build_runners()
            for name, status in runners.items():
                self.runners[name].status = status

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

        for name, comp in self._components.items():
            comp.init()

    def add_component(self, name, comp_type, **initializations):
        """
        Instantiate a component and add it to the network.

        Parameters
        ----------
        name : str
            name of component
        comp_type : ``rill.engine.component.Component`` class
            component class to instantiate

        Returns
        -------
        ``rill.engine.component.Component``
        """
        if name in self._components:
            raise FlowError(
                "Component {} already exists in network".format(name))
        if not isclass(comp_type) or not issubclass(comp_type, Component):
            raise TypeError("comp_type must be a sub-class of Component")

        comp = comp_type(name, self)
        self.put_component(name, comp)

        for name, value in initializations.items():
            self.initialize(value, comp.port(name))
        return comp

    def remove_component(self, name):
        # FIXME: this needs some love
        assert not self.active
        component = self._components.pop(name)
        for inport in component.inports:
            if inport.is_connected() and not inport.is_static():
                for outport in inport._connection.outports:
                    self.disconnect(inport, outport)
        for outport in component.outports:
            if outport.is_connected():
                self.disconnect(outport._connection.inport, outport)

    def rename_component(self, orig_name, new_name):
        # FIXME: this needs some love
        assert not self.active
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

    def get_component_port(self, arg, index=None, kind=None):
        """
        Get a port on a component.

        Parameters
        ----------
        arg : ``rill.engine.outputport.OutputPort`` or
            ``rill.engine.inputport.InputPort`` or str
        index : int or None
            index of element, if port is an array. If None, the next available
            index is used
        kind : {'in', 'out'}

        Returns
        -------
        port : ``rill.engine.outputport.OutputPort`` or
            ``rill.engine.inputport.InputPort``
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
        sender : ``rill.engine.inputport.InputPort`` or str
        receiver : ``rill.engine.outputport.OutputPort`` or str

        Returns
        -------
        ``rill.engine.inputport.InputPort``
        """
        outport = self.get_component_port(sender, kind='out')
        inport = self.get_component_port(receiver, kind='in')

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
        sender : ``rill.engine.inputport.InputPort`` or str
        receiver : ``rill.engine.outputport.OutputPort`` or str
        """
        outport = self.get_component_port(sender, kind='out')
        inport = self.get_component_port(receiver, kind='in')
        outport._connection = None
        inport._connection.outports.remove(outport)
        if not inport._connection.outports:
            inport._connection = None

    def initialize(self, content, receiver):
        """
        Initialize an inport port with a value

        Parameters
        ----------
        content : object
        receiver : ``rill.engine.inputport.InputPort`` or str
        """
        inport = self.get_component_port(receiver, kind='in')

        if inport.name == 'IN_NULL':
            raise FlowError(
                "Cannot initialize null port: {}".format(inport))

        inport.initialize(content)

    def uninitialize(self, receiver):
        """
        Initialize an inport port with a value

        Parameters
        ----------
        receiver : ``rill.engine.inputport.InputPort`` or str
        """
        inport = self.get_component_port(receiver, kind='in')

        if inport.name == 'IN_NULL':
            raise FlowError(
                "Cannot uninitialize null port: {}".format(inport))

        return inport.uninitialize()

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
        self.runners = OrderedDict()
        for name, comp in self._components.items():
            runner = ComponentRunner(comp)
            comp._runner = runner
            self.runners[name] = runner
            runner.status = StatusValues.NOT_STARTED

    def _open_ports(self):
        errors = []
        for runner in self.runners.values():
            errors += runner.open_ports()
        if errors:
            for error in errors:
                logger.error(error)
            raise FlowError("Errors opening ports")

    def resume(self):
        self_starters = []
        for runner in self.runners.values():
            for port in runner.component.inports:
                if port.is_connected() and not port.is_static() and \
                        port._connection._queue:
                    logger.info("Existing data in connection buffer: {}",
                                args=[port])

            if runner.status in (StatusValues.TERMINATED, StatusValues.ERROR):
                runner.kill()
                continue

            elif runner.status in (StatusValues.SUSP_RECV,
                                   StatusValues.SUSP_SEND,
                                   StatusValues.DORMANT,
                                   StatusValues.ACTIVE):
                self_starters.append(runner)
            else:
                runner.auto_starting = True

                if not runner.component._self_starting:
                    for port in runner.component.inports:
                        if port.is_connected() and not port.is_static():
                            runner.auto_starting = False
                            break

                if runner.auto_starting:
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
        self_starters = []
        for runner in self.runners.values():
            runner.auto_starting = True

            if not runner.component._self_starting:
                for port in runner.component.inports:
                    if port.is_connected() and not port.is_static():
                        runner.auto_starting = False
                        break

            if runner.auto_starting:
                self_starters.append(runner)

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
            for completed in gevent.iwait(self.runners.values()):
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
        logger.error("Network has deadlocked")
        for status, objs in statuses:
            logger.error("  {:<13}{{}}".format(status), args=objs)
        raise NetworkDeadlock("Deadlock detected in Network", statuses)

    def _test_deadlocks(self):
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
        msgs : list of (status, obj) tuple
            status messages

        Returns
        -------
        is_deadlocked : bool
        """
        from rill.engine.subnet import SubNet
        # Messages are added to list, rather than written directly,
        # in case it is not a self.deadlock

        terminated = True
        for runner in self.runners.values():
            if isinstance(runner, SubNet):
                # consider components of subnets
                if not runner.list_comp_status(msgs):
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
            for comp in self.runners.values():
                comp.terminate(StatusValues.ERROR)

    def terminate(self, new_status=StatusValues.TERMINATED):
        """
        Shut down the network
        """
        # prevent deadlock testing, components will be shut down anyway
        self._abort = True
        for comp in self.runners.values():
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
        self._packet_counts[connection] += 1

    def get_components(self):
        """
        Get a dictionary of components in this network

        Returns
        -------
        dict of str, ``rill.enginge.component.Component`
        """
        return self._components

    def get_component(self, name):
        """
        Returns the requested component in this network if present or None
        if not present.

        Returns
        -------
        ``rill.enginge.component.Component`
        """
        return self._components.get(name)

    # FIXME: make private
    def put_component(self, name, comp):
        """
        Adds a component and inits it.

        Parameters
        ----------
        name : str
        comp : ``rill.enginge.component.Component`

        Returns
        -------
        comp : ``rill.enginge.component.Component` or None
            previous component, if set
        """
        from rill.engine.subnet import SubNet

        old_component = self._components.get(name)

        # FIXME: this can be found by the component by calling get_parents(). it doesn't need to be assigned here
        # find the root Network and assign it to comp.network
        network = self
        while True:
            if not isinstance(network, SubNet):
                break
            network = network.parent
        comp.network = network

        comp._init()

        self._components[name] = comp

        if old_component is not None:
            return old_component

    def export(self, internal_port_name, external_port_name):
        """
        Exports component port for connecting to other networks
        as a sub network

        Parameters
        ----------
        internal_port_name : str
                             name of internal port
        external_port_name : str
                             name of port that will be exposed

        Returns
        -------
        self : ``rill.engine.network.Network`
        """
        internal_port = self.get_component_port(internal_port_name)

        if isinstance(internal_port, InputPort):
            self.inports[external_port_name] = internal_port

        elif isinstance(internal_port, OutputPort):
            self.outports[external_port_name] = internal_port

        return self

    def to_dict(self):
        """
        Serialize network to dictionary

        Returns
        ----------------
        definition : dict
                     json representation of network
                     according to fbp json standard
        """
        definition = {
            'processes': {},
            'connections': [],
            'inports': {},
            'outports': {}
        }
        for (name, component) in self.get_components().items():
            if component.is_export: continue

            definition['processes'][name] = {
                "component": component.get_type()
            }

            for inport in component.inports:
                if not inport.is_connected(): continue

                if isinstance(inport._connection, InitializationConnection):
                    definition['connections'].append({
                        'data': inport._connection._content,
                        'tgt': {
                            'process': name,
                            'port': inport.name
                        }
                    })
                else:
                    for outport in inport._connection.outports:
                        sender = outport.component
                        if outport.component.is_export: continue

                        connection = {
                            'src': {
                                'process': sender.get_name(),
                                'port' : outport._name if outport.is_element()
                                         else outport.name
                            },
                            'tgt': {
                                'process': name,
                                'port' : inport._name if inport.is_element()
                                         else inport.name
                            }
                        }
                        if outport.is_element():
                            connection['src']['index'] = outport.index

                        if inport.is_element():
                            connection['tgt']['index'] = inport.index

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
    def from_dict(cls, definition, components):
        """
        Create network from dictionary definition

        Parameters
        ----------
        definition : dict
                     defines network structure according to fbp json standard
        components : dict
                     maps component name to ``rill.enginge.component.Component`

        Returns
        -------
        net : ``rill.enginge.network.Network`
        """
        net = cls()
        for (name, spec) in definition['processes'].items():
            net.add_component(name, components[spec['component']])

        for connection in definition['connections']:
            if connection.get('src'):
                if connection['src'].get('index'):
                    src = '{}.{}[{}]'.format(
                        connection['src']['process'],
                        connection['src']['port'],
                        connection['src']['index']
                    )

                else:
                    src = '{}.{}'.format(
                        connection['src']['process'],
                        connection['src']['port']
                    )

                if connection['tgt'].get('index'):
                    tgt = '{}.{}[{}]'.format(
                        connection['tgt']['process'],
                        connection['tgt']['port'],
                        connection['tgt']['index']
                    )

                else:
                    tgt = '{}.{}'.format(
                        connection['tgt']['process'],
                        connection['tgt']['port']
                    )

                net.connect(src, tgt)
            else:
                data = connection['data']
                tgt = '{}.{}'.format(
                    connection['tgt']['process'],
                    connection['tgt']['port']
                )

                net.initialize(data, tgt)

        for (name, inport) in definition['inports'].items():
            net.export('{}.{}'.format(inport['process'], inport['port']), name)

        for (name, outport) in definition['outports'].items():
            net.export('{}.{}'.format(
                outport['process'], outport['port']
            ), name)

        return net


def apply_network(network, inputs, outports=None):
    """
    Apply network like a function treating iips as arguments to inports
    and the values of outports as returned

    Parameters
    ----------
    network : ``rill.engine.network.Network`

    inputs : dict
             map port names to iips

    outports : array, optional
               list of outports whose results should be collected

    Returns
    -------
    outputs : dict
              map network outport names to values
    """
    from functools import reduce
    from rill.engine.subnet import SubNet
    from rill.components.basic import Capture
    from rill.decorators import inport, outport

    if not outports:
        outports = network.outports.keys()

    class ApplyNet(SubNet):
        sub_network = network
        def define(self, network): pass

    reverse_apply = lambda cls, fn: fn(cls)

    ApplyNet = reduce(reverse_apply, map(outport, outports), ApplyNet)

    ApplyNet = reduce(reverse_apply,
        map(inport, network.inports.keys()), ApplyNet)

    wrapper = Network()
    wrapper.add_component('ApplyNet', ApplyNet)

    for (port_name, value) in inputs.items():
        sub_in = 'ApplyNet.{}'.format(port_name)
        wrapper.initialize(value, sub_in)

    captures = {}
    for outport_name in outports:
        capture_name = 'Capture_{}'.format(outport_name)
        sub_out = 'ApplyNet.{}'.format(outport_name)
        capture_port_name = '{}.IN'.format(capture_name)

        capture = wrapper.add_component(capture_name, Capture)
        wrapper.connect(sub_out, capture_port_name)

        captures[outport_name] = capture

    wrapper.go()

    return {name: capture.value for (name, capture) in captures.items()}

