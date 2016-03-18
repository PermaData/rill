from collections import defaultdict, OrderedDict
from inspect import isclass
import time

from future.utils import raise_with_traceback

from rill.engine.exceptions import FlowError
from rill.engine.component import Component, logger
from rill.engine.status import StatusValues
from rill.engine.outputport import OutputPort, OutputArray
from rill.engine.inputport import Connection, InputPort, InputArray
from rill.engine.utils import CountDownLatch

import gevent

try:
    basestring  # PY2
except NameError:
    basestring = str  # PY3


class Network(object):
    """
    The abstract class which all flow networks extend directly or indirectly. A
    specific flow network must override the `define()` method, which
    is written using the <i>mini-language</i> (actually just highly restricted
    Java invoking the `protected` methods of self class). The
    mini-language specifies what threads are to be created using which
    self._components, and what connections are established between the ports of those
    self._components.
    """

    def __init__(self, default_capacity=10):
        # FIXME: what should the name be?
        # super(Network, self).__init__(self.__class__.__name__, None)
        self.logger = logger
        self.default_capacity = default_capacity
        self.deadlock_test = True

        self.active = False  # used for deadlock detection

        self._components = OrderedDict()
        # FIXME: get rid of this. using gevent.iwait now
        self.cdl = None
        self.timeouts = {}

        # globals is a global synchronized_map intended for real global use.
        # It is not intended for component-to-component communication.
        self.globals = {}
        self.deadlock = False

        # holds the first error raised
        self._error = None
        self._abort = False
        # for use by list_comp_status(). here for post-mortem inspection
        self.msgs = None

        self._packet_counts = defaultdict(int)

        # FIXME: these were AtomicInteger instances, with built-in locking.
        # might not be safe to make them regular ints
        self.sends = self.receives = self.creates = self.drops = self.drop_olds = 0

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
        from rill.engine.subnet import SubNet

        if name in self._components:
            raise FlowError(
                "Component {} already exists in network".format(name))
        if not isclass(comp_type) or not issubclass(comp_type, Component):
            raise TypeError("comp_type must be a sub-class of Component")

        comp = comp_type(name, self)
        self.put_component(name, comp)

        # FIXME: this can be found by the component by calling get_parents(). it doesn't need to be assigned here
        # find the root Network and assign it to comp.network
        network = self
        while True:
            if not isinstance(network, SubNet):
                break
            network = network.mother
        comp.network = network

        comp.status = StatusValues.NOT_STARTED
        comp.init()
        for name, value in initializations.items():
            self.initialize(value, comp.port(name))
        return comp

    def component(self, name):
        """
        Parameters
        ----------
        name : str
            name of component

        Returns
        -------
        ``rill.engine.component.Component``
        """
        comp = self.get_component(name)
        if comp is None:
            raise FlowError("Reference to unknown component " + name)
        return comp

    def get_component_port(self, arg, index=None, kind=None):
        """
        Parameters
        ----------
        arg : ``rill.engine.outputport.OutputPort`` or
            ``rill.engine.inputport.InputPort`` or str

        Returns
        -------
        port : ``rill.engine.outputport.OutputPort`` or
            ``rill.engine.inputport.InputPort``
        """
        if isinstance(arg, (OutputPort, OutputArray, InputPort, InputArray)):
            port = arg
        else:
            if isinstance(arg, (tuple, list)):
                comp_name, port_name = arg
            elif isinstance(arg, basestring):
                comp_name, port_name = arg.split('.')
            else:
                raise TypeError(arg)

            comp = self.component(comp_name)
            port = comp.port(port_name, kind=kind)

        if port.is_array():
            port = port.get_element(index, create=True)

        if kind == 'input' and not isinstance(port, InputPort):
            raise TypeError("Expected InputPort, got {}".format(type(port)))
        elif kind == 'output' and not isinstance(port, OutputPort):
            raise TypeError("Expected OutputPort, got {}".format(type(port)))

        return port

    def connect(self, sender, receiver, connection_capacity=None,
                count_packets=False):
        """
        Connect an output port of one component to an input port of another,
        specifying a connection capacity.

        Parameters
        ----------
        sender : ``rill.engine.inputport.InputPort`` or str
        receiver : ``rill.engine.outputport.OutputPort`` or str

        Returns
        -------
        ``rill.engine.inputport.InputPort``
        """
        outport = self.get_component_port(sender, kind='output')
        inport = self.get_component_port(receiver, kind='input')

        if connection_capacity is None:
            connection_capacity = self.default_capacity

        if inport._connection is None:
            inport._connection = Connection()
        inport._connection.connect(inport, outport, connection_capacity)
        return inport

    def initialize(self, content, receiver):
        """
        Initialize an inport port with a value

        Parameters
        ----------
        content : object
        receiver : ``rill.engine.inputport.InputPort`` or str
        """
        inport = self.get_component_port(receiver, kind='input')

        if inport.name == 'NULL':
            raise FlowError(
                "Cannot initialize NULL port: {}".format(inport))

        inport.initialize(content)

    def go(self):
        """
        Execute the network
        """
        self.receives = 0
        self.sends = 0
        self.creates = 0
        self.drops = 0
        self.drop_olds = 0

        now = time.time()

        try:
            self.active = True
            self.initiate()
            self.wait_for_all()

        except FlowError as e:
            s = "Flow Error :" + str(e)
            logger.info("Network: " + s)
            raise
        finally:
            self.active = False

        duration = time.time() - now
        logger.info("Run complete.  Time: %.02f seconds" % duration)
        logger.info("Counts:")
        logger.info(" creates:        %d", self.creates)
        logger.info(" drops (manual): %d", self.drops)
        logger.info(" drops (old):    %d", self.drop_olds)
        logger.info(" sends:          %d", self.sends)
        logger.info(" receives:       %d", self.receives)

        if self._error is not None:
            # throw the exception which caused the network to stop
            # FIXME: figure out how to re-raise exception with traceback
            raise_with_traceback(self._error)

    # FIXME: get rid of this:  we don't need the CDL anymore...
    # may be useful if we want to support threading systems other than gevent
    def indicate_terminated(self, comp):
        # -- synchronized (comp)
        comp.status = StatusValues.TERMINATED
        # -- end
        self.logger.debug(comp.get_name() + ": Terminated")
        self.cdl.count_down()
        # net.interrupt()

    def initiate(self):
        """
        Go through components opening ports, and activating those which are
        self-starting (have no input connections)
        """
        freq = 0.5 if self.deadlock_test else None
        self.cdl = CountDownLatch(len(self._components), freq)

        errors = []
        for comp in self._components.values():
            errors += comp._open_ports()
        if errors:
            for error in errors:
                logger.error(error)
            raise FlowError("Errors opening ports")

        self_starters = []
        for comp in self._components.values():
            comp.auto_starting = True

            if not comp._self_starting:
                for port in comp.inports.ports():
                    if port.is_connected() and not port.is_static():
                        comp.auto_starting = False
                        break

            if comp.auto_starting:
                self_starters.append(comp)

        for comp in self_starters:
            comp._activate()

    def interrupt_all(self):
        """
        Interrupt all components
        """
        logger.warning("*** Crashing whole application!")

        # FIXME: overkill
        sys.exit(0)  # trying this - see if more friendly!

    def wait_for_all(self):
        """
        Test if network as a whole has terminated
        """
        possible_deadlock = False

        # freq = .5  # check every .5 second
        # while True:
        #     res = True
        #     # GenTraceLine("Starting await")
        #     try:
        #         self.cdl.start()
        #         print "cdl get"
        #         res = self.cdl.get()
        #         print "cdl check", res
        #     # FIXME:
        #     # except InterruptedException as e:
        #     except Exception as e:
        #         raise e
        #         # raise FlowError("Network " + self.get_name() + " interrupted")
        #     if res:
        #         break

        try:
            for completed in gevent.iwait(self._components.values()):
                logger.debug("Component completed: {}".format(completed))
                # if an error occurred, skip deadlock testing
                if self._error is not None:
                    break

                # if the network was aborted, skip deadlock testing
                if self._abort:
                    break
        except gevent.hub.LoopExit:
            self.msgs = []
            # if True, it is a deadlock
            if self.list_comp_status(self.msgs):
                logger.error("Network has deadlocked")
                for status, objs in self.msgs:
                    logger.error("  {:<13}{{}}".format(status), args=objs)
            raise

            #
            # # time elapsed
            # if not self.deadlock_test:
            #     continue
            #
            # # enabled
            #
            # # FIXME: figure this out
            # # self.test_timeouts(freq)
            #
            # if self.active:
            #     self.active = False  # reset flag every 1/2 sec
            # elif not possible_deadlock:
            #     possible_deadlock = True
            # else:
            #     self.deadlock = True  # well, maybe
            #     # so test state of components
            #     self.msgs = []
            #     # add in case self.msgs are printed
            #     self.msgs.append("Network has deadlocked")
            #     # if True, it is a deadlock
            #     if self.list_comp_status(self.msgs):
            #         #          interrupt_all()
            #         for m in self.msgs:
            #             logger.info(m)
            #         # FlowError.Complain("Deadlock detected")
            #         logger.info("*** Deadlock detected in Network ")
            #         # terminate the net instead of crashing the application
            #         self.terminate()
            #         # tell the caller a self.deadlock occurred
            #         raise FlowError("Deadlock detected in Network")
            #     # one or more components haven't started or are in a wait
            #     self.deadlock = False
            #     possible_deadlock = False  # while

            # print "JOIN"
            # for comp in self._components.values():
            #     comp.join()

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
        for comp in self._components.values():
            if isinstance(comp, SubNet):
                # consider components of subnets
                if not comp.list_comp_status(msgs):
                    return False
            else:
                status = comp.status
                if status in (StatusValues.ACTIVE, StatusValues.LONG_WAIT):
                    return False

                if not status == StatusValues.TERMINATED:
                    terminated = False

                if status == StatusValues.SUSP_RECV:
                    objs = [comp._curr_conn]
                elif status == StatusValues.SUSP_SEND:
                    objs = [comp._curr_outport._connection]
                else:
                    objs = [comp]

                msgs.append((status, objs))

        return not terminated

    def test_timeouts(self, freq):
        for t in self.timeouts.values():
            t.decrement(freq)  # if negative, complain

    def signal_error(self, e):
        """
        Handle errors in the network.
        """
        # only react to the first error, the others presumably are inherited
        # errors
        if self._error is None:
            assert isinstance(e, Exception)
            # set the error field to let go() raise the exception
            self._error = e
            # terminate the network's components
            for comp in self._components.values():
                comp.terminate(StatusValues.ERROR)

    def terminate(self, new_status=StatusValues.TERMINATED):
        """
        Shut down the network
        """
        # prevent deadlock testing, components will be shut down anyway
        self._abort = True
        for comp in self._components.values():
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

    # FIXME: remove
    def get_components(self):
        """
        Get a dictionary of components in this network

        Returns
        -------
        dict of str, ``rill.enginge.component.Component`
        """
        return self._components

    # FIXME: remove
    def get_component(self, name):
        """
        Returns the requested component in self network if present, Noneif not present.

        Returns
        -------
        ``rill.enginge.component.Component`
        """
        return self._components.get(name)

    # FIXME: remove
    def put_component(self, name, comp):
        """
        Adds a component, if necessary replacing the old component.

        Parameters
        ----------
        name : str
        comp : ``rill.enginge.component.Component`

        Returns
        -------
        comp : ``rill.enginge.component.Component` or None
            previous component, if set
        """
        old_component = self._components.get(name)
        self._components[name] = comp
        return old_component
