from collections import deque, Counter
import logging
import re
from abc import ABCMeta, abstractmethod
from threading import Condition

from future.utils import with_metaclass, raise_from
import gevent
from gevent import Greenlet, GreenletExit
from gevent.lock import RLock
from termcolor import colored

from rill.engine.status import StatusValues
from rill.engine.port import BasePort
from rill.engine.outputport import OutputPort, OutputArray, OutputCollection
from rill.engine.inputport import InputPort, InputArray, InputCollection, \
    Connection
from rill.engine.packet import Packet, Chain
from rill.engine.exceptions import FlowError, ComponentException
from rill.decorators import *

PORT_NAME_REG = r"^([a-zA-Z][_a-zA-Z0-9]*)(?:\[(\d+)\])?$"


class Adapter(logging.LoggerAdapter):
    @classmethod
    def _format(cls, obj, include_count=True):
        if isinstance(obj, Component):
            comp = obj
            port_name = None
        elif isinstance(obj, BasePort):
            comp = obj.component
            port_name = obj.name
            port_attrs = ['dark', 'bold'] if obj.is_closed() else []
        elif isinstance(obj, Connection):
            if obj.outport is not None:
                s1, n1 = cls._format(obj.outport, include_count=False)
            else:
                s1 = colored('<unset>', 'cyan')
                n1 = len('<unset>')
            s2, n2 = cls._format(obj.inport)
            return s1 + ' => ' + s2, n1 + n2 + 2
        elif isinstance(obj, gevent.greenlet.greenlet):
            # FIXME: make better
            comp = obj
            port_name = None
        else:
            raise TypeError(obj)

        comp_name = str(comp)
        s = colored(comp_name, 'cyan')
        n = len(comp_name)
        if port_name:
            s += '.' + colored(port_name, 'magenta', attrs=port_attrs)
            n += len(port_name) + 1

        if include_count and isinstance(obj, BasePort):
            if isinstance(obj, InputPort):
                count = ' ({})'.format(obj.upstream_count())
            elif isinstance(obj, OutputPort):
                count = ' ({})'.format(obj.downstream_count())
            else:
                raise TypeError(obj)
            s += colored(count, 'yellow', attrs=port_attrs)
            n += len(count)
        return s, n

    @classmethod
    def _format_args(cls, args):
        results = []
        for arg in args:
            try:
                results.append(cls._format(arg)[0])
            except TypeError:
                results.append(arg)
        return tuple(results)

    def process(self, msg, kwargs):
        thread = gevent.getcurrent()
        # use explicit component if it was provided
        comp = kwargs.pop('component', None)
        if comp is not None:
            show_thread = comp != thread
        else:
            comp = thread
            show_thread = False

        args = kwargs.pop('args', None)
        if args:
            msg = msg.format(*self._format_args(args))

        message, n = self._format(kwargs.pop('port', comp))
        # FIXME: get maximum port name length:
        pad = max(15 - n, 0)
        # can't use .format to left justify because of the color codes
        message += ' ' * pad
        section = kwargs.pop('section', None)
        if section:
            message += ' {} :'.format(section)
        message += ' {}'.format(msg)
        if show_thread:
            message += colored(" (on thread {})".format(thread), 'yellow')

        return message, kwargs


_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)
logger = Adapter(_logger, {})

# FIXME: put these on InputInterface
inport._static_port_type = InputPort
inport._array_port_type = InputArray

# FIXME: put these on OutputPort
outport._static_port_type = OutputPort
outport._array_port_type = OutputArray


class Component(with_metaclass(ABCMeta, object)):
    _inport_definitions = []
    _outport_definitions = []
    _self_starting = False
    _must_run = False

    # same as module-level logger, but provided here for convenience
    logger = logger

    def __init__(self, name, parent):
        assert name is not None
        self._name = name

        # cached values
        self._full_name = None
        self._parents = None

        # the component's immediate network parent: used for subnet support
        self.parent = parent
        # the root network
        # FIXME: this is set by the Network, but should be set by the component using get_parents()
        self.network = None

        # set by the network
        self._runner = None

        # All the input ports are stored here, keyed by name.
        self.inports = None
        # All the output ports are stored here, keyed by name.
        self.outports = None

        # a stack available to each component
        self._stack = deque()

        # count of packets owned by this component.
        # Whenever the component deactivates, the count must be zero.
        self._packet_count = 0

    def __str__(self):
        return self.get_full_name()

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.get_full_name())

    def __getstate__(self):
        data = self.__dict__.copy()
        for k in ('_runner',):
            data.pop(k)
        return data

    def __setstate__(self, data):
        for key, value in data.items():
            self.__dict__[key] = value
        self.logger = logger

    def _create_port(self, port):
        if port.args.get('fixed_size') and not port.array:
            raise ValueError(
                "{}.{}: @{} specified fixed_size but not array".format(
                    self, port.args['name'],
                    port.__class__.__name__))
        if port.array:
            ptype = port._array_port_type
        else:
            ptype = port._static_port_type
        return ptype(self, **port.args)

    def _init(self):
        """
        Initialize internal attributes from decorators.
        """
        self._must_run = must_run.get(self.__class__)
        self._self_starting = self_starting.get(self.__class__)

        inports = inport.get(self.__class__)
        outports = outport.get(self.__class__)
        # Enforce unique names between input and output ports. This ensures
        # that _FunctionComponent functions can receive a named argument per
        # port
        names = [p.args['name'] for p in inports + outports]
        dupes = [x for x, count in Counter(names).items() if count > 1]
        if dupes:
            self.error(
                "Duplicate port names: {}".format(', '.join(dupes)))

        self.inports = InputCollection(
            self, [self._create_port(p) for p in inports + [inport('NULL')]])
        self.outports = OutputCollection(
            self, [self._create_port(p) for p in outports + [outport('NULL')]])

    def get_parents(self):
        """
        Returns
        -------
        list of ``rill.engine.network.SubNet``
        """
        from rill.engine.subnet import SubNet
        if self._parents is None:
            parent = self.parent
            parents = []
            while True:
                if parent is None:
                    break
                if not isinstance(parent, SubNet):
                    break
                parents.append(parent)
                parent = parent.parent
            parents.reverse()
            self._parents = parents
        return self._parents

    def get_children(self):
        """
        Returns
        -------
        list of ``Component``
        """
        return []

    # FIXME: property
    def get_name(self):
        """
        Name of the component.

        Returns
        -------
        str
        """
        return self._name

    # FIXME: cached property
    def get_full_name(self):
        """
        Name of the component, including all parent components.

        Returns
        -------
        str
        """
        if self._full_name is None:
            self._full_name = '.'.join(
                [x.get_name() for x in self.get_parents()] + [self.get_name()])
        return self._full_name

    def init(self):
        pass

    @abstractmethod
    def execute(self):
        """Main run method

        Must be implemented by all sub-classes.
        """
        raise NotImplementedError

    def port(self, port_name, kind=None):
        """
        Get a port on the component.

        Parameters
        ----------
        port_name : str
        kind : {'in', 'out'} or None

        Returns
        -------
        port : ``rill.engine.outputport.OutputPort`` or
               ``rill.engine.outputport.OutputArray`` or
               ``rill.engine.inputport.InputPort`` or
               ``rill.engine.inputport.InputArray``
        """
        reg = re.compile(PORT_NAME_REG)
        m = reg.match(port_name)

        if not m:
            raise FlowError("Invalid port name: " + port_name)

        port_name, index = m.groups()
        if index is not None:
            index = int(index)

        if kind == 'in':
            try:
                port = self.inports[port_name]
            except KeyError as err:
                raise_from(FlowError(str(err)), err)
        elif kind == 'out':
            try:
                port = self.outports[port_name]
            except KeyError as err:
                raise_from(FlowError(str(err)), err)
        elif not kind:
            try:
                port = self.outports[port_name]
            except KeyError:
                try:
                    port = self.inports[port_name]
                except KeyError:
                    raise FlowError("{} is not a registered input or "
                                    "output port".format(port_name))
        else:
            raise TypeError(kind)

        if index is not None:
            if not port.is_array():
                raise FlowError("Element {} specified for non-array "
                                "port {}".format(index, port))
            port = port.get_element(index, create=True)

        return port

    def error(self, msg, errtype=FlowError):
        raise errtype("{}: {}".format(self, msg))

    # Packets --

    def validate_packet(self, packet):
        if not isinstance(packet, Packet):
            raise ComponentException(
                "Expected a Packet instance, got {}".format(type(packet)))

        if self is not packet.owner:
            raise ComponentException(
                "Packet not owned by current component, "
                "or component has terminated (owner is %s)" % packet.owner)

    def create(self, contents):
        """
        Create a Packet and set its owner to this component.

        Returns
        -------
        packet : ``rill.engine.packet.Packet``
        """
        self.logger.debug("Creating packet: " + repr(contents))
        self.network.creates += 1
        # FIXME: this could be nicer
        if contents in (Packet.Type.OPEN, Packet.Type.CLOSE):
            type = contents
            contents = ""
        else:
            type = Packet.Type.NORMAL
        return Packet(contents, self, type)

    def drop(self, packet):
        """
        Drop packet and clear owner reference.

        Parameters
        ----------
        packet : ``rill.engine.packet.Packet``

        Returns
        -------
        contents : object
            packet contents
        """
        self.logger.debug("Dropping packet: " + str(packet))
        self.network.drops += 1
        self.validate_packet(packet)
        packet.clear_owner()
        return packet.get_contents()

    def get_packet_count(self):
        return self._packet_count

    # Packet stack --

    def push(self, packet):
        """
        Push onto the stack and clear owner reference.

        Parameters
        ----------
        packet : ``rill.engine.packet.Packet``
        """
        self.validate_packet(packet)
        self._stack.push(packet)
        packet.clear_owner()

    def pop(self):
        """
        Pop a Packet off the stack or return None if empty.

        Returns
        -------
        ``rill.engine.packet.Packet``
        """
        if len(self._stack) == 0:
            return None

        packet = self._stack.pop()
        packet.set_owner(self)
        return packet

    def stack_size(self):
        """
        Return current size of the stack.

        Returns
        -------
        int
        """
        return len(self._stack)

    # Packet chains --

    def attach(self, packet, name, subpacket):
        """
        Attach a Packet to a named Chain of Packets.

        A Packet may have multiple named Chains attached to it.
        Since Packets are attached to Chains, as well as Chains to Packets,
        this results in an alternation of Packets and Chains, creating a tree structure.

        Parameters
        ----------
        packet : ``rill.engine.packet.Packet``
            the packet to which to attach `subpacket`
        name : str
            name of chain
        subpacket : ``rill.engine.packet.Packet``
        """
        self.validate_packet(subpacket)

        p = packet
        while p.isinstance(p.owner, Packet):
            if p == subpacket:
                self.error("Loop in packet tree structure")
            p = p.owner

        if p == subpacket:
            self.error("Loop in packet tree structure")

        if p.owner is not self:
            self.error("Packet not owned (directly or indirectly) "
                              "by current component")

        chain = packet.chains.get(name)
        if chain is None:
            chain = Chain(name)
            packet.chains[name] = chain

        subpacket.set_owner(packet)
        chain.members.append(subpacket)

    def detach(self, packet, name, subpacket):
        """
        Detach a packet from named chain.

        Parameters
        ----------
        packet : ``rill.engine.packet.Packet``
        name : str
          name of Chain
        subpacket : ``rill.engine.packet.Packet``
        """
        if not isinstance(subpacket, Packet):
            self.error("Expected a Packet instance, got {}".format(
                type(subpacket)))

        root = packet.get_root()
        # FIXME: get_root returns a Component, but this code seems to expect a Packet
        if root.owner is not self:
            self.error(
                "Packet not owned (directly or indirectly) "
                "by current component")

        if packet.chains is None or packet.chains.get(name) is None:
            self.error(
                "Named chain does not exist: {}".format(name))

        chain = packet.chains.get(name)
        if not chain.members.remove(subpacket):
            self.error("Object not found on chain {}".format(name))
        subpacket.set_owner(self)
        return

    # Globals --

    def put_global(self, key, value):
        """
        Set a global value.

        Use this carefully as global data creates a fat coupling.

        Parameters
        ----------
        key : hashable
        value : object

        Returns
        -------
        prev_value : object or None
            previous value
        """
        # FIXME: lock here?
        res = self.network.globals.get(key)
        self.network.globals[key] = value
        return res

    def get_global(self, key):
        """
        Get a global value.

        Parameters
        ----------
        key : hashable

        Returns
        -------
        value : object or None
        """
        # FIXME: lock here?
        return self.network.globals.get(key)


class ComponentRunner(Greenlet):
    logger = Adapter(logging.getLogger("component.runner"), {})

    def __init__(self, component):
        """
        Parameters
        ----------
        component : ``Component``
        """
        Greenlet.__init__(self)

        self.component = component

        self._lock = RLock()
        self._can_go = Condition(self._lock)

        # the automatic input port named "NULL"
        self._null_input = None
        # the automatic output port named "NULL"
        self._null_output = None

        self.network = component.network
        self._must_run = component._must_run
        self._self_starting = component._self_starting
        self.auto_starting = False

        self.timeout = None

        # used when evaluating component statuses for deadlocks
        self._curr_conn = None  # InputInterface
        self._curr_outport = None  # OutputPort

        self.ignore_packet_count_error = False
        self._status = StatusValues.NOT_STARTED

    def __str__(self):
        return self.component.get_full_name()

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__,
                           self.component.get_full_name())

    def __getstate__(self):
        data = self.__dict__.copy()
        for k in ('_lock', '_can_go'):
            data.pop(k)
        return data

    def error(self, msg, errtype=FlowError):
        self.component.error(msg, errtype)

    # FIXME: figure out the logging stuff
    def trace_funcs(self, msg, section='funcs'):
        self.logger.debug(msg)
        # self.parent.trace_funcs(self, msg)

    def trace_locks(self, msg, **kwargs):
        self.logger.debug(msg, section='locks', **kwargs)
        # self.parent.trace_locks(self, msg)

    # Packets --

    def create(self, data):
        return self.component.create(data)

    def drop(self, packet):
        return self.component.drop(packet)

    # Ports --

    @property
    def inports(self):
        return self.component.inports

    @property
    def outports(self):
        return self.component.outports

    def _open_ports(self):
        """
        Open all ports.

        Returns
        -------
        list of Exception
        """
        errors = []
        for container in [self.inports, self.outports]:
            try:
                container.open()
            except FlowError as e:
                errors.append(str(e))
        return errors

    def _close_ports(self):
        """
        Close all ports.
        """
        self.outports.close()
        self.inports.close()

    # Statuses --

    @property
    def status(self):
        """
        Get the component's current status.

        Returns
        -------
        status : str
            one of ``rill.engine.status.StatusValues``
        """
        return self._status

    @status.setter
    def status(self, new_status):
        if new_status != self._status:
            self.logger.debug(
                "Changing status {} -> {}".format(self._status, new_status),
                component=self)
            self._status = new_status

    def is_terminated(self):
        """
        Return whether the component has terminated.

        Returns
        -------
        bool
        """
        return self.status == StatusValues.TERMINATED

    def has_error(self):
        """
        Return whether the component has an error.

        Returns
        -------
        bool
        """
        return self.status == StatusValues.ERROR

    def terminate(self, new_status=StatusValues.TERMINATED):
        """
        Terminate the component.

        Parameters
        ----------
        new_status : int
            one of ``rill.engine.status.StatusValues`` (usually "TERMINATED" or
            "ERROR")
        """
        for child in self.component.get_children():
            child._runner.terminate(new_status)
        self.logger.debug("Terminated", component=self)
        self.status = new_status
        # self.parent.indicate_terminated(self)
        # FIXME: Thread.interrupt()

    # def long_wait_start(self, intvl):  # interval in seconds!
    #     self.timeout = TimeoutHandler(intvl, self)
    #     self._addto_timeouts(self.timeout)
    #
    # def _addto_timeouts(self, t):
    #     """
    #     t : TimeoutHandler
    #     """
    #     # synchronized (network)
    #     self.network.timeouts[self] = t
    #     self.status = StatusValues.LONG_WAIT
    #
    # def long_wait_end(self):
    #     self.timeout.dispose(self)

    def _activate(self):
        """
        Called from other parts of the system to activate this Component.

        This will start its thread or will notify it to continue.
        """
        if self.is_terminated():
            return
        if not self.started():
            self.start()
        else:
            self.trace_locks("act - lock")
            try:
                with self._lock:
                    if self.status in (StatusValues.DORMANT,
                                       StatusValues.SUSP_FIPE):
                        self._can_go.notify()
                        self.trace_locks("act - signal")
            except GreenletExit as e:
                return
            finally:
                self.trace_locks("act - unlock")

    def _await_actionable_input_state(self):
        """
        Wait for packets to arrive or for all ports to be drained.

        Returns
        -------
        all_drained : bool
            all input ports are drained
        """
        try:
            self.trace_locks("input states - acquired")
            with self._lock:
                while True:
                    conns = [c._connection for c in self.inports.ports()
                             if c.is_connected()]
                    all_drained = all(c.is_drained() for c in conns)
                    has_data = any(not c.is_empty() for c in conns)

                    if has_data or all_drained:
                        return all_drained

                    self.status = StatusValues.DORMANT
                    self.trace_funcs("Dormant")

                    # wait for something to change
                    self.trace_locks("input state - wait")
                    self._can_go.wait()
                    self.trace_locks("input state - wait ended")

                    self.status = StatusValues.ACTIVE
                    self.trace_funcs("Active")
        finally:
            self.trace_locks("input states - unlocked")  # while

    # override of Greenlet._run
    def _run(self):
        try:
            if self.is_terminated() or self.has_error():
                if self._lock._is_owned():
                    self._lock.release()
                    self.trace_locks("run - unlock")
                return

            self.status = StatusValues.ACTIVE
            self.trace_funcs("Started")
            if self.inports.NULL.is_connected():
                self._null_input = self.inports.NULL
                self._null_input.receive_once()
            if self.outports.NULL.is_connected():
                self._null_output = self.outports.NULL

            if self._self_starting:
                self.auto_starting = True
            else:
                all_drained = self._await_actionable_input_state()

            while (self.auto_starting or
                       not all_drained or
                           self._null_input is not None or
                       (all_drained and self._must_run) or
                           self.component.stack_size() > 0):
                self._null_input = None
                # FIXME: added has_error to allow this loop to exit if another
                # thread calls parent.signal_error() to set our status to ERROR
                if self.is_terminated() or self.has_error():
                    break

                for value in self.inports.ports():
                    if value.is_static():
                        value.open()

                self.trace_funcs(colored("Activated", attrs=['bold']))

                self.component.execute()

                self.trace_funcs(colored("Deactivated", attrs=['bold']))

                if self.component._packet_count != 0 and not self.ignore_packet_count_error:
                    self.trace_funcs(
                        "deactivated holding {} packets".format(
                            self.component._packet_count))
                    self.error(
                        "{} packets not disposed of during component "
                        "deactivation".format(self.component._packet_count))

                # FIXME: what is the significance of closing and reopening the InitializationConnections?
                # - _await_actionable_input_state only checks Connections.
                # - tests succeed if we simply hard-wire InitializationConnection to always open
                # - it ensures that it yields a new result when component is re-activated
                for ip in self.inports.ports():
                    if ip.is_static():
                        ip.close()
                        # if (not icp.is_closed()):
                        #  raise FlowError("Component deactivated with IIP port not closed: " + self.get_name())
                        #

                self._must_run = False
                self._self_starting = False

                if self.auto_starting:
                    break

                all_drained = self._await_actionable_input_state()

                if all_drained and self.component.stack_size() == 0:
                    break  # while

            if self._null_output is not None:
                # p = create("")
                # self._null_output.send(p)
                self._null_output.close()

            self._close_ports()

            if self.component.stack_size() != 0:
                self.error("Compodenent terminated with stack not empty")
            self.component.parent.indicate_terminated(self)

        except ComponentException as e:
            # FIXME:
            if e.get_value() > 0:
                self.trace_funcs("Component exception: " + e.get_value())
                if e.get_value() > 999:
                    self.logger.error(
                        "terminated with exception code " + e.get_value())

                    if self.parent is not None:
                        # record the error and terminate siblings
                        self.parent.signal_error(e)
                    self._close_ports()
            raise GreenletExit()

        except Exception as e:
            # don't tell the parent if we are already in the ERROR or TERMINATE state
            # because then the parent told us to terminate
            if self.is_terminated() or self.has_error():
                # if we are in the TERMINATED or ERROR state we terminated
                # intentionally
                return

            import traceback
            traceback.print_exc()

            self.status = StatusValues.ERROR

            if self.component.parent is not None:
                # record the error and terminate siblings
                self.component.parent.signal_error(e)
            self._close_ports()

    def started(self):
        return bool(self)


class _FunctionComponent(with_metaclass(ABCMeta, Component)):
    """
    Base class for components created from functions via
    ``rill.engine.decorators.component``
    """
    _pass_context = False

    @abstractmethod
    def _execute(self, *args):
        raise NotImplementedError

    def execute(self):
        # FIXME: use named arguments to allow user to change order? could
        # inspect the names and if they're all accounted for, use names,
        # otherwise, fall back to ordered args.
        args = []
        if self._pass_context:
            args.append(self)
        for port in self.inports.root_ports():
            if port.auto_receive:
                args.append(port.receive_once())
            else:
                args.append(port)
        args.extend(self.outports.root_ports())
        self._execute(*args)
