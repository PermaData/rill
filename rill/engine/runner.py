import logging
from threading import Condition

from gevent import Greenlet, GreenletExit
from gevent.lock import RLock
from termcolor import colored

from rill.engine.utils import LogFormatter
from rill.engine.status import StatusValues
from rill.engine.exceptions import FlowError, ComponentException


class ComponentRunner(Greenlet):
    """
    A ``ComponentRunner`` is a specialized ``gevent.Greenlet`` sub-class
    that manages  a ``Component`` instance during the execution of a
    ``Network``.

    While the ``Component`` class provides the public API for defining the
    behavior of a component subclass, the ``ComponentRunner`` provides the
    private API used by the Network and port classes.
    """
    logger = LogFormatter(logging.getLogger("component.runner"), {})

    def __init__(self, component):
        """
        Parameters
        ----------
        component : ``rill.engine.component.Component``
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
        self.has_run = False

        # used when evaluating component statuses for deadlocks
        self.curr_conn = None  # set externally
        self.curr_outport = None  # set externally

        # FIXME: allow this value to be set.  should we read it from the Network, or do we need per-component control?
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

    # Ports --

    def open_ports(self):
        """
        Open all ports.

        Returns
        -------
        list of Exception
        """
        errors = []
        try:
            for port in self.component.ports:
                port.open()
        except FlowError as e:
            errors.append(str(e))
        return errors

    def close_ports(self):
        """
        Close all ports.
        """
        for port in self.component.ports:
            port.close()

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

    def activate(self):
        """
        Called from other parts of the system to activate this Component.

        This will start its thread or will notify it to continue.
        """
        if self.is_terminated():
            return
        if not self.active():
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

    @property
    def self_starting(self):
        if self.has_run:
            return False
        if self.component._self_starting:
            return True
        for port in self.component.inports:
            if port.is_connected() and not port.is_static():
                return False
        return True

    @property
    def must_run(self):
        return not self.has_run and self.component._must_run

    def is_all_drained(self):
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
                    conns = [c._connection for c in self.component.inports
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
            if self.component.ports.IN_NULL.is_connected():
                self._null_input = self.component.ports.IN_NULL
                self._null_input.receive_once()
            if self.component.ports.OUT_NULL.is_connected():
                self._null_output = self.component.ports.OUT_NULL

            self_started = self.self_starting

            while (self_started or
                       not self.is_all_drained() or
                           self._null_input is not None or
                       (self.is_all_drained() and self.must_run) or
                           self.component.stack_size() > 0):
                self._null_input = None
                self.has_run = True

                # FIXME: added has_error to allow this loop to exit if another
                # thread calls parent.signal_error() to set our status to ERROR
                if self.is_terminated() or self.has_error():
                    break

                for value in self.component.inports:
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
                # - is_all_drained only checks Connections.
                # - tests succeed if we simply hard-wire InitializationConnection to always open
                # - it ensures that it yields a new result when component is re-activated
                for ip in self.component.inports:
                    if ip.is_static():
                        ip.close()
                        # if (not icp.is_closed()):
                        #  raise FlowError("Component deactivated with IIP port not closed: " + self.get_name())
                        #

                if self_started:
                    break

                if self.is_all_drained() and self.component.stack_size() == 0:
                    break  # while

            if self._null_output is not None:
                # p = create("")
                # self._null_output.send(p)
                self._null_output.close()

            self.close_ports()

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
                    self.close_ports()
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
            self.close_ports()

    def active(self):
        return bool(self)
