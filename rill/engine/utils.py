import os
import logging
import gevent
from gevent.lock import RLock
from termcolor import colored

is_patched = False


def patch():
    global is_patched
    if is_patched or os.environ.get('RILL_SKIP_GEVENT_PATCH', False):
        return
    from gevent import monkey
    print("Performing gevent monkey-patching")
    monkey.patch_all()
    is_patched = True


class LogFormatter(logging.LoggerAdapter):
    def setLevel(self, level):
        self.logger.setLevel(level)

    @classmethod
    def _format(cls, obj, include_count=True):
        from rill.engine.component import Component
        from rill.engine.port import BasePort
        from rill.engine.outputport import OutputPort
        from rill.engine.inputport import InputPort, Connection
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
            arrow = ' => '
            return s1 + arrow + s2, n1 + n2 + len(arrow)
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
        import gevent
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


class CountDownLatch(gevent.Greenlet):
    def __init__(self, count, freq=0.1):
        super(CountDownLatch, self).__init__()
        assert count >= 0
        self._lock = RLock()
        self._count = count
        self._freq = freq

    def count_down(self):
        with self._lock:
            assert self._count > 0

            self._count -= 1
            # Return inside lock to return the correct value,
            # otherwise another thread could already have
            # decremented again.
            return self._count

    def get_count(self):
        return self._count

    def _run(self):
        while self._count > 0:
            gevent.sleep(self._freq)
        return True
