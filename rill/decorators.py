from past.builtins import basestring

from rill.utils import ProxyAnnotation, FlagAnnotation
from rill.engine.portdef import InputPortDefinition, OutputPortDefinition

__all__ = ['inport', 'outport', 'must_run', 'self_starting', 'component']


class inport(ProxyAnnotation):
    multi = True
    attribute = 'inport_definitions'
    proxy_type = InputPortDefinition


class outport(ProxyAnnotation):
    multi = True
    attribute = 'outport_definitions'
    proxy_type = OutputPortDefinition


# FIXME: could be a nice feature to make users add an explanation, e.g.
#  @must_run("file must always be opened for writing so that data is reset")
class must_run(FlagAnnotation):
    """
    A component decorated with `must_run` is activated once even if all
    upstream packets are drained.
    """
    default = True


class self_starting(FlagAnnotation):
    """
    A component decorated with `self_starting` does not need to receive any
    upstream packets to begin sending packets.
    """
    default = False


ANNOTATIONS = (
    inport,
    outport,
    must_run,
    self_starting
)


def component(name=None, **kwargs):
    """
    Decorator to create a component from a function.
    """
    from rill.engine.component import _FunctionComponent

    def decorator(func):
        name_ = name or func.__name__
        attrs = {
            'name': name_,
            '_pass_context': kwargs.get('pass_context', False),
            '_execute': staticmethod(func),
            '__doc__': getattr(func, '__doc__', None),
            '__module__': func.__module__,
        }
        cls = type(name_, (_FunctionComponent,), attrs)
        # transfer annotations
        for ann in ANNOTATIONS:
            val = ann.pop(func)
            if val is not None:
                ann.set(cls, val)
        return cls

    if callable(name):
        # @component
        f = name
        name = None
        return decorator(f)
    else:
        # @component()
        assert name is None or isinstance(name, basestring)
        return decorator
