from rill.utils.annotations import ProxyAnnotation, FlagAnnotation
from rill.engine.portdef import InputPortDefinition, OutputPortDefinition
from rill.compat import *

from typing import Union, Callable, Type

__all__ = ['inport', 'outport', 'must_run',
           'self_starting', 'component', 'subnet']


class inport(ProxyAnnotation):
    """Decorator class to make a ``rill.engine.inputport.InputPort`` from a
    function.
    """
    multi = True
    attribute = '_inport_definitions'
    proxy_type = InputPortDefinition


class outport(ProxyAnnotation):
    """Decorator class to make a ``rill.engine.outputport.OutputPort`` from a
    function.
    """
    multi = True
    attribute = '_outport_definitions'
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


def component(name_or_func=None, **kwargs):
    """
    Decorator to create a component from a function.

    Parameters
    ----------
    name_or_func : Union[Callable, str]
        Given a callable, create a ``rill.engine.component.Component``
        subclass from it. If given a string, the resultant subclass will have
        that string as its name.
    kwargs : Dict[str, Any]
        Possible keys:
        pass_context: bool
            Should instances of the resulting class pass themselves in when
            called?
        base_class: ``rill.engine.component.Component``
            Make the returned class a subclass of base_class instead of the
            default ``rill.engine.component._FunctionComponent``

    Returns
    -------
    Type[``rill.engine.component.Component``]
        Creates a Component subclass to be instantiated into nodes within a
        Graph later.
    """
    from rill.engine.component import _FunctionComponent

    def decorator(func):
        name_ = name or func.__name__
        attrs = {
            # The name explicitly given or the function's __name__
            'type_name': name_,
            # Explained above
            '_pass_context': kwargs.get('pass_context', False),
            # Set the created class to execute the function
            '_execute': staticmethod(func),
            # Copy docstring
            '__doc__': getattr(func, '__doc__', None),
            # Copy location info
            '__module__': func.__module__,
        }
        # Create class with new attributes
        cls = type(name_,
                   (kwargs.get('base_class', _FunctionComponent),),
                   attrs)
        # transfer annotations from func to cls
        for ann in ANNOTATIONS:
            val = ann.pop(func)
            if val is not None:
                ann.set(cls, val)
        return cls

    if callable(name_or_func):
        # @component
        if kwargs:
            raise ValueError("If you call @component with a callable **kwargs "
                             "is ignored")
        f = name_or_func
        name = None
        return decorator(f)
    else:
        # @component('name')
        assert name_or_func is None or isinstance(name_or_func, basestring)
        name = name_or_func
        return decorator


def subnet(name_or_func):
    """
    Decorator for creating subnet

    Callback expects a function with one argument, the Network that will be
    wrapped by the SubGraph component.

    Parameters
    ----------
    name_or_func : Union[Callable, str]
        Given a callable, create a ``rill.engine.subnet.SubGraph`` subclass
        from it. If given a string, the resultant subclass will have that
        string as its name.

    Returns
    -------
    subnet : Type[``rill.engine.subnet.SubGraph``]
        Creates a SubGraph subclass that will be instantiated within a Graph
        later.
    """
    from rill.engine.subnet import SubGraph

    def decorator(func):
        def define(cls, graph):
            func(graph)

        name_ = name or func.__name__
        attrs = {
            # The name explicitly given or the function's __name__
            'name': name_,
            # Set the created class to use the function to define a subgraph
            'define': classmethod(define),
            # Copy docstring
            '__doc__': getattr(func, '__doc__', None),
            # Copy location
            '__module__': func.__module__,
        }
        # Create class with new attributes
        cls = type(name_, (SubGraph,), attrs)
        # transfer annotations from func to cls
        # FIXME: not sure if all of the annotations make sense for subnets...
        for ann in ANNOTATIONS:
            val = ann.pop(func)
            if val is not None:
                ann.set(cls, val)
        return cls

    if callable(name_or_func):
        # @subnet
        f = name_or_func
        name = None
        return decorator(f)
    else:
        # @subnet('name')
        assert name_or_func is None or isinstance(name_or_func, basestring)
        name = name_or_func
        return decorator
