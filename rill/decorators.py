from past.builtins import basestring

from rill.utils import Annotation, FlagAnnotation, NOT_SET


__all__ = ['inport', 'outport', 'must_run', 'self_starting', 'component']


class _Port(Annotation):
    multi = True

    _kind = None
    _static_port_type = None
    _array_port_type = None

    def __init__(self, name, type=None, array=False, fixed_size=None,
                 description='', optional=True):
        self.array = array
        self.args = {
            'name': name,
            'type': type,
            'optional': optional,
            'description': description
        }
        if fixed_size is not None:
            self.args['fixed_size'] = fixed_size
        super(_Port, self).__init__(self)

    def create_port(self, component):
        if self.args.get('fixed_size') and not self.array:
            raise ValueError(
                "{}.{}: @{} specified fixed_size but not array".format(
                    self, self.args['name'],
                    self.__class__.__name__))
        if self.array:
            ptype = self._array_port_type
        else:
            ptype = self._static_port_type
        return ptype(component, **self.args)


class inport(_Port):
    """
    Decorator to add an input port to a component.
    """
    attribute = 'inport_definitions'
    _kind = 'input'

    def __init__(self, name, type=None, array=False, fixed_size=None,
                 description='', optional=True, static=False, default=NOT_SET):
        super(inport, self).__init__(
            name, type=type, array=array, fixed_size=fixed_size,
            description=description, optional=optional)
        self.args['static'] = static
        self.args['default'] = default

    @classmethod
    def from_port(cls, port):
        return cls(port._name, type=port.type, array=port.is_array(),
                   fixed_size=port.fixed_size if port.is_array() else None,
                   description=port.description,
                   optional=port.optional, static=port.auto_receive,
                   default=port.default)


class outport(_Port):
    """
    Decorator to add an output port to a component.
    """
    attribute = 'outport_definitions'
    _kind = 'output'

    @classmethod
    def from_port(cls, port):
        return cls(port._name, type=port.type, array=port.is_array(),
                   fixed_size=port.fixed_size if port.is_array() else None,
                   description=port.description,
                   optional=port.optional)


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
