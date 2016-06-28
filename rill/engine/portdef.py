import inspect
import itertools

from rill.utils import NOT_SET

IN_NULL = 'wait'
OUT_NULL = 'done'


class PortDefinition(object):
    """
    Used to store the properties of a port while defining a component class,
    prior to instantiation of the component and the ports themselves.
    """
    kind = None
    __slots__ = ('array', 'name', 'type', 'required', 'description',
                 'fixed_size')

    def __init__(self, name, type=None, array=False, fixed_size=None,
                 description='', required=False):
        from rill.engine.types import get_type_handler
        self.array = array
        self.name = name
        self.type = get_type_handler(type)
        self.required = required
        self.description = description
        self.fixed_size = fixed_size

    @property
    def data(self):
        """
        Returns
        -------
        dict
        """
        data = {k: getattr(self, k) for k in
                itertools.chain(*[getattr(cls, '__slots__', tuple()) for cls
                                  in inspect.getmro(self.__class__)])}
        is_array = data.pop('array')
        if not is_array:
            data.pop('fixed_size')
        return data

    def get_port_type(self):
        '''
        Get the class used by this port.

        Returns
        -------
        Type[``rill.engine.port.BasePort``]
        '''
        raise NotImplementedError

    def create_port(self, component):
        """
        Create a port from this component definition

        Parameters
        ----------
        component : ``rill.engine.component.Component``

        Returns
        -------
        ``rill.engine.port.BasePort``
        """
        if self.fixed_size is not None and not self.array:
            raise ValueError(
                "{}.{}: @{} specified fixed_size but not array".format(
                    self, self.name,
                    self.__class__.__name__))
        ptype = self.get_port_type()
        return ptype(component, **self.data)

    def get_spec(self):
        """
        Get a fbp-protocol-compatible component spec

        Returns
        -------
        dict
        """
        spec = {
            'id': self.name,
            'description': self.description,
            'addressable': self.array,
            'required': self.required,
        }
        spec.update(self.type.get_spec())
        return spec


class InputPortDefinition(PortDefinition):
    kind = 'in'
    __slots__ = ('static', 'default')

    def __init__(self, name, type=None, array=False, fixed_size=None,
                 description='', required=False, static=False, default=NOT_SET):
        super(InputPortDefinition, self).__init__(
            name, type=type, array=array, fixed_size=fixed_size,
            description=description, required=required)
        self.static = static
        self.default = default

    def get_port_type(self):
        from rill.engine.inputport import InputPort, InputArray
        return InputArray if self.array else InputPort

    @classmethod
    def from_port(cls, port):
        """
        Create a port definition from a port.

        Parameters
        ----------
        port : Union[``rill.engine.inputport.InputPort``, ``rill.engine.inputport.InputArray``]

        Returns
        -------
        ``InputPortDefinition``
        """
        return cls(port._name, type=port.type.type_def, array=port.is_array(),
                   fixed_size=port.fixed_size if port.is_array() else None,
                   description=port.description,
                   required=port.required, static=port.auto_receive,
                   default=port.default)

    def get_spec(self):
        spec = super(InputPortDefinition, self).get_spec()
        if self.default is not NOT_SET:
            spec['default'] = self.type.to_primitive(self.default)
        if self.name == IN_NULL:
            spec['type'] = 'bang'
        return spec


class OutputPortDefinition(PortDefinition):
    kind = 'out'
    __slots__ = ()

    def get_port_type(self):
        from rill.engine.outputport import OutputPort, OutputArray
        return OutputArray if self.array else OutputPort

    @classmethod
    def from_port(cls, port):
        """
        Create a port definition from a port.

        Parameters
        ----------
        port : Union[``rill.engine.outputport.OutputPort``, ``rill.engine.outputport.OutputArray``]

        Returns
        -------
        ``OutputPortDefinition``
        """
        return cls(port._name, type=port.type.type_def, array=port.is_array(),
                   fixed_size=port.fixed_size if port.is_array() else None,
                   description=port.description,
                   required=port.required)

    def get_spec(self):
        spec = super(OutputPortDefinition, self).get_spec()
        spec.pop('values', None)
        if self.name == OUT_NULL:
            spec['type'] = 'bang'
        return spec
