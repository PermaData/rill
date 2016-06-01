from rill.utils import NOT_SET


class PortDefinition(object):
    """
    Used to store the properties of a port while defining a component class,
    prior to instantiation of the component and the ports themselves.
    """
    _kind = None

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

    def get_port_type(self):
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
        if self.args.get('fixed_size') and not self.array:
            raise ValueError(
                "{}.{}: @{} specified fixed_size but not array".format(
                    self, self.args['name'],
                    self.__class__.__name__))
        ptype = self.get_port_type()
        return ptype(component, **self.args)

    def get_spec(self):
        """
        Get a fbp-protocol-compatible component spec

        Returns
        -------
        dict
        """
        from rill.engine.types import get_type_handler
        spec = {
            'id': self.args['name'],
            'description': self.args['description'],
            'addressable': self.array,
            'required': (not self.args['optional']),
        }

        spec.update(get_type_handler(self.args['type']).get_spec())
        return spec


class InputPortDefinition(PortDefinition):
    """
    Decorator to add an input port to a component.
    """
    _kind = 'input'

    def __init__(self, name, type=None, array=False, fixed_size=None,
                 description='', optional=True, static=False, default=NOT_SET):
        super(InputPortDefinition, self).__init__(
            name, type=type, array=array, fixed_size=fixed_size,
            description=description, optional=optional)
        self.args['static'] = static
        self.args['default'] = default

    def get_port_type(self):
        from rill.engine.inputport import InputPort, InputArray
        return InputArray if self.array else InputPort

    @classmethod
    def from_port(cls, port):
        """
        Create a port definition from a port.

        Parameters
        ----------
        port : ``rill.engine.inputport.InputPort``

        Returns
        -------
        ``InputPortDefinition``
        """
        return cls(port._name, type=port.type, array=port.is_array(),
                   fixed_size=port.fixed_size if port.is_array() else None,
                   description=port.description,
                   optional=port.optional, static=port.auto_receive,
                   default=port.default)

    def get_spec(self):
        spec = super(InputPortDefinition, self).get_spec()
        spec['default'] = self.args['default']
        return spec


class OutputPortDefinition(PortDefinition):
    """
    Decorator to add an output port to a component.
    """
    _kind = 'output'

    def get_port_type(self):
        from rill.engine.outputport import OutputPort, OutputArray
        return OutputArray if self.array else OutputPort

    @classmethod
    def from_port(cls, port):
        """
        Create a port definition from a port.

        Parameters
        ----------
        port : ``rill.engine.outputport.OutputPort``

        Returns
        -------
        ``OutputPortDefinition``
        """
        return cls(port._name, type=port.type, array=port.is_array(),
                   fixed_size=port.fixed_size if port.is_array() else None,
                   description=port.description,
                   optional=port.optional)

    def get_spec(self):
        spec = super(OutputPortDefinition, self).get_spec()
        spec.pop('values')
        return spec
