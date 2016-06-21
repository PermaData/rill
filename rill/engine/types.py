from abc import ABCMeta, abstractmethod
import inspect

import schematics.types
import schematics.models

from future.utils import with_metaclass

from rill.engine.exceptions import TypeHandlerError, PacketValidationError

_type_handlers = []

# Mapping of native Python types to FBP protocol types
TYPE_MAP = {
    str: 'string',
    bool: 'boolean',
    int: 'int',
    float: 'number',
    complex: 'number',
    dict: 'object',
    list: 'array',
    tuple: 'array',
    #color
    #date
    #function
    #buffer
}


def register(cls):
    """
    Register a ``TypeHandler`` class

    Parameters
    ----------
    cls : ``TypeHandler`` class
    """
    # LIFO
    _type_handlers.insert(0, cls)


def get_type_handler(type_def):
    """

    Parameters
    ----------
    type_def : object
        instance stored on the `type` attribute of
        ``rill.engine.portdef.PortDefinition``

    Returns
    -------
    ``TypeHandler``
    """
    for cls in _type_handlers:
        if cls.claim_type_def(type_def):
            return cls(type_def)


class TypeHandler(with_metaclass(ABCMeta, object)):
    """
    Base class for validating and serializing content.
    """
    def __init__(self, type_def):
        self.type_def = type_def

    @abstractmethod
    def get_spec(self):
        """
        Get a fbp-protocol-compatible type spec

        Returns
        -------
        dict

        Raises
        ------
        ``rill.exceptions.TypeHandlerError``
        """
        raise NotImplementedError

    @abstractmethod
    def validate(self, value):
        """
        Validate `value`.

        Parameters
        ----------
        value

        Raises
        ------
        ``rill.exceptions.PacketValidationError``

        Returns
        -------
        object or None
            if non-None is returned, the returned value *may* replace the
            existing value in the packet, depending on where this is called
        """
        raise NotImplementedError

    @abstractmethod
    def to_primitive(self, data):
        """
        Convert data to a value safe to serialize.
        """
        raise NotImplementedError

    @abstractmethod
    def to_native(self, data):
        """
        Convert primitive data to its native Python construct.
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def claim_type_def(cls, type_def):
        raise NotImplementedError


class BasicTypeHandler(TypeHandler):
    """
    Simple type handler that is used when setting a port's `type` to a basic
    python type, such as `str`, `int`, `float`, or `bool`.

    This class provides no additional functionality when serializing data:
    the types are expected to be json-serializable.
    """
    def get_spec(self):
        return {'type': TYPE_MAP.get(self.type_def, 'object')}

    def validate(self, value):
        if isinstance(value, self.type_def):
            return value
        try:
            # FIXME: we probably want a list of allowable types to cast from
            return self.type_def(value)
        except Exception as err:
            raise PacketValidationError(
                "Data is type {}: expected {}. Error while casting: {}".format(
                    value.__class__.__name__, self.type_def.__name__, err))

    def to_primitive(self, data):
        return data

    def to_native(self, data):
        return data

    @classmethod
    def claim_type_def(cls, type_def):
        return inspect.isclass(type_def)


register(BasicTypeHandler)


schematics.types.UUIDType.primitive_type = str
schematics.types.IPv4Type.primitive_type = str
schematics.types.StringType.primitive_type = str
schematics.types.URLType.primitive_type = str
schematics.types.EmailType.primitive_type = str
schematics.types.IntType.primitive_type = int
schematics.types.FloatType.primitive_type = float
schematics.types.DecimalType.primitive_type = str
schematics.types.BooleanType.primitive_type = bool
schematics.types.DateTimeType.primitive_type = str
schematics.types.ModelType.primitive_type = dict
schematics.types.ListType.primitive_type = list
schematics.types.DictType.primitive_type = dict


class SchematicsTypeHandler(TypeHandler):

    def __init__(self, type_def):
        if inspect.isclass(type_def):
            type_def = type_def()
        super(SchematicsTypeHandler, self).__init__(type_def)

    def get_spec(self):
        # FIXME: warn if primitive_type is not set?
        spec = {'type': TYPE_MAP.get(self.type_def.primitive_type, str)}
        choices = self.type_def.choices
        if choices:
            spec['values'] = choices
        return spec

    def validate(self, value):
        try:
            return self.type_def.to_native(value)
        except Exception as e:
            raise PacketValidationError(str(e))

    def to_primitive(self, data):
        return self.type_def.to_primitive(data)

    def to_native(self, data):
        return self.type_def.to_native(data)

    @classmethod
    def claim_type_def(cls, type_def):
        bases = (schematics.types.BaseType, schematics.models.Model)
        return (isinstance(type_def, bases) or
                (inspect.isclass(type_def) and
                 issubclass(type_def, bases)))


register(SchematicsTypeHandler)
