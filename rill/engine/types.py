from abc import ABCMeta, abstractmethod
import inspect
import collections

import schematics.types
import schematics.models
from schematics.undefined import Undefined

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
    # color
    # date
    # function
    # buffer
}

FBP_TYPES = {
    'any': {
        'color_id': 0
    },
    'string': {
        'color_id': 1
    },
    'boolean': {
        'color_id': 2
    },
    'int': {
        'color_id': 3
    },
    'number': {
        'color_id': 3
    },
    'object': {
        'color_id': 4
    },
    'array': {
        'color_id': 4
    },
}


def register(cls):
    """
    Register a ``TypeHandler`` class

    Parameters
    ----------
    cls : Type[``TypeHandler``]
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
    if type_def is None:
        return UnspecifiedTypeHandler(type_def)

    for cls in _type_handlers:
        if cls.claim_type_def(type_def):
            return cls(type_def)

    raise TypeHandlerError("Could not find type handler "
                           "for {!r}".format(type_def))


class TypeHandler(with_metaclass(ABCMeta, object)):
    """
    Base class for validating and serializing content.
    """
    has_schema = False

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


class UnspecifiedTypeHandler(TypeHandler):
    def get_spec(self):
        return {'type': 'any'}

    def validate(self, value):
        return value

    def to_primitive(self, data):
        # this is assumed to be json-serializable
        return data

    def to_native(self, data):
        # this is assumed to be json-deserializable
        return data

    @classmethod
    def claim_type_def(cls, type_def):
        return False


class BasicTypeHandler(TypeHandler):
    """
    Simple type handler that is used when setting a port's `type` to a basic
    python type, such as `str`, `int`, `float`, or `bool`.

    This class provides no additional functionality when serializing data:
    the types are expected to be json-serializable.
    """
    def get_spec(self):
        return {'type': TYPE_MAP.get(self.type_def, 'any')}

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
        # this is assumed to be json-serializable
        return data

    def to_native(self, data):
        # this is assumed to be json-deserializable
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
schematics.types.DateType.primitive_type = str
schematics.types.TimestampType.primitive_type = float
schematics.types.ModelType.primitive_type = dict
schematics.types.ListType.primitive_type = list
schematics.types.DictType.primitive_type = dict


class SchematicsTypeHandler(TypeHandler):
    has_schema = True

    def __init__(self, type_def):
        if inspect.isclass(type_def):
            type_def = type_def()
        super(SchematicsTypeHandler, self).__init__(type_def)

    def get_spec(self):
        # FIXME: warn if primitive_type is not set?
        return to_jsonschema(self.type_def)

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

# Parameters for serialization to JSONSchema
schema_kwargs_to_schematics = {
    'maxLength': 'max_length',
    'minLength': 'min_length',
    'pattern': 'regex',
    'minimum': 'min_value',
    'maximum': 'max_value',
    'enum': 'choices',
}


TITLE_METADATA_KEYS = ['title', 'label']
DESCRIPTION_METADATA_KEYS = ['description', 'help']


def _convert_field(field_instance):
    if isinstance(field_instance, schematics.types.ModelType):
        # TODO: handle polymorphic models
        schema = _convert_model(field_instance.model_class)

    elif isinstance(field_instance, schematics.types.ListType):
        subschema = _convert_model(field_instance.model_class)
        schema = {
            'type': 'array',
            'title': '%s Array' % subschema['title'],
            'items': subschema
        }
        # TODO: min_size -> minItems
        # TODO: max_size -> maxItems

    elif isinstance(field_instance, schematics.types.BaseType):
        schema = {
            "type": TYPE_MAP[getattr(field_instance, 'primitive_type', str)]
        }
        # TODO: date-time, email, ipv4, ipv6
        for js_key, schematic_key in schema_kwargs_to_schematics.items():
            value = getattr(field_instance, schematic_key, None)
            if value is not None:
                schema[js_key] = value
    else:
        raise TypeError(field_instance)
    # TODO: handle UnionType

    default = field_instance.default
    if default is not Undefined:
        schema['default'] = default

    metadata = field_instance.metadata
    if metadata:
        if 'jsonschema' in metadata:
            schema.update(metadata['jsonschema'])

        for key in TITLE_METADATA_KEYS:
            if key in metadata:
                schema['title'] = metadata[key]
                break
        for key in DESCRIPTION_METADATA_KEYS:
            if key in metadata:
                schema['desciption'] = metadata[key]
                break

    return schema


def _convert_model(model):
    properties = collections.OrderedDict()
    required = []
    for field_name, field_instance in model._fields.items():
        serialized_name = getattr(field_instance, 'serialized_name',
                                  None) or field_name
        properties[serialized_name] = _convert_field(field_instance)
        if getattr(field_instance, 'required', False):
            required.append(serialized_name)

    schema = {
        'type': 'object',
        'title': model.__name__,
        'properties': properties,
    }
    # TODO: __doc__ -> description?
    # TODO: add order if cls._options.export_order?
    if required:
        schema['required'] = required

    return schema


# FIXME: convert this to use schematics.transforms.export_loop()
def to_jsonschema(obj):
    if isinstance(obj, schematics.models.Model):
        return _convert_model(obj)
    elif isinstance(obj, schematics.types.BaseType):
        return _convert_field(obj)
    else:
        raise TypeError(obj)
