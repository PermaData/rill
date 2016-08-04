import inspect
import collections
import schematics.types
import schematics.models
from schematics.undefined import Undefined

def setup_primitive_types():
    import decimal
    import datetime
    # temp fix until PR is merged
    schematics.types.IPv4Type.primitive_type = str
    schematics.types.IPv4Type.native_type = str
    schematics.types.StringType.primitive_type = str
    schematics.types.StringType.native_type = str
    schematics.types.URLType.primitive_type = str
    schematics.types.URLType.native_type = str
    schematics.types.EmailType.primitive_type = str
    schematics.types.EmailType.native_type = str
    schematics.types.IntType.primitive_type = int
    schematics.types.IntType.native_type = int
    schematics.types.FloatType.primitive_type = float
    schematics.types.FloatType.native_type = float
    schematics.types.DecimalType.primitive_type = str
    schematics.types.DecimalType.native_type = decimal.Decimal
    schematics.types.BooleanType.primitive_type = bool
    schematics.types.BooleanType.native_type = bool
    schematics.types.DateTimeType.primitive_type = str
    schematics.types.DateTimeType.native_type = datetime.datetime
    schematics.types.DateType.primitive_type = str
    schematics.types.DateType.native_type = datetime.date
    schematics.types.TimestampType.primitive_type = float
    schematics.types.TimestampType.native_type = datetime.timedelta
    schematics.types.ModelType.primitive_type = dict
    schematics.types.ListType.primitive_type = list
    schematics.types.DictType.primitive_type = dict


setup_primitive_types()


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
    from rill.engine.types import TYPE_MAP

    if isinstance(field_instance, schematics.types.ModelType):
        # TODO: handle polymorphic models
        schema = _convert_model(field_instance.model_class)

    elif isinstance(field_instance, schematics.types.ListType):
        subschema = to_jsonschema(field_instance.field)
        schema = {
            'type': 'array',
            'items': subschema
        }
        if 'title' in subschema:
            schema['title'] = '%s Array' % subschema['title']

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
    # TODO: UnionType
    # TODO: DictType

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
    if inspect.isclass(obj) and issubclass(obj, schematics.models.Model):
        return _convert_model(obj)
    elif isinstance(obj, schematics.types.BaseType):
        return _convert_field(obj)
    else:
        raise TypeError(obj)
