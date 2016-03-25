from abc import ABCMeta, abstractmethod
import inspect

from future.utils import with_metaclass

from rill.engine.exceptions import PacketValidationError

_type_handlers = []


def register(cls):
    # LIFO
    _type_handlers.insert(0, cls)


def get_type_handler(type_def):
    """

    Parameters
    ----------
    type_def : object
        type definition object passed to ``inport`` or ``outport``

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
    def validate(self, value):
        """
        Validate `value`.

        Parameters
        ----------
        value

        Returns
        -------
        object or None
            if non-None is returned, the returned value *may* replace the
            existing value in the packet, depending on where this is called
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def claim_type_def(cls, type_def):
        raise NotImplementedError


class BasicTypeHandler(TypeHandler):
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

    @classmethod
    def claim_type_def(cls, type_def):
        return inspect.isclass(type_def)


register(BasicTypeHandler)

try:
    import schematics.types
    import schematics.models


    class SchematicsTypeHandler(TypeHandler):

        def __init__(self, type_def):
            if inspect.isclass(type_def):
                type_def = type_def()
            super(SchematicsTypeHandler, self).__init__(type_def)

        def validate(self, value):
            try:
                return self.type_def.to_native(value)
            except Exception as e:
                raise PacketValidationError(str(e))

        @classmethod
        def claim_type_def(cls, type_def):
            bases = (schematics.types.BaseType, schematics.models.Model)
            return (isinstance(type_def, bases) or
                    (inspect.isclass(type_def) and
                     issubclass(type_def, bases)))


    register(SchematicsTypeHandler)

except ImportError:
    pass
