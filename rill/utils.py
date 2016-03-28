from weakref import WeakSet

class NOT_SET(object):
    pass


class Annotation(object):
    """
    An annotation provides a simple interface to create a decorator that stores
    data about the object it decorates.

    Attributes
    ----------
    multi : bool
        Whether the annotion can be repeated to create a list of values
    default : object
        default value if the annotation is not present
    """
    multi = False
    default = None
    attribute = None

    @classmethod
    def seen(cls, obj):
        """
        Return whether the object has previously been annotated.

        Parameters
        ----------
        obj : object
            object to check for previous annotation

        Returns
        -------
        bool
        """
        # this ensures that the _seen attribute is stored on each leaf
        # class
        if not hasattr(cls, '_seen'):
            # for the multi feature, we need to know if this object has already
            # been annotated by this class. we can't simply check the existence
            # of cls.attribute on the object because it may already exist there
            # (i.e. as a default value which we should override).
            cls._seen = WeakSet()
        seen = obj in cls._seen
        if not seen:
            cls._seen.add(obj)
        return seen

    @classmethod
    def attr(cls):
        if cls.attribute is not None:
            return cls.attribute
        else:
            return '_' + cls.__name__

    @classmethod
    def get(cls, obj):
        """
        Get annotated data for `obj`
        """
        return getattr(obj, cls.attr(), cls.default)

    @classmethod
    def pop(cls, obj):
        """
        Remove and return annotated data for `obj`
        """
        value = cls.get(obj)
        if hasattr(obj, cls.attr()):
            delattr(obj, cls.attr())
        return value

    @classmethod
    def set(cls, obj, value):
        """
        Set annotated data for `obj`
        """
        if not cls.multi and cls.seen(obj):
            raise ValueError("Annotation %s used more than once with %r" %
                             (cls.__name__, obj))
        setattr(obj, cls.attr(), value)

    @classmethod
    def _append(cls, obj, value):
        assert cls.multi
        if not cls.seen(obj):
            values = []
        else:
            values = cls.get(obj)
        cls.set(obj, values)
        # we actually prepend because decorators are applied bottom up, which
        # when maintaining order, is not usually intuitive
        values.insert(0, value)

    def __init__(self, value):
        # store this value, so it can be used by __call__
        self._value = value

    def __call__(self, obj):
        if self.multi:
            self._append(obj, self._value)
        else:
            self.set(obj, self._value)
        return obj


class FlagAnnotation(Annotation):
    """
    An boolean annotation that is either present or not.
    """
    default = False

    def __new__(cls, obj):
        cls.set(obj, True)
        return obj

