from weakref import WeakSet
import inspect

from kids.cache import cache


try:
    from abc import abstractclassmethod
except ImportError:
    class abstractclassmethod(classmethod):

        __isabstractmethod__ = True

        def __init__(self, callable):
            callable.__isabstractmethod__ = True
            super(abstractclassmethod, self).__init__(callable)


def importable_class_name(klass, assert_valid=False):
    '''
    Create an string to use for locating the given class.

    Returns
    -------
    str
    '''
    import pydoc
    name = "{}.{}".format(klass.__module__, klass.__name__)
    if assert_valid:
        obj = pydoc.locate(name)
        if obj is None:
            raise ValueError("Could not locate {} at {}".format(klass, name))
        elif obj is not klass:
            raise ValueError("Object {} at {} is not "
                             "the same as {}".format(obj, name, klass))
    return name


def locate_class(class_location):
    # pydoc incorrectly raises ErrorDuringImport when gevent is patched in,
    # so we have to look for the class attribute ourselves.
    import pydoc
    mod_name, class_name = class_location.rsplit('.', 1)
    module = pydoc.locate(mod_name)
    if module is None:
        raise ValueError("Failed to find module {!r}".format(mod_name))
    try:
        return getattr(module, class_name)
    except AttributeError:
        raise ValueError("Failed to find object {!r} in module {!r}".format(
            class_name, mod_name))


class classproperty(object):
    """
    Class for creating properties for un-initialized classes. Works like a
    combination of the classmethod and property decorators.

    Note that it is NOT possible to overwrite the "setter" for a classproperty,
    and any attempt to assign to the class property will, in fact, simply
    replace the classproperty object with the new value.

    Decorated methods will be callable at the class-instance level.

    Examples
    --------
    >>> class MyClass(object):
    ...     @classproperty
    ...     def someMethod(cls):
    ...         return 123
    >>> MyClass.someMethod
    123
    """
    def __init__(self, getter, doc=None):
        if doc is None and hasattr(getter, "__doc__"):
            doc = getter.__doc__
        self.getter = getter
        self.__doc__ = doc

    def __get__(self, instance, owner):
        return self.getter(owner)


class NOT_SET(object):
    pass


class Annotation(object):
    """
    An annotation provides a simple interface to create a decorator that stores
    data about the object it decorates.

    Attributes
    ----------
    multi : bool
        Whether the annotation can be repeated to create a list of values
    default : object
        default value if the annotation is not present
    attribute : str
        name of attribute on the decorated object on which to store the
        annotated value.
    """
    # sub-classes provide:
    multi = False
    default = None
    attribute = None

    def __init__(self, value):
        """
        Called to add arguments to the decorator.
        """
        # store this value, so it can be used by __call__
        self._value = value

    def __call__(self, obj):
        """
        Called when used as a decorator.

        Parameters
        ----------
        obj
            the object being decorated

        Returns
        -------
        obj
            the original object
        """
        if self.multi:
            self._append(obj, self._value)
        else:
            self.set(obj, self._value)
        return obj

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
            cls._seen.add(obj)
            return False
        else:
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
        # don't get inherited values
        return obj.__dict__.get(cls.attr(), cls.default)

    @classmethod
    def get_inherited(cls, obj):
        """
        For use with multi=True
        """
        assert cls.multi
        result = []
        for base in reversed(inspect.getmro(obj)):
            result.extend(cls.get(base) or [])
        return result

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
            values = cls.get(obj) or []
        cls.set(obj, values)
        # we actually prepend because decorators are applied bottom up, which
        # when maintaining order, is not usually intuitive
        values.insert(0, value)


class ProxyAnnotation(Annotation):
    """
    Like Annotation, but instead of instantiating with a value instance,
    is instantiated with arguments for the class defined at `proxy_type`.
    """
    # sub-classes provide:
    proxy_type = None

    def __init__(self, *args, **kwargs):
        super(ProxyAnnotation, self).__init__(self.proxy_type(*args, **kwargs))


class FlagAnnotation(Annotation):
    """
    An boolean annotation that is either present or not.
    """
    default = False

    def __new__(cls, obj):
        """
        Parameters
        ----------
        obj : the object being decorated

        Returns
        -------
        obj : the object being decorated
        """
        cls.set(obj, True)
        return obj

