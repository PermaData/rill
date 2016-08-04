
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