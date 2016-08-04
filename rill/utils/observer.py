import weakref
import types


class BoundMethodWeakref(object):
    '''
    Safe and reusable weak references to bound (instance) methods.

    ``BoundMethodWeakref`` provides a mechanism for referencing a bound method
    weakly, without requiring that the method object itself (which is normally
    a transient object) is kept alive. Instead, it keeps weak references to
    both the instance and the function that together define the instance method.

    This is an improved and cleaned-up implementation of the class of the same
    name from the `pydispatch` project.

    Example
    -------
    First, we need our example class and instance:

    >>> class Foo(object):
    ...     def bar(self):
    ...         print 'bar', self
    ...
    >>> f = Foo()
    >>> f.bar
    <bound method Foo.bar of <__main__.Foo object at 0x1542610>>

    Using a ``weakref.ref`` with the class instance `f` is easy:

    >>> r = weakref.ref(f)
    >>> r
    <weakref at 0x1546310; to 'Foo' at 0x1542610>
    >>> # To "dereference" a ``weakref.ref``, we call it.
    >>> r()
    <__main__.Foo object at 0x1542610>
    >>> # If the object has been deleted, the call will return None.
    >>> del f
    >>> print r()
    None

    Everything works as expected. Now, let's try using the same approach with
    the bound method `f.bar`:

    >>> f = Foo()
    >>> r = weakref.ref(f.bar)
    >>> print r
    <weakref at 0x1546f70; dead>
    >>> print r()
    None

    As you can see, the reference to the instance method is seemingly dead on
    arrival, but that's not entirely true. Because the ``Foo`` class' 'bar'
    attribute points to a descriptor, each attribute access is really calling
    the descriptor's `__get__` method, which in turn returns a new bound method
    instance (which can be thought of as a closure or "partial" function).
    However, because we only ever have a weak reference to that instance, it
    can be (and often is) garbage collected immediately.

    Using the ``BoundMethodWeakref`` class, we get the behavior we expect:

    >>> f = Foo()
    >>> r = BoundMethodWeakref(f.bar)
    >>> r
    BoundMethodWeakref(Foo.bar)
    >>> print r()
    <bound method Foo.bar of <__main__.Foo object at 0x1548510>>
    '''
    _instances = {}

    def __new__(cls, target, *args, **kwargs):
        key = cls.calculate_key(target)
        self = cls._instances.get(key)
        if self is None:
            self = super(BoundMethodWeakref, cls).__new__(cls)
            cls._instances[key] = self
        return self

    def __init__(self, target, callback=None):
        '''
        Return a weak-reference-like instance for a bound method.

        Parameters
        ----------
        target : instancemethod
            The instance method to create a weak reference to. This must have
            'im_self' and 'im_func' attributes, and will be recreated by calling:
            `target.im_func.__get__(target.im_self, target.__class__)`.
        callback : callable or None
            An optional callback to be called when this weak reference ceases
            to be valid (i.e. either the object or the function is garbage
            collected). Should take a single argument, which will be a pointer
            to this object.
        '''
        # See if this is an initialized instance
        callbacks = getattr(self, 'delete_callbacks', None)
        if callbacks is None:
            callbacks = self.delete_callbacks = []
            self.key = self.calculate_key(target)
            self.classname = target.im_self.__class__.__name__
            self.funcname = target.im_func.__name__

            def remove(weak, self=self):
                try:
                    del self.__class__._instances[self.key]
                except KeyError:
                    pass
                # Reverse iteration over callbacks to match ``weakref`` behavior
                for callback in reversed(self.delete_callbacks):
                    try:
                        callback(self)
                    except Exception as e:
                        import traceback
                        try:
                            traceback.print_exc()
                        except AttributeError:
                            print ('Exception during {0} cleanup function '
                                   '{1}: {2}').format(self, callback, e)
                del self.delete_callbacks[:]

            self.weakself = weakref.ref(target.im_self, remove)
            self.weakfunc = weakref.ref(target.im_func, remove)

        if callable(callback):
            callbacks.append(callback)

    def __repr__(self):
        return '<{0.__class__.__name__} to {0.classname}.{0.funcname}>'.format(self)

    def __nonzero__(self):
        return self() is not None

    def __cmp__(self, other):
        if isinstance(other, self.__class__):
            return cmp(self.key, other.key)
        return cmp(self.__class__, type(other))

    def __call__(self):
        '''
        Return a strong reference to the bound instancemethod.

        Returns None if the target cannot be retrieved. Otherwise, returns a
        bound method for our object and function, which is generated by calling
        `target.im_func.__get__(target.im_self, target.__class__)`.
        '''
        instance = self.weakself()
        if instance is not None:
            function = self.weakfunc()
            if function is not None:
                return function.__get__(instance, instance.__class__)

    @staticmethod
    def calculate_key(target):
        '''
        Calculate the reference key for the given reference target.

        This is a two-tuple of the id()'s of the target object and the target
        function respectively.
        '''
        return (id(target.im_self), id(target.im_func))


class EventHandlerWeakref(weakref.ReferenceType):
    '''
    Basic weak reference type that keeps a unique key (currently id()) for its
    reference target.

    This is used by the ``Event`` class to track weak references to listeners,
    and the .key attribute keeps it symmetrical with the ``BoundMethodWeakref``
    class for use in dictionaries.
    '''
    __slots__ = 'key'

    def __new__(cls, target, callback=None):
        # Note that we have to implement __new__ rather than __init__ in order
        # for the Event GC callback to fire properly. I don't fully understand
        # why, but it seems to be one of the peculiarities of built-in types,
        # possibly intersecting with the peculiarities of the weakref.ref type.
        self = super(EventHandlerWeakref, cls).__new__(cls, target, callback)
        self.key = id(target)
        return self


class Event(object):
    '''
    A basic event type, loosely comparable to a Qt signal.

    Event listeners (callables) can be registered with an ``Event`` instance,
    and will subsequently be called if the event is emitted, receiving any
    positional and keyword arguments passed to the event's `.emit()` method.
    '''
    def __init__(self, name, abort_on_error=True):
        self._name = name
        # TODO: add/remove/clearErrorHandler?
        self._abort_on_error = abort_on_error
        self._listeners = {}

        def remove(listener_ref, eventRef=weakref.ref(self)):
            event = eventRef()
            if event is not None:
                del event._listeners[listener_ref.key]
        self._remove = remove

    def __str__(self):
        return self._name

    def __repr__(self):
        return '{0.__class__.__name__}({0._name!r})'.format(self)

    def __len__(self):
        return len(self._listeners)

    @property
    def name(self):
        return self._name

    def listen(self, listener, weak=False):
        '''
        Add a listener (callable) to this event.

        Calling `listen` repeatedly with the same callable will replace the
        stored value, rather than registering the same listener multiple times.
        This allows external code to replace strong listener references with
        weak ones, or vice-versa.

        Parameters
        ----------
        listener : callable
            The callable that will be called when this event is emitted.
        weak : bool
            Whether to store a weak reference to the listener, allowing it to be
            automatically unregistered when the object is garbage-collected.
        '''
        if not callable(listener):
            raise TypeError('Event listeners must be callable')

        is_method = isinstance(listener, types.MethodType)
        if weak:
            if is_method:
                listener = BoundMethodWeakref(listener, callback=self._remove)
            else:
                listener = EventHandlerWeakref(listener, callback=self._remove)
            key = listener.key
        else:
            if is_method:
                key = BoundMethodWeakref.calculate_key(listener)
            else:
                key = id(listener)
        self._listeners[key] = listener

    def remove_listener(self, listener_or_key):
        '''
        Given a listener callable OR its identity key, remove it from this event.
        Raises ``KeyError`` if the listener is not registered with this event.
        '''
        if isinstance(listener_or_key, int):
            key = listener_or_key
        elif isinstance(listener_or_key, types.MethodType):
            key = BoundMethodWeakref.calculate_key(listener_or_key)
        else:
            key = id(listener_or_key)
        try:
            del self._listeners[key]
        except KeyError:
            raise KeyError('Handler {0!r} is not registered for event '
                           '{1!r}'.format(listener_or_key, self._name))

    def clear(self):
        self._listeners.clear()

    def emit(self, *args, **kwargs):
        '''
        Emit the event with the given positional and keyword arguments.

        Returns a dict mapping event listeners to their results, or an exception
        instance if calling the listener raised one.
        '''
        results = {}
#         kwargs['__event__'] = self
        for listener in self._listeners.values():
            if isinstance(listener, (weakref.ReferenceType, BoundMethodWeakref)):
                listener = listener()
                if listener is None:
                    # This shouldn't happen, but we need to guard for it.
                    continue
            try:
                results[listener] = listener(*args, **kwargs)
            except Exception as e:
                if self._abort_on_error:
                    raise
                results[listener] = e
                print("error in listener: %s".format(listener))
                import traceback
                traceback.print_exc()
        return results

    __call__ = emit

    @staticmethod
    def listener_key(listener):
        '''
        Given a listener object, return the key that would be used to identify it
        for the purposes of event registration.
        '''
        if isinstance(listener, types.MethodType):
            return BoundMethodWeakref.calculate_key(listener)
        return id(listener)


def supports_listeners(f):
    """
    Decorator to bind an event to a function.

    An Event instance is bound to an `event` attribute on the function, and uses
    the function's name as the event name.
    """
    f.event = Event(f.__name__)
    return f


__all__ = ('supports_listeners', 'Event')
