from weakref import WeakKeyDictionary
import gevent
from gevent.lock import RLock

NOT_SET = object()


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

    @classmethod
    def _data(cls):
        # this ensures that the _annotations attribute is stored on each leaf
        # class
        if not hasattr(cls, '_annotations'):
            cls._annotations = WeakKeyDictionary()
        return cls._annotations

    @classmethod
    def get(cls, obj):
        """
        Get annotated data for `obj`
        """
        return cls._data().get(obj, cls.default)

    @classmethod
    def pop(cls, obj):
        """
        Remove and return annotated data for `obj`
        """
        return cls._data().pop(obj, cls.default)

    @classmethod
    def set(cls, obj, value):
        """
        Set annotated data for `obj`
        """
        data = cls._data()
        if not cls.multi and obj in data:
            raise ValueError("Annotation %s used more than once with %r" %
                             (cls.__name__, obj))
        data[obj] = value

    @classmethod
    def _append(cls, obj, value):
        assert cls.multi
        data = cls._data()
        values = data.setdefault(obj, [])
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

    @classmethod
    def set(cls, obj, value):
        cls._data()[obj] = value

    def __new__(cls, obj):
        cls.set(obj, True)
        return obj


class CountDownLatch(gevent.Greenlet):
    def __init__(self, count, freq=0.1):
        super(CountDownLatch, self).__init__()
        assert count >= 0
        self._lock = RLock()
        self._count = count
        self._freq = freq

    def count_down(self):
        with self._lock:
            assert self._count > 0

            self._count -= 1
            # Return inside lock to return the correct value,
            # otherwise an other thread could already have
            # decremented again.
            return self._count

    def get_count(self):
        return self._count

    def _run(self):
        while self._count > 0:
            gevent.sleep(self._freq)
        return True
