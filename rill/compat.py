import sys as _sys
import inspect as _inspect

from six import PY2, PY3, add_metaclass, text_type, binary_type, string_types
str = text_type
bytes = binary_type
basestring = string_types


# from future:

def _get_caller_globals_and_locals():
    """
    Returns the globals and locals of the calling frame.

    Is there an alternative to frame hacking here?
    """
    caller_frame = _inspect.stack()[2]
    myglobals = caller_frame[0].f_globals
    mylocals = caller_frame[0].f_locals
    return myglobals, mylocals


def _repr_strip(mystring):
    """
    Returns the string without any initial or final quotes.
    """
    r = repr(mystring)
    if r.startswith("'") and r.endswith("'"):
        return r[1:-1]
    else:
        return r

if PY3:
    from abc import abstractclassmethod

    def raise_from(exc, cause):
        """
        Equivalent to:

            raise EXCEPTION from CAUSE

        on Python 3. (See PEP 3134).
        """
        # Is either arg an exception class (e.g. IndexError) rather than
        # instance (e.g. IndexError('my message here')? If so, pass the
        # name of the class undisturbed through to "raise ... from ...".
        if isinstance(exc, type) and issubclass(exc, Exception):
            exc = exc.__name__
        if isinstance(cause, type) and issubclass(cause, Exception):
            cause = cause.__name__
        execstr = "raise " + _repr_strip(exc) + " from " + _repr_strip(cause)
        myglobals, mylocals = _get_caller_globals_and_locals()
        exec(execstr, myglobals, mylocals)

    def raise_(tp, value=None, tb=None):
        """
        A function that matches the Python 2.x ``raise`` statement. This
        allows re-raising exceptions with the cls value and traceback on
        Python 2 and 3.
        """
        if value is not None and isinstance(tp, Exception):
            raise TypeError("instance exception may not have a separate value")
        if value is not None:
            exc = tp(value)
        else:
            exc = tp
        if exc.__traceback__ is not tb:
            raise exc.with_traceback(tb)
        raise exc

    def raise_with_traceback(exc, traceback=Ellipsis):
        if traceback == Ellipsis:
            _, _, traceback = _sys.exc_info()
        raise exc.with_traceback(traceback)

else:
    class abstractclassmethod(classmethod):

        __isabstractmethod__ = True

        def __init__(self, callable):
            callable.__isabstractmethod__ = True
            super(abstractclassmethod, self).__init__(callable)

    def raise_from(exc, cause):
        """
        Equivalent to:

            raise EXCEPTION from CAUSE

        on Python 3. (See PEP 3134).
        """
        # Is either arg an exception class (e.g. IndexError) rather than
        # instance (e.g. IndexError('my message here')? If so, pass the
        # name of the class undisturbed through to "raise ... from ...".
        if isinstance(exc, type) and issubclass(exc, Exception):
            e = exc()
            # exc = exc.__name__
            # execstr = "e = " + _repr_strip(exc) + "()"
            # myglobals, mylocals = _get_caller_globals_and_locals()
            # exec(execstr, myglobals, mylocals)
        else:
            e = exc
        e.__suppress_context__ = False
        if isinstance(cause, type) and issubclass(cause, Exception):
            e.__cause__ = cause()
            e.__suppress_context__ = True
        elif cause is None:
            e.__cause__ = None
            e.__suppress_context__ = True
        elif isinstance(cause, BaseException):
            e.__cause__ = cause
            e.__suppress_context__ = True
        else:
            raise TypeError("exception causes must derive from BaseException")
        e.__context__ = _sys.exc_info()[1]
        raise e

    exec('''
def raise_(tp, value=None, tb=None):
    raise tp, value, tb

def raise_with_traceback(exc, traceback=Ellipsis):
    if traceback == Ellipsis:
        _, _, traceback = sys.exc_info()
    raise exc, None, traceback
'''.strip())


raise_with_traceback.__doc__ = (
"""Raise exception with existing traceback.
If traceback is not passed, uses sys.exc_info() to get traceback."""
)

