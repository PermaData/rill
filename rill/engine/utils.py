import gevent
from gevent.lock import RLock


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
