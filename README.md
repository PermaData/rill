# rill

[![Build Status](https://travis-ci.org/chadrik/rill.svg)](https://travis-ci.org/chadrik/rill)

rill is a lightweight python framework for [Flow-based Programming](http://www.jpaulmorrison.com/fbp/)
built on [gevent](http://www.gevent.org/). With it, you can create networks of
worker components, each operating on their own [green thread](https://en.wikipedia.org/wiki/Green_threads),
which push and pull streams of data packets through named ports.


The internals are heavily inspired by [JavaFBP](https://github.com/jpaulm/javafbp), which
is maintained by the author of the seminal [book](http://www.jpaulmorrison.com/fbp/book.html)
on the subject, while the API draws from concise and expressive frameworks like click and flask.

