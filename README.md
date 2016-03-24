# rill

[![Build Status](https://travis-ci.org/chadrik/rill.svg)](https://travis-ci.org/chadrik/rill)

rill is a lightweight python framework for [Flow-based Programming](http://www.jpaulmorrison.com/fbp/)
built on [gevent](http://www.gevent.org/). With it, you can create networks of
worker components, each operating on their own [green thread](https://en.wikipedia.org/wiki/Green_threads),
which push and pull streams of data packets through named ports.


The internals are heavily inspired by [JavaFBP](https://github.com/jpaulm/javafbp), which
is maintained by the author of the seminal [book](http://www.jpaulmorrison.com/fbp/book.html)
on the subject, while the API draws from concise and expressive frameworks like click and flask.

## Using the UI

### Installing

```
npm install -g n
npm install -g bower
npm install -g grunt-cli
sudo n 4.1

mkdir noflo
cd noflo

git clone https://github.com/noflo/noflo-ui
cd noflo-ui
git checkout 40acbb4d92f178837f161b7c15f2dde2a5bfa4bc
npm install
# running bower before grunt prompts to resolve a dependency conflict which otherwise causes grunt to fail
bower install
grunt build
python -m SimpleHTTPServer 8000
```

### Running

1. Open your browser to `http://localhost:8000/`
2. Log in using your github account. Go to "Settings" and copy your "User Identifier"
3. In a fresh shell, `cd` into the root of the rill repo.
4. Start the rill runtime.
   ```
   python -m rill.runtime --user-id <USER_ID>
   ```
5. Back in the browser, create a new project in NoFlo selecting the rill runtime
6. Green arrows should appear on the top-right menu, right before
   `ws:\\localhost:3569`

## Testing

First install the test suite:
```
nmp install -g fbp-protocol
```

Then, from the repo directory, run the tests
```
fbp-test
```
