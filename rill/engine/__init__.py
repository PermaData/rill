from gevent import monkey

print("Performing gevent monkey-patching")

monkey.patch_all()

import logging

logging.basicConfig(
    # level=logging.DEBUG,
    format='%(levelname)-5s : %(message)s')
