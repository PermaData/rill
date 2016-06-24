"""
Server to register rill with ui

A UI can interact with many runtimes.  The registry provides a single resource
to query all registered runtimes.
"""

from bottle import route, request, response, run
from urlparse import urlparse
from datetime import datetime
import json


def create_routes(host, port):
    """
    Define registry routes
    """
    @route("/runtimes/", method=['OPTIONS', 'GET'])
    def get_registry():
        """
        Get data about rill runtime
        """
        from rill.runtime import Runtime

        response.set_header('Access-Control-Allow-Origin', '*')
        response.set_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        response.set_header('Allow', 'GET, OPTIONS')
        response.set_header(
            'Access-Control-Allow-Headers',
            'Content-Type, Authorization'
        )

        if request.method == 'OPTIONS':
            return 'GET,OPTIONS'

        response.content_type = 'application/json'

        runtime = Runtime()

        runtime_meta = runtime.get_runtime_meta()
        runtime_meta['address'] = address = 'ws://{}:{}'.format(host, port)
        runtime_meta['protocol'] = 'websocket'
        runtime_meta['id'] = 'rill_' + urlparse(address).netloc
        runtime_meta['seen'] = str(datetime.now())
        return json.dumps([runtime_meta])


def serve_registry(registry_host, registry_port, runtime_host, runtime_port,
                   **kwargs):
    """
    Run the runtime registry http process.

    Parameters
    ----------
    registry_host : str
    registry_port : int
    runtime_host : str
    runtime_port : int
    """

    print('registry running at {}:{}'.format(registry_host, registry_port))
    create_routes(runtime_host, runtime_port)
    run(host=registry_host, port=registry_port, **kwargs)

if __name__ == "__main__":
    serve_registry('localhost', 8080, 'localhost', 3569)
