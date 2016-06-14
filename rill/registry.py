"""
Server to register rill with ui
"""

from bottle import route, request, response, run
from urlparse import urlparse
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
        from rill.runtime import Runtime, create_runtime_id

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
        runtime_meta['id'] = create_runtime_id(
            urlparse(address).netloc)
        return json.dumps([runtime_meta])

def run_registry(host, port, **kwargs):
    """
    Define registry routes

    Parameters
    ----------
    host : str
    port : int
    """

    print('registry running at {}:{}'.format(host, port))
    run(host=host, port=port, **kwargs)

if __name__ == "__main__":
    create_routes('localhost', '3569')
    run_registry('localhost', '8080')
