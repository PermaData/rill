from rill.engine.utils import patch
patch()
import argparse
from rill.runtime import DEFAULTS, Runtime, serve_runtime


def main():
    # Parse arguments
    argp = argparse.ArgumentParser(
        description='Runtime that responds to commands sent over the network, '
                    'managing and executing graphs.')
    argp.add_argument(
        '--host', default=DEFAULTS['host'], metavar='HOSTNAME',
        help='Listen host for websocket (default: %(host)s)' % DEFAULTS)
    argp.add_argument(
        '--port', type=int, default=DEFAULTS['port'], metavar='PORT',
        help='Listen port for websocket (default: %(port)d)' % DEFAULTS)
    argp.add_argument(
        '--registry-host', default=DEFAULTS['registry_host'],
        metavar='HOSTNAME',
        help='Listen host for registry (default: %(registry_host)s)' % DEFAULTS)
    argp.add_argument(
        '--registry-port', type=int, default=DEFAULTS['registry_port'],
        metavar='PORT',
        help='Listen port for registry (default: %(registry_port)d)' % DEFAULTS)
    argp.add_argument(
        '--log-file', metavar='FILE_PATH',
        help='File to send log output to (default: none)')
    argp.add_argument(
        '-m', '--module', dest='modules', action='append', default=[],
        help='Module to load')
    argp.add_argument(
        '-v', '--verbose', action='store_true',
        help='Enable verbose logging')

    args = argp.parse_args()

    # Configure logging
    # utils.init_logger(filename=args.log_file,
    #                   default_level=(logging.DEBUG if args.verbose else
    # logging.INFO),
    #                   logger_levels={
    #                       'requests': logging.WARN,
    #                       'geventwebsocket': logging.INFO,
    #                       'sh': logging.WARN,
    #
    #                       'rill.core': logging.INFO,
    #                       'rill.components': logging.INFO,
    #                       'rill.executors': logging.INFO
    #                   })

    runtime = Runtime()

    for modname in args.modules:
        runtime.register_module(modname)

    serve_runtime(runtime, args.host, args.port, args.registry_host,
                  args.registry_port)


if __name__ == '__main__':
    main()
