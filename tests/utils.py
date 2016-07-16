from rill.engine.port import is_null_port


def names(ports, include_null=False):
    return [x.name for x in ports if include_null or not is_null_port(x)]
