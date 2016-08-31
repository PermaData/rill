import rill


def variable_map(headers, filename, name_overrides, map):
    """
    takes a list of headers and maps those headers to column indices
    if given name_overrides, it replaces selected header entries with given names
    name_overrides should recieve either a sequence that has a one-to-one map with the original header sequence
        or a dictionary of name: new name
    """
    """
    map = {}
    if name_overrides is not given:
        name_overrides = [None for each in headers]
    for i, (name, override) in enumerate(zip(headers, name_overrides)):
        if override is not None:
            map.update(override=i)
        else:
            map.update(name=i)
    send map to port
    """
    """

    """
