import itertools

import rill


@rill.component
@rill.inport('headers')
@rill.inport('filename')
@rill.inport('name_overrides')
@rill.outport('name_map')
def variable_map(headers, filename, name_overrides, name_map):
    """
    USING NAME_OVERRIDES IS NOT CURRENTLY RECOMMENDED
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
    for name, header in zip(filename.iter_contents(), headers.iter_contents()):
        _name_map = {}
        if (name_overrides.is_empty()):
            overrides = itertools.repeat(None)
        else:
            overrides = next(name_overrides.iter_contents())
        for i, (head, override) in zip(header, overrides):
            if (override is not None):
                _name_map.update({override: i})
            else:
                _name_map.update({head: i})
        name_map.send(_name_map)
