from collections import OrderedDict
from tests.components import PassthruPerson

def test_schematics_port():
    spec = PassthruPerson.get_spec()
    inports = spec['inPorts']

    assert {
        'required': False,
        'addressable': False,
        'type': 'bang',
        'id': 'wait',
        'description': ''
    } in inports
    assert {
        'required': False,
        'schema': {
            'required': ['name'],
            'type': 'object',
            'properties': OrderedDict([
                ('name', {'type': 'string'}),
                ('age', {'default': 0, 'type': 'int'})
            ]),
            'title': 'Person'
        },
        'addressable': False,
        'id': 'IN',
        'description': ''
    } in inports
