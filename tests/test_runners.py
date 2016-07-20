from mock import Mock
from rill.engine.runner import ComponentRunner


def test_self_starting():
    """
    Components are self-starting if they are explicitly marked as so, or if
    they have only unconnected or static ports.
    """
    comp = Mock(_self_starting=True)
    network = Mock()
    runner = ComponentRunner(comp, network)
    assert runner.self_starting is True
    runner.has_run = True
    assert runner.self_starting is False

    static_port = {
        'is_connected.return_value': True,
        'is_static.return_value': True}

    connected_port = {
        'is_connected.return_value': True,
        'is_static.return_value': False}

    unconnected_port = {
        'is_connected.return_value': False}

    comp = Mock(_self_starting=False,
                inports=[
                    Mock(**static_port),
                    Mock(**unconnected_port)
                ])
    runner = ComponentRunner(comp, network)
    assert runner.self_starting is True
    runner.has_run = True
    assert runner.self_starting is False

    comp = Mock(_self_starting=False,
                inports=[
                    Mock(**static_port),
                    Mock(**connected_port)
                ])
    runner = ComponentRunner(comp, network)
    assert runner.self_starting is False
    runner.has_run = True
    assert runner.self_starting is False
