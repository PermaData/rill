from rill import *


@component
@inport('IN')
@outport('OUT')
def First(IN, OUT):
    """
    Pass along only the first packet in the stream.
    """
    value = IN.receive_once()
    OUT.send(value)


@component
@outport("ACC")
@outport("REJ", optional=True)
@inport("IN")
@inport("NUMBER", optional=False)
def SelNthItem(IN, NUMBER, ACC, REJ):
    """Select from IN one packet by NUMBER (0 means first), sending via ACC,
    rejected packets via REJ"""
    selector = NUMBER.receive_once()

    for i, p in enumerate(IN):
        if i == selector:
            ACC.send(p)
        else:
            REJ.send(p)
