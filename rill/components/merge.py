from rill.engine.component import *
from rill.fn import cycle, synced, eager_merged


@component
@inport("IN", array=True, description="Incoming packets")
@outport("OUT", description="Merged output")
def SubstreamSensitiveMerge(IN, OUT):
    """
    Merge multiple input streams, first-in, first-out, but sensitive to
    substreams
    """
    inport = None
    inports = eager_merged(IN)
    substream_level = 0
    while True:
        if substream_level != 0:
            p = inport.receive()
            if p is None:
                break
        else:
            inport = inports.next_port()
            if inport is None:
                # all elements are drained
                return
            p = inport.receive()
        if p.get_type() == Packet.OPEN:
            substream_level += 1
        elif p.get_type() == Packet.CLOSE:
            substream_level -= 1
        OUT.send(p)


@component
@outport("IN", array=True, description="Incoming packets")
@inport("OUT", description="Merged output")
def RoundRobinMerge(IN, OUT):
    """"Merge multiple input streams, following Round Robin system

    Merges an IP from input array element 0, then one from 1, then one from 2,
    and so on until it cycles back to 0. This continues until the first end of
    stream.

    The assumption is that all input streams have the same number of IPs
    """
    for inport in cycle(IN.ports()):
        p = inport.receive()
        if p is None:
            IN.close()
            return
        OUT.send(p)


@component
@inport("IN", array=True)
@outport("OUT")
def Concatenate(IN, OUT):
    """Concatenate two or more streams of packets"""
    for inport in IN.ports():
        for packet in inport:
            OUT.send(packet)


@component
@inport("IN", array=True)
@outport("OUT", type=tuple)
def Group(IN, OUT):
    for packets in synced(IN).iter_packets():
        OUT.send(tuple(p.drop() for p in packets))
