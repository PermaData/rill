from rill.engine.component import *
from rill.fn import zip, cycle, load_balanced, forked


# TODO: add ability to control number of packets sent to each
@component
@inport("IN", description="Incoming packets")
@outport("OUT", array=True, description="Split output")
def RoundRobinSplit(IN, OUT):
    """"Split an input stream into multiple output streams, following Round
    Robin system
    """
    for outport, p in zip(cycle(OUT), IN.iter_packets()):
        outport.send(p)
        if IN.is_drained():
            outport.close()


@component
@outport("OUT", array=True, description="Replicated packets")
@inport("IN", description="Incoming packets")
def Replicate(IN, OUT):
    """Replicate stream of packets to multiple output streams"""
    for p in IN:
        for outport in OUT:
            outport.send(p.clone())
            if IN.is_drained():
                # this is here to avoid a deadlock with connections with a
                # capacity of 1:
                # one element port delivers its packet, but the receiver can't
                # consume it until it receives a packet from a subsequent
                # element port, so the send blocks which prevetns the loop
                # through element ports from progressing. Deadlock.
                # To solve it, we close element ports as soon as possible to
                # indicate to the receiver to move on, and thus allow the loop
                # here to continue.
                outport.close()
        # forked(OUT).send(p)
        p.drop()


@outport("OUT", array=True, description="Packets being output")
@inport("IN", description="Incoming packets")
def LoadBalance(IN, OUT):
    """
    Sends incoming packets to output array element with smallest backlog
    """

    active_outport = None
    substream_level = 0
    outports = load_balanced(OUT)
    for p in IN.iter_packets():
        if substream_level == 0:
            # find output port with the least number of downstream packets
            active_outport = outports.next_port()
        if p.get_type() == Packet.Type.OPEN:
            substream_level += 1
        elif p.get_type() == Packet.Type.CLOSE:
            substream_level -= 1
        active_outport.send(p)
