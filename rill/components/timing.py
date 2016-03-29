
import time
from rill import *


@component
@outport("OUT")
@inport("INTERVAL", type=float)
def Heartbeat(INTERVAL, OUT):
    """Generates a packet every 'n' seconds"""
    # receive interval in seconds
    itvl = INTERVAL.receive_once()
    if itvl is None:
        return

    # when the send returns False, component closes down
    while True:
        if not OUT.send(" "):
            break
        time.sleep(itvl)


@component
@outport("OUT")
@inport("IN")
@inport("DELAY", type=float, optional=False)
def SlowPass(IN, DELAY, OUT):
    """
    Pass a stream of packets to an output stream with a delay between packets
    """
    delay = DELAY.receive_once()
    time.sleep(delay)
    for p in IN:
        # in order to remain fault-tolerant, we have to sleep after we send
        # and not before, or else we could be terminated while holding a
        # packet.
        OUT.send(p)
        time.sleep(delay)


# FIXME: I think this can be replaced with a Copy using the NULL input port

@component
@outport("OUT")
@inport("IN")
@inport("TRIGGER")
def Gate(IN, TRIGGER, OUT):
    """Copies incoming packets - delayed until trigger received"""
    # receive trigger
    tp = TRIGGER.receive_once()
    if tp is None:
        return

    logger.info("got trigger")

    rp = IN.receive()
    logger.info("rp = '" + rp + "'")

    if rp is None:
        return
    # IN.close()

    # pass output
    OUT.send(rp.get_contents())
    rp.drop()
