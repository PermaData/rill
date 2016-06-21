from rill import *
from rill.fn import range
import itertools


@component
@inport("IN", description="Packets to be displayed")
@outport("OUT", required=False, description="Output port, if connected")
def Output(IN, OUT):
    """displays the content of incoming IPs open and close brackets"""
    level = 1
    for p in IN:
        if p.get_type() == Packet.Type.OPEN:
            logger.info("OPEN({})".format(level))
            level += 1
            break
        elif p.get_type() == Packet.Type.CLOSE:
            level -= 1
            logger.info("CLOSE({})".format(level))
            break
        else:
            logger.info(repr(p.get_contents()))
        OUT.send(p)


@component
@outport("OUT")
@inport("CONST")
def Inject(CONST, OUT):
    """Inject CONST from IIP to the IP OUT"""
    c = CONST.receive_once()
    if c is None:
        return
    if not OUT.is_closed():
        OUT.send(c)


@component
@outport("OUT", description="Single packet containing blank", type=str)
def Kick(OUT):
    """
    Component to generate a single packet with a single blank character

    mostly used for debugging.
    """
    OUT.send(" ")


@component
@inport("IN")
@outport("OUT")
def Passthru(IN, OUT):
    """Pass a stream of packets to an output stream"""
    # make it a non-looper - for testing
    p = IN.receive()
    OUT.send(p)


@component
@inport("IN")
@inport("COUNT", type=int,
        description="Number of times to repeat. If None, the first packet on "
                    "IN repeats forever")
@outport("OUT")
def Repeat(IN, COUNT, OUT):
    """Repeat each packet from IN to OUT, COUNT times"""
    count = COUNT.receive_once()
    kwargs = {}
    if count is not None:
        kwargs['times'] = count

    # FIXME: non-loopers have some inherent problems.  for one, this component
    # is deactivated after each input packet still holding several output packets

    # p = IN.receive()
    # if p is None:
    #     return
    #
    # for i in range(count):
    #     if OUT.is_closed():
    #         break
    #     OUT.send(p.clone())

    for packet in IN.iter_packets():
        for p in itertools.repeat(packet, **kwargs):
            if OUT.is_closed():
                break
            OUT.send(p.clone())
        packet.drop()


@component
@inport("IN")
@outport("OUT")
def Copy(IN, OUT):
    """Copy all incoming packets to output"""
    for p in IN.iter_contents():
        OUT.send(p.clone())


@component
@inport("IN", description="Stream of packets to be discarded")
def Discard(IN):
    """Discards all incoming packets"""
    IN.receive().drop()


# @component
@inport("IN", description="Incoming stream")
@outport("OUT", description="Stream being passed through", required=False)
@outport("COUNT", description="Count packet to be output", type=int)
@must_run
# def Counter(IN, OUT, COUNT):
#     """Component to count a stream of packets, and output the result on the
#     COUNT port.
#     """
#     count = 0
#     for p in IN.iter_packets():
#         count += 1
#         OUT.send(p)
#     COUNT.send(count)
class Counter(Component):
    """Component to count a stream of packets, and output the result on the
    COUNT port.
    """
    def execute(self):
        # FIXME: is it possible for a looper to be terminated and then
        # reactivated? if so, self.count will be wrong.  it is designed this
        # way to allow pause/resume (i.e. fault-tolerance)
        for p in self.ports.IN.iter_packets():
            self.count += 1
            self.ports.OUT.send(p)
        self.ports.COUNT.send(self.count)

    def init(self):
        self.count = 0


@component
@inport("IN", description="Packets to be sorted")
@inport("MAX", description="Maximum number of packets to be sorted", type=int)
@outport("OUT", description="Output port")
def Sort(IN, MAX, OUT):
    """
    Sort a stream of Packets to an output stream
    """
    max = MAX.receive_once(9999) - 1

    array = []
    for i, p in enumerate(IN.iter_packets()):
        if i == max:
            break
        array.append(p)

    # this is designed to stream results during the sort operation, which is
    # theoretically "better" than sorting everything up front with the sort()
    # function, though in practice it's likely that sort() is faster in most
    # cases by virtue of being implemented in C.
    j = 0
    k = len(array)
    n = k  # no. of packets to be sent out

    while n > 0:
        curr_min = None

        for i in range(k):
            if array[i] is not None:
                s = array[i].get_contents()
                if curr_min is None or s < curr_min:  # was `cmp(s, t) < 0`
                    j = i
                    curr_min = s
        # if (array[j] is None) break
        OUT.send(array[j])
        array[j] = None
        n -= 1


@component
@inport("IN", description="Packets to be sorted", type=int)
@inport("MAX", description="Maximum number of packets to be sorted", type=int,
        required=True)
@outport("OUT", description="Output port")
def Cap(IN, MAX, OUT):
    """
    Cap a numeric stream by closing IN when a value greater than or equal to
    MAX is received
    """
    max = MAX.receive_once()
    for p in IN.iter_packets():
        if p.get_contents() >= max:
            p.drop()
            IN.close()
            break
        else:
            OUT.send(p)


@inport("IN", description="Value to be captured")
class Capture(Component):
    """
    Capture a single value and store it on an internal attribute.
    Useful for testing and debugging.
    """
    def execute(self):
        captured = self.ports.IN.receive_once()
        if captured is None:
            return

        self.value = captured

    def init(self):
        self.value = None
