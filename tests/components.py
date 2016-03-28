from rill.engine.component import *
from rill.fn import range

@component
@outport("OUT", description="Generated stream", type=str)
@inport("COUNT", description="Count of packets to be generated", type=int,
        default=1)
def GenerateTestData(COUNT, OUT):
    """"Generates stream of packets under control of a counter"""
    count = COUNT.receive_once()
    if count is None:
        return

    for i in range(count, 0, -1):
        s = "%06d" % i
        if not OUT.send(s):
            break


@component
@outport("OUT", description="Generated stream", type=str)
@inport("COUNT", description="Count of packets to be generated", type=int)
def GenerateTestDataDumb(COUNT, OUT):
    """"Generates stream of packets under control of a counter

    Fails to break if OUT is closed
    """
    count = COUNT.receive_once()
    if count is None:
        return

    for i in range(count, 0, -1):
        s = "%06d" % i
        OUT.send(s)


@component
@outport("OUT", optional=True, fixed_size=4, description="Generated stream",
         type=str, array=True)
@inport("COUNT", description="Count of packets to be generated",
        type=int)
def GenerateOptionalFixedArray(COUNT, OUT):
    """"Generates stream of packets under control of a counter"""
    count = COUNT.receive_once()
    if count is None:
        return

    for outport in OUT:
        logger.info("writing to port %s" % outport)
        for i in range(count, 0, -1):
            s = "%06d" % i
            if outport.is_closed():
                break
            outport.send(s)


@component
@outport("OUT", optional=True, description="Generated stream",
         type=str, array=True)
@inport("COUNT", description="Count of packets to be generated",
        type=int)
def GenerateArray(COUNT, OUT):
    """"Generates stream of packets under control of a counter"""
    count = COUNT.receive_once()
    if count is None:
        return

    for outport in OUT:
        logger.info("writing to port %s" % outport)
        for i in range(count, 0, -1):
            s = "%06d" % i
            if outport.is_closed():
                break
            outport.send(s)


@component
@outport("OUT", optional=False, fixed_size=2, description="Generated stream",
         type=str, array=True)
@inport("COUNT", description="Count of packets to be generated",
        type=int)
def GenerateFixedSizeArray(COUNT, OUT):
    """"Generates stream of packets under control of a counter"""
    count = COUNT.receive_once()
    if count is None:
        return

    for outport in OUT:
        for i in range(count, 0, -1):
            s = "%06d" % i
            if OUT.is_closed():
                break
            #  if (out_port_array[k].is_connected()):
            outport.send(s)
            #  else:
            #    self.drop(p)
            #


@component
@outport("OUT", type=str)
@inport("COUNT", type=int)
def GenSS(COUNT, OUT):
    """Generates stream of 5-packet substreams under control of a counter
    """
    count = COUNT.receive_once()
    OUT.send(Packet.Type.OPEN)

    for i in range(count):
        s = "%06d" % (count - i)
        OUT.send(s)
        if i < count - 1:  # prevent empty bracket pair at end
            if i % 5 == 5 - 1:
                OUT.send(Packet.Type.CLOSE)
                OUT.send(Packet.Type.OPEN)
    OUT.send(Packet.Type.CLOSE)
