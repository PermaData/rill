from rill.engine.component import *
from rill.fn import synced


@component
@inport("IN", description="Packets to be written", type=str)
@inport("FILEPATH", description="File name", type=str)
@outport("OUT", optional=True, description="Output port, if connected",
         type=str)
@must_run
def WriteLines(IN, FILEPATH, OUT):
    """
    Write each packet from IN to a line FILEPATH, and also pass it through to
    OUT.
    """
    # FIXME: consider adding a buffer to reduce IO

    filename = FILEPATH.receive_once()
    if filename is None:
        return

    logger.info("Writing file {}".format(filename))
    try:
        with open(filename, 'w') as f:
            for p in IN:
                # long_wait_start(_timeout)

                try:
                    f.write(p.get_contents() + '\n')
                except IOError as e:
                    logger.error("Failed reading file {}: {}".format(
                        filename, str(e)))

                # long_wait_end()
                OUT.send(p)

    except IOError as e:
        logger.error("Failed writing file {}: {}".format(
            filename, str(e)))


@component
@inport("IN", description="Packets to be written", type=str)
@inport("FILEPATH", description="File name", type=str)
@outport("OUT", optional=True, description="Output port, if connected",
         type=str)
@must_run
def Write(IN, FILEPATH, OUT):
    """
    Write each packet from IN to FILEPATH.

    Each packet is written to its own file (open/write/close), thus to avoid
    data being overwritten IN and FILEPATH should be streams of the same length
    """
    # p = FILEPATH.receive()
    # if p is None:
    #     return
    # filename = p.get_contents()
    #
    # p = IN.receive()
    # if p is None:
    #     return
    #
    # logger.info("Writing file {}".format(filename))
    # try:
    #     with open(filename, 'w') as f:
    #         f.write(p.get_contents())
    # except IOError as e:
    #     logger.error("Failed writing file {}: {}".format(
    #         filename, str(e)))
    # OUT.send(p)

    for pfile, ptext in synced(FILEPATH, IN):
        filename = pfile.get_contents()
        pfile.drop()
        logger.info("Writing file {}".format(filename))
        try:
            with open(filename, 'w') as f:
                f.write(ptext.get_contents())
        except IOError as e:
            logger.error("Failed writing file {}: {}".format(
                filename, str(e)))
        OUT.send(ptext)


@component
@outport("OUT", description="Generated packets", type=str)
@inport("FILEPATH", description="File name", type=str)
def ReadLines(FILEPATH, OUT):
    """
    Creates a packets for each line in a file.
    """
    # filename = FILEPATH.receive_once()
    # if filename is None:
    #     return
    #
    for filename in FILEPATH.iter_contents():

        logger.info("Reading file {}".format(filename))
        try:
            with open(filename, 'r') as f:
                for line in f:
                    if OUT.is_closed():
                        break
                    OUT.send(line.rstrip('\n'))
        except IOError as e:
            logger.error("Failed reading file {}: {}".format(
                filename, str(e)))
