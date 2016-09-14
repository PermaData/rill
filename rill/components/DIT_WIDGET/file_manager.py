import rill


@rill.component
@rill.inport('filenames')
@rill.outport('current')
@rill.outport('FID')
def file_manager(filenames, current, FID):
    """
    takes in a collection of file names through that port
    does some preprocessing? at least verify that the file exists
    perhaps copy the original file into a temporary one?
    sends the current file that needs to be processed to the other port
    """
    """
    for name in filenames:
        try:
            f = open(name)
            current.send(name)
        except FileNotFoundError:
            drop severity to a warning and don't pass the file on
    """
    for name in filenames.iter_contents():
        identifier = 1
        try:
            f = open(name)
            f.close()
            current.send(name)
            FID.send(identifier)
            identifier += 1
        except FileNotFoundError:
            # TODO: Make this send to some log instead of the console
            print('The file {f} was not found.'.format(f=name))
