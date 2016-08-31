import rill


def file_manager(filenames, current):
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
