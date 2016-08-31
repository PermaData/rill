import rill


def read_file(filename, headers, csv_file):
    """
    recieves a filename from that port
    reads the headers (if any) and sends them as a single list to that port
    Verify file integrity, if it fails give a warning and do not pass the filename along
    """
    """
    open(filename)
    read first line
    if first line is just numbers, assign headers that are just the column indices
    otherwise use the first line as the headers
    construct output file name (going to be the base input file from now on)
    read the data from the main file and write that to the output file in a standard format
    send out the output file name and the headers
    """
