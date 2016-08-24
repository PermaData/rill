"""Counts the number of valid (non-empty) records in a column file."""

import sys
import getopt

from .common import readwrite as io
from .common import definitions as d

__all__ = ['count_records']


def count_records(infile, outfile):
    """Count how many valid records there are."""
    data = io.pull(infile, float)

    out = 0
    for val in data:
        if (val not in d.missing_values):
            out += 1

    io.push([out], outfile)


def parse_args(args):
    def help():
        print('minsec_to_decimal.py -i <input file> -o <output file>')

    infile = None
    outfile = None

    options = ('i:o:',
               ['input', 'output'])
    readoptions = zip(['-'+c for c in options[0] if c != ':'],
                      ['--'+o for o in options[1]])

    try:
        (vals, extras) = getopt.getopt(args, *options)
    except getopt.GetoptError as e:
        print(str(e))
        help()
        sys.exit(2)

    for (option, value) in vals:
        if (option in readoptions[0]):
            infile = value
        elif (option in readoptions[1]):
            outfile = value

    if (any(val is None for val in [infile, outfile])):
        help()
        sys.exit(2)

    return infile, outfile

#                 PERFORM FUNCTION USING COMMAND-LINE OPTIONS                 #
if (__name__ == '__main__'):
    args = parse_args(sys.argv[1:])

    count_records(*args)
