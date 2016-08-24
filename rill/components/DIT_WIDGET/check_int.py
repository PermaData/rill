"""Checks whether all the values in a column file are integers."""

import sys
import getopt

from .common import readwrite as io

__all__ = ['check_ints']


def check_ints(infile, outfile):
    """Check whether a value is not an integer."""
    data = io.pull(infile, float)

    out = []
    for (i, val) in enumerate(data):
        if (abs(val) % 1 > .000000001):
            out.append('{0}: {1}'.format(i + 1, val))

    io.push(out, outfile)


def parse_args(args):
    def help():
        print('check_int.py -i <input CSV file> -o <output csv file>')

    infile = None
    outfile = None

    options = ('i:o:', ['input', 'output'])
    readoptions = zip(['-' + c for c in options[0] if c != ':'],
                      ['--' + o for o in options[1]])

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

    check_ints(*args)
