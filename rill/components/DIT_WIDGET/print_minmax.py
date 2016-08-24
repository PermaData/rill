#! /usr/bin/python

import sys
import getopt

from .common import readwrite as io
from .common import definitions as d
from .printfamily import prints as p

__all__ = ['print_minmax']


def print_minmax(infile, outfile):
    """Prints the maximum and minimum values along with their locations.
    Inputs:
        infile: name of file to read data from
        outfile: name of file to write mins and maxes to
    Outputs:
        Pushes the location and value of maxima and minima to outfile.
    """
    data = io.pull(infile, float)

    # Ignore missing values
    filtered = [x for x in data if x not in d.missing_values]
    mini = min(filtered)
    maxi = max(filtered)

    out = []
    for which, target in enumerate([mini, maxi]):
        out.append(('Location', ['Minimum', 'Maximum'][which]))
        for place, val in enumerate(data):
            if(val == target):
                out.append((place, val))

    rv = p.interpret_out(out)
    # Number of records is useful for Fortran but not pure Python
    rv.insert(0, len(out))

    io.push(rv, outfile)


def parse_args(args):
    def help():
        print('print_minmax.py -i <input file> -o <output file>')

    infile = None
    outfile = None

    options = ('i:o:', ['input', 'output'])
    readoptions = zip(['-'+c for c in options[0] if c != ':'],
                      ['--'+o for o in options[1]])

    try:
        (vals, extras) = getopt.getopt(args, *options)
    except getopt.GetoptError as e:
        print(str(e))
        help()
        sys.exit(2)

    for (option, val) in vals:
        if (option in readoptions[0]):
            infile = val
        elif (option in readoptions[1]):
            outfile = val

    if (any(val is None for val in [infile, outfile])):
        help()
        sys.exit(2)

    return infile, outfile

#                 PERFORM FUNCTION USING COMMAND-LINE OPTIONS                 #
if (__name__ == '__main__'):
    args = parse_args(sys.argv[1:])

    print_max(*args)
