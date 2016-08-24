#! /usr/bin/python

import sys
import getopt

from .printfamily import prints as p

__all__ = ['print_gt']


def print_gt(infile, outfile, threshold):
    """Print all values greater than a threshold."""
    p.print_conditional(infile, outfile, threshold, lambda x, y: x > y)


def parse_args(args):
    def help():
        print('print_gt.py -i <input file> -o <output file> -t <threshold> -v <replacement value>')
        print('Prints values greater than threshold')


    infile = None
    outfile = None
    threshold = None

    options = ('i:o:t:',
               ['input', 'output', 'threshold'])
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
        elif (option in readoptions[2]):
            threshold = float(val)

    if (any(val is None for val in [infile, outfile, threshold])):
        help()
        sys.exit(2)

    return infile, outfile, threshold

#                 PERFORM FUNCTION USING COMMAND-LINE OPTIONS                 #
if (__name__ == '__main__'):
    args = parse_args(sys.argv[1:])

    print_gt(*args)
