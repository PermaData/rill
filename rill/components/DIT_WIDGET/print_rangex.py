#! /usr/bin/python

import sys
import getopt

from .printfamily import prints as p

__all__ = ['print_rangex']


def print_rangex(infile, outfile, threshold):
    """Prints all values between two thresholds."""
    p.print_conditional(infile, outfile, threshold,
                        lambda x, y: x > y[0] and x < y[1])


def parse_args(args):
    def help():
        print('print_rangex.py -i <input file> -o <output file> -l <lower bound> -u <upper bound>')
        print('Prints values within a range')


    infile = None
    outfile = None
    lower = None
    upper = None

    options = ('i:o:l:u:',
                ['input', 'output', 'lower', 'upper'])
    readoptions = zip(['-'+c for c in options[0] if c != ':'],
                      ['--'+o for o in options[1]])

    try:
        (vals, extras) = getopt.getopt(args, *options)
    except getopt.GetoptError as e:
        print(str(e))
        help()
        sys.exit(2)

    threshold = [0, 0]
    for (option, val) in vals:
        if (option in readoptions[0]):
            infile = val
        elif (option in readoptions[1]):
            outfile = val
        elif (option in readoptions[2]):
            lower = float(val)
            threshold[0] = lower
        elif (option in readoptions[3]):
            upper = float(val)
            threshold[1] = upper

    if (any(val is None for val in [infile, outfile, upper, lower])):
        help()
        sys.exit(2)

    return infile, outfile, threshold

#                 PERFORM FUNCTION USING COMMAND-LINE OPTIONS                 #
if (__name__ == '__main__'):
    args = parse_args(sys.argv[1:])

    print_le(*args)
