#! /usr/bin/python

import sys
import getopt

from .replacefamily import replacements as r

__all__ = ['replace_gt']


def replace_gt(infile, outfile, threshold, value):
    """Replace values greater than threshold with value within a column file."""
    r.replace_conditional(infile, outfile, threshold, value,
                          lambda x, y: x > y)


def parse_args(args):
    def help():
        print('replace_gt.py -i <input file> -o <output file> -t <threshold> -v <replacement value>')
        print('Replaces values greater than threshold with replacement')


    infile = None
    outfile = None
    threshold = None
    value = None

    options = ('i:o:t:v:',
                ['input', 'output', 'threshold', 'value'])
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
        elif (option in readoptions[3]):
            value = float(val)

    if (any(val is None for val in
            [infile, outfile, threshold, value])):
        help()
        sys.exit(2)

    return infile, outfile, threshold, value

#                 PERFORM FUNCTION USING COMMAND-LINE OPTIONS                 #
if (__name__ == '__main__'):
    args = parse_args(sys.argv[1:])

    replace_gt(*args)
