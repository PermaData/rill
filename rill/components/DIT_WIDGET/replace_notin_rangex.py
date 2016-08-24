#! /usr/bin/python

import sys
import getopt

from .replacefamily import replacements as r

__all__ = ['replace_notin_rangex']


def replace_notin_rangex(infile, outfile, threshold, value):
    """Replace values not within the range defined by threshold with value within a column file."""
    r.replace_conditional(infile, outfile, threshold, value,
                          lambda x, y: x < y[0] or x > y[1])


def parse_args(args):
    def help():
        print('replace_notin_rangex.py -i <input file> -o <output file> -l <lower bound> -u <upper bound> -v <replacement value>')
        print('Prints values outside of a range')


    infile = None
    outfile = None
    lower = None
    upper = None
    value = None

    options = ('i:o:l:u:v:',
               ['input', 'output', 'lower', 'upper', 'value'])
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
        elif (option in readoptions[4]):
            value = float(val)

    if (any(val is None for val in [infile, outfile, upper, lower, value])):
        help()
        sys.exit(2)

    return infile, outfile, threshold, value

#                 PERFORM FUNCTION USING COMMAND-LINE OPTIONS                 #
if (__name__ == '__main__'):
    args = parse_args(sys.argv[1:])

    replace_notin_rangex(*args)
