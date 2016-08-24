#! /usr/bin/python
"""Divides all numeric values in a column file by a constant."""
import sys
import getopt

from .mathfamily import arithmetic as a

__all__ = ['div_const']


def div_const(infile, outfile, constant):
    # Divides all values in infile by constant and writes the result to
    # outfile.
    a.arithmetic(infile, outfile, constant, lambda x, y: x/y,
                 lambda x, y: y == 0)


def parse_args(args):
    def help():
        print('div_const.py -i <input file> -o <output file> -n <number>')

    infile = None
    outfile = None
    constant = None

    options = ('i:o:n:',
               ['input', 'output', 'constant'])
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
        elif (option in readoptions[2]):
            constant = float(value)

    if (any(val is None for val in [infile, outfile, constant])):
        help()
        sys.exit(2)

    return infile, outfile, constant

#                 PERFORM FUNCTION USING COMMAND-LINE OPTIONS                 #
if (__name__ == '__main__'):
    args = parse_args(sys.argv[1:])

    div_const(*args)
