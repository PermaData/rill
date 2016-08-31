#! /usr/bin/python
"""Multiplies all numeric values in a column file by a constant."""
import sys
import getopt
import csv

import rill

from .mathfamily import arithmetic as a

__all__ = ['mult_const']


@rill.component
@rill.inport('infile')
@rill.inport('outfile')
@rill.inport('indices')
@rill.inport('constant')
@rill.outport('modified')
def mult_const(infile, outfile, indices, constant, modified):
    # Multiplies all values in infile by constant and writes the result
    # to outfile.
    infile_ = infile.receive_once()
    outfile_ = outfile.receive_once()
    constant_ = constant.receive_once()
    # indices_ = indices.receive_once()
    with open(infile_) as f, open(outfile_, 'w', newline='') as out:
        data = csv.reader(f)
        output = csv.writer(out)
        for row in data:
            for i in indices.iter_contents():
                try:
                    val = float(row[i])
                except ValueError:
                    continue  # Skip this line, it's probably a header
                if (val != -999):
                    row[i] = val * constant_
            output.writerow(row)
    # a.arithmetic(infile_, outfile_, constant_, lambda x, y: x*y,
    #              lambda x, y: False)
    modified.send(outfile_)


def parse_args(args):
    def help():
        print('mult_const.py -i <input file> -o <output file> -n <number>')


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

    mult_const(*args)
