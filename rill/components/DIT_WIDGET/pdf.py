#! /usr/bin/python
"""Calculates a probability density function of the numeric data in a column file."""
import sys
import getopt

from .common import readwrite as io
from .common import definitions as d

__all__ = ['pdf']


def pdf(infile, outfile, bins, minmax, lower, upper, outliers, norm):
    """Create a probability density function of the data in infile.
    Inputs:
        infile: the file to read data from
        outfile: the file to write data to
        bins: the number of bins to sort data into
        minmax: If 'auto', sets the range of the distribution to be the
            entire range of the data set. If 'manual', sets the range to
            be defined by [lower, upper].
        outliers: If 'exclude', ignore values that fall outside the
            range. If 'include', values outside the range will be sorted
            into the tail bins.
        norm: If 'raw', returns the number of values that fall in each
            bin. If 'probability', returns the proportion of values that
            fall in each bin.
    Outputs:
        Writes a vector of bin values as determined by the 'norm'
        argument to outfile.
    """

    # The function may be called without options being explicitly passed
    if(norm in d.missing_values):
        norm = 'raw'
    if(minmax in d.missing_values):
        minmax = 'auto'
    if(outliers in d.missing_values):
        outliers = 'exclude'

    data = io.pull(infile, float)

    bins = int(bins)

    # Look only at valid data
    filtered = [x for x in data if x not in d.missing_values]

    if(minmax == 'auto'):
        mini = min(filtered)
        maxi = max(filtered)
        val_range = maxi - mini
    elif(minmax == 'manual'):
        mini = lower
        maxi = upper
        val_range = upper - lower
    else:
        # Error with the minmax option
        print("Use a valid form for the minmax argument")
        return None

    out = [[] for each in range(bins)]
    for val in filtered:
        # Calculate which bin the value should go in.
        loc = ((val - mini) * bins) / val_range
        loc = int(loc)

        if(outliers == 'include'):
            if(loc < 0):
                # The value is below the lower limit on range
                loc = 0
            elif(loc >= bins):
                # The value is above the upper range limit
                loc = bins - 1
            out[loc].append(val)  # Place the value in the bin
        elif(outliers == 'exclude'):
            if(loc >= 0 and loc < bins):
                out[loc].append(val)
            elif(val == maxi):
                # The maximum value is sometimes sorted into the bin
                # beyond the end
                out[-1].append(val)
        else:
            # Error with the outlier option
            print("Use a valid form for the outlier argument")
            return None

    norms = {'raw': raw,
             'probability': probability
             }
    if(norm not in norms):
        # Error with the norm option
        print("Use a valid form for the norm argument")
        return None

    out = norms[norm](out)

    out.insert(0, 'Minimum: {0}'.format(mini))
    out.append('Maximum: {0}'.format(maxi))

    # Push the correct data to outfile
    io.push(out, outfile)


def raw(data):
    # Return the number of data points that fall in each bin
    return [len(group) for group in data]


def probability(data):
    # Return the relative probabilities of the bins
    npoints = sum(raw(data))
    return [float(len(group)) / npoints for group in data]


def parse_args(args):
    def help():
        print('pdf.py -i <input file> -o <output file> -b <number of bins> -m <min/max behavior> -l <input minimum> -u <input maximum> -t <outlier behavior> -n <output behavior>')

    infile = None
    outfile = None
    bins = None

    minmax = ''
    outliers = ''
    norm = ''

    lower = 0
    upper = 0

    options = ('i:o:b:m:l:u:t:n:',
               ['input', 'output', 'bins', 'minmax', 'lower', 'upper',
                'outliers', 'norm'])
    # readoptions is a list of -short_option --long_option pairs in
    # the order shown above.
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
            bins = int(value)
        elif (option in readoptions[3]):
            minmax = value
        elif (option in readoptions[4]):
            lower = float(value)
        elif (option in readoptions[5]):
            upper = float(value)
        elif (option in readoptions[6]):
            outliers = value
        elif (option in readoptions[7]):
            norm = value

    if (any(val is None for val in [infile, outfile, bins])):
        help()
        sys.exit(2)

    return infile, outfile, bins, minmax, lower, upper, outliers, norm

#                 PERFORM FUNCTION USING COMMAND-LINE OPTIONS                 #
if (__name__ == '__main__'):
    args = parse_args(sys.argv[1:])

    pdf(*args)
