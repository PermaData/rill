from ..common import readwrite as io
from ..common import definitions as d


def print_conditional(infile, outfile, threshold, comparison):
    """Print values to a file if they fall in some range.

    Inputs:
        infile: A string naming the input file.
        outfile: A string naming the output file.
        threshold: The extreme value(s) of the range. Single for
            greater/less or 2-element tuple for range. This tuple
            should be of the form (lower bound, upper bound).
        comparison: The function that determines whether a given value
            should be printed (e.g. num > threshold). This function
            should take (number, threshold) and return True if the
            number should be selected and False otherwise.
    Outputs:
        The values selected are printed to outfile.
    """
    # Read data from input
    data = io.pull(infile, float)

    # Build a vector of values to output
    out = []
    for (i, val) in enumerate(data):
        if(comparison(val, threshold) and val not in d.missing_values):
            # Add to output vector iff this value satisfies the comparison
            out.append((i+1, val))
    rv = interpret_out(out)
    # Necessary for Fortran compatibility but not Python
    rv.insert(0,len(out))
    
    # Write the output vector to the output file after formatting
    io.push(rv, outfile)


def interpret_out(data):
    """Makes the output of the above function into strings for writing."""
    out = ['']*len(data)
    for i, pair in enumerate(data):
        if isinstance(pair[0], str):
            # There may be column headers, as in print_max.
            out[i] = pair[0] + ',' + pair[1]
        else:
            # Makes a string out of the (location, value) pairs given
            out[i] = '{0},{1:0.{p}f}'.format(*pair, p=7)
    return out
