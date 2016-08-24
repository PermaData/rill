from ..common import readwrite as io
from ..common import definitions as d


def replace_conditional(infile, outfile, threshold, value, comparison):
    """Replace a range of values within a file.

    Inputs:
        infile: A string naming the input file.
        outfile: A string naming the output file.
        threshold: The extreme value(s) of the range. Single for
            greater/less or 2-element tuple for range. This tuple
            should be of the form (lower bound, upper bound).
        value: The value to replace all of the selected data by.
        comparison: The function that determines whether a given value
            should be printed (e.g. num > threshold). This function
            should take (number, threshold) and return True if the
            number should be selected and False otherwise.
    Outputs:
        Prints the values post-replacement to outfile.
    """
    # Read data from infile
    data = io.pull(infile, float)

    out = [0]*len(data)

    count = 0
    for (i, num) in enumerate(data):
        if(comparison(num, threshold) and num not in d.missing_values):
            # Replace value if it meets the condition and is not missing
            out[i] = value
            count += 1
        else:
            # Or just pass through the value unchanged
            out[i] = num
    # Number of replacements made is useful for FORTRAN but not pure python
    out.insert(0, count)
    
    # Write the replaced values to the output file
    io.push(out, outfile)
