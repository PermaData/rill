from ..common import readwrite as io
from ..common import definitions as d


def arithmetic(infile, outfile, constant, operation, errorcase):
    """Perform an arithmetic operation on data.

    infile: name of the input file
        string
    outfile: name of the output file
        string
    constant: the constant to add/multiply/divide
        numeric
    operation: the operation to perform
        function
            inputs: datapoint, constant
            outputs: numeric result of the operation between these two
    errorcase: determines if the operation is invalid, e.g. division by zero
        function
            inputs: datapoint, constant
            outputs: boolean, if the operation is an error or not

    Returns nothing, but writes the results into outfile
    """

    # Get data from infile
    data = io.pull(infile, float)

    # Calculate modified values
    out = []
    for val in data:
        if(errorcase(val, constant) or val in d.missing_values):
            out.append(val)
        else:
            out.append(operation(val, constant))

    # Write result values to outfile
    io.push(out, outfile)
