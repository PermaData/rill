import csv

import rill


def column_extract(name_map, desired_names, temp_file, temp_map):
    """
    name_map is the dictionary of names: column indices
    desired_names is a sequence of the names of the columns that you wish to include in the temporary input csv file
    builds a map of the temporary file
    constructs a temp_file name
    writes the specified columns to the output file and sends the filename along
    """
    """
    open temp_file, 'w'
    open in_file
    input = csv.reader(in_file)
    output = csv.reader(out_file)
    for line in input:
        to_write = []
        for name in desired_names:
            to_write.append(line[name_map[name]])
        output.writerow(to_write)
    """
