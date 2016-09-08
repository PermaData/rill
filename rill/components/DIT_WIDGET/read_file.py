import csv

import rill


@rill.component
@rill.inport('flowID')
@rill.inport('filename')
@rill.outport('headers')
@rill.outport('csv_file')
def read_file(flowID, filename, headers, csv_file):
    """
    recieves a filename from that port
    reads the headers (if any) and sends them as a single list to that port
    Verify file integrity, if it fails give a warning and do not pass the filename along
    """
    """
    open(filename)
    read first line
    if first line is just numbers, assign headers that are just the column indices
    otherwise use the first line as the headers
    construct output file name (going to be the base input file from now on)
    read the data from the main file and write that to the output file in a standard format
    send out the output file name and the headers
    """
    ID = flowID.receive_once()
    for name in filename.iter_contents():
        # TODO: does not clean the original filename before using it in the new
        newname = '{ID}_In_{NAME}.csv'.format(ID=ID, NAME=name)
        with open(name) as IN, open(newname, 'w') as OUT:
            data = csv.reader(IN, quoting=csv.QUOTE_NONNUMERIC)
            output = csv.writer(OUT, quoting=csv.QUOTE_NONNUMERIC)
            firstline = True
            for line in data:
                if (firstline):
                    firstline = False
                    unsuccessful = False
                    for item in line:
                        if (not isinstance(item, str)):
                            # Not all of the headers are actually headers
                            # So send column indices as strings instead
                            headers.send(list(str(i) for i in range(len(line))))
                            output.writerow(line)
                            unsuccessful = True
                            break
                    if (not unsuccessful):
                        headers.send(line)
                else:
                    output.writerow(line)
        csv_file.send(newname)
