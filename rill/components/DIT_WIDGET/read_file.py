import csv
import os

import rill


@rill.component
@rill.inport('filename')
@rill.inport('FID')
@rill.outport('IN')
@rill.outport('FID_out')
def read_file(filename, FID, FID_out, IN):
    # ID = flowID.receive_once()
    # for name in filename.iter_contents():
    #     # TODO: does not clean the original filename before using it in the new
    #     newname = '{ID}_In_{NAME}.csv'.format(ID=ID, NAME=name)
    #     with open(name) as IN, open(newname, 'w') as OUT:
    #         data = csv.reader(IN, quoting=csv.QUOTE_NONNUMERIC)
    #         output = csv.writer(OUT, quoting=csv.QUOTE_NONNUMERIC)
    #         firstline = True
    #         for line in data:
    #             if (firstline):
    #                 firstline = False
    #                 unsuccessful = False
    #                 for item in line:
    #                     if (not isinstance(item, str)):
    #                         # Not all of the headers are actually headers
    #                         # So send column indices as strings instead
    #                         headers.send(list(str(i) for i in range(len(line))))
    #                         output.writerow(line)
    #                         unsuccessful = True
    #                         break
    #                 if (not unsuccessful):
    #                     headers.send(line)
    #             else:
    #                 output.writerow(line)
    #     csv_file.send(newname)
    for name, ID in zip(filename.iter_contents(), FID.iter_contents()):
        main_name = '{ID}_In_{base}.csv'.format(ID=ID, base=name)
        with open(name) as _from, open(main_name) as _to:
            data = csv.reader(_from, quoting=csv.QUOTE_NONNUMERIC, quotechar="'")
            column_check(data)
            _from.seek(0)
            output = csv.writer(_to, quoting=csv.QUOTE_NONNUMERIC, quotechar="'")
            # Copies the data into the file that will be the base input from now on
            for line in data:
                output.writerow(line)
        IN.send(main_name)
        FID_out.send(ID)


def column_check(data):
    # TODO: Check if this exhausts the iterator in containing scope
    # NOTE: It does, you need to reset with a seek(0) command
    """Expects a csv.reader object."""
    count = []
    for line in data:
        count.append(len(line))
    mean = round(float(sum(count)) / len(count))
    error = False
    for (i, item) in enumerate(count):
        if (item != mean):
            print('Line {0} has a different number of columns than the rest '
                  'of the file.'.format(i + 1))
            error = True
    if (error):
        raise IOError('One or more of the lines was flawed.')
    else:
        return True
