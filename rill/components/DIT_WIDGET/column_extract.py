import csv

import rill


@rill.component
@rill.inport('flowID')
@rill.inport('base_csv')
@rill.inport('base_map')
@rill.inport('desired_names')
@rill.outport('temp_file_in')
@rill.outport('temp_file_out')
@rill.outport('temp_map')
def column_extract(flowID, base_csv, base_map, desired_names, temp_file_in, temp_file_out, temp_map):
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
    flow = flowID.receive_once()
    # TODO: add stepID
    for datafile, _map, desired in zip(base_csv.iter_contents(), base_map.iter_contents(), desired_names.iter_contents()):
        print(desired)
        # TODO: Does not recieve the step ID at any point
        temp_name = '{FID}_{SID}_{IO}_tmp.csv'
        temp_name_in = temp_name.format(FID=flow, SID=0, IO='In')
        temp_name_out = temp_name.format(FID=flow, SID=0, IO='Out')

        columns_to_take = []
        temp_file_map = {}
        for i, name in enumerate(desired):
            columns_to_take.append(_map[name])
            temp_file_map[name] = i

        with open(datafile, newline='') as IN, open(temp_name_in, 'w', newline='') as OUT:
            data = csv.reader(IN)
            output = csv.writer(OUT)
            header = True
            for line in data:
                if (header):
                    header = False
                else:
                    output.writerow(list(line[i] for i in columns_to_take))

        temp_file_in.send(temp_name_in)
        temp_file_out.send(temp_name_out)
        temp_map.send(temp_file_map)
