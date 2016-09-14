import itertools
import csv
import re

import rill


@rill.component
@rill.inport('filename')
@rill.inport('mapfile')
@rill.outport('IN')
@rill.outport('OUT')
@rill.outport('STEP')
@rill.outport('INMAP')
@rill.outport('OUTMAP')
def variable_map(filename, mapfile, IN, OUT, STEP, INMAP, OUTMAP):
    # for name, header in zip(filename.iter_contents(), headers.iter_contents()):
    #     _name_map = {}
    #     if (name_overrides.is_empty()):
    #         overrides = itertools.repeat(None)
    #     else:
    #         overrides = next(name_overrides.iter_contents())
    #     for i, (head, override) in zip(header, overrides):
    #         if (override is not None):
    #             _name_map.update({override: i})
    #         else:
    #             _name_map.update({head: i})
    #     name_map.send(_name_map)

    # Columns are separated by tabs
    sep = '\t'
    n_entries = 7

    if (mapfile.upstream_packets() == 1):
        # Use a single map file for all the files
        mapiter = itertools.repeat(mapfile.receive_once())
    else:
        # use different map files
        mapiter = mapfile.iter_contents()

    for Dname, Mname in zip(filename.iter_contents(), mapiter):
        # Dname is the data file name
        # Mname is the map file name
        in_map = {}
        in_details = {}
        out_map = {}
        out_details = {}
        with open(Mname) as f:
            firstline = True
            for line in f:
                if (firstline):
                    firstline = False
                    continue
                pattern = '{0}+'.format(sep)
                entries = re.split(pattern, line)
                if (len(entries) != n_entries):
                    raise IndexError('File has the wrong number of columns.')
                else:
                    in_header, operation, out_header, in_index, out_index, \
                        units, description = entries_breakout(entries)
                    # TODO: description and units should be passed around as metadata
                    if (in_header and in_index):
                        in_map.update({in_header: in_index})
                        in_details.update({in_header: [operation, description]})
                    if (out_header and out_index):
                        out_map.update({out_header: out_index})
                        out_details.update({out_header: [units, description]})
        with open(Dname) as IN, open(convert_to_out(Dname)) as OUT:
            data = csv.reader(IN)
            output = csv.writer(OUT)
            headline = next(data) # Pulls the first line of the file as headers
            for name, index, details in zip(out_map.items(), out_details.values()):
                headline[index] = '{name} ({unit})'.format(name=name, unit=details[0])
            output.writerow(headline)
            copies = {}
            for in_name, in_index, out_name, out_index in zip(in_map.items(), out_map.items()):
                # Figure out which items need to be copied
                if (in_details[0] == 'copy'):
                    copies.update(in_index: out_index)
            for line in data:
                # Copy selected columns
                outputline = [''] * len(line)
                for _from, _to in copies.items():
                    outputline[_to] = line[_from]
                output.writerow(outputline)

        IN.send(Dname)
        OUT.send(convert_to_out(Dname))
        STEP.send(1)
        INMAP.send(in_map)
        OUTMAP.send(out_map)


def convert_to_out(infile_name):
    return re.sub('([_/])In[_/]', lambda m: '{0}Out{0}'.format(m.groups(1)),
                  infile_name)


def entries_breakout(entries):
    in_header = entries[0]
    operation = entries[1]
    out_header = entries[2]
    in_index = int(entries[3])
    out_index = int(entries[4])
    units = entries[5]
    description = entries[6]
    return in_header, operation, out_header, in_index, out_index, units, description
