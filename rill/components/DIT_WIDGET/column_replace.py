import csv
import os

import rill

@rill.component
@rill.inport('base_csv')
@rill.inport('base_map')
@rill.inport('temp_csv')
@rill.inport('temp_map')
@rill.outport('base_map_updated')
def column_replace(base_csv, base_map, temp_csv, temp_map, base_map_updated):
    """
    EVENTUALLY WILL TAKE A MAP OF THE OUTPUT FILE
    name_map is the map created by column_extract (this should just be linked directly, bypassing the intervening widget)
    column_name is the name to be replaced
    data is what to replace the column with
    filename is the
    """
    """
    construct name of base_out_csv
    for name in temp_map:
        if name not in base_map:
            newcol = max(base_map.items()) + 1
            base_map[name] = newcol
    for mainline, templine in zip(base_csv, temp_csv):
        for name in temp_map:
            index = base_map[name]
            mainline[index] = templine[index]
        base_out_csv.writerow(mainline)
    """
    for basefile, basemap, tempfile, tempmap in zip(base_csv.iter_contents(), base_map.iter_contents(), temp_csv.iter_contents(), temp_map.iter_contents()):
        baseoutput = basefile.replace('_In_', '_Out_')
        with open(tempfile, newline='') as IN, open(baseoutput, newline='') as EXISTING, open('temp', 'w', newline='') as OUT:
            data = csv.reader(IN)
            previous = csv.reader(EXISTING)
            output = csv.writer(OUT)
            # TODO: will require work to adapt to new map schema
            # TODO: not safe with new file columns
            headers = next(iter(previous))
            for name in tempmap:
                headers[basemap[name]] = name
            output.writerow(headers)
            for templine, mainline in zip(data, previous):
                for name in tempmap:
                    if (name not in basemap):
                        # Append the data to the next column
                        # In future will read from outmap
                        basemap[name] = max(basemap.items() + 1)
                    mainline[basemap[name]] = templine[tempmap[name]]
                output.writerow(mainline)
        with open('temp') as processed, open('DEMO_In_AKUL232.csv', 'w') as raw:
            for line in processed:
                raw.write(line)
        base_map_updated.send(basemap)
