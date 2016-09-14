import rill


@rill.component
@rill.inport('infile')
@rill.inport('inmap')
@rill.inport('outfile')
@rill.inport('outmap')
@rill.inport('columns')
@rill.inport('widget')
@rill.inport('widget_args')
@rill.inport('sid')
@rill.outport('sid_out')
@rill.outport('infile_out')
@rill.outport('inmap_out')
@rill.outport('outfile_out')
@rill.outport('outmap_out')
def step(infile, infile_out, outfile, outfile_out, inmap, inmap_out, outmap, outmap_out, columns, widget, widget_args, sid, sid_out):
    """These ports will send relevant data between steps.
    infile should recieve the name of the main input file that was constructed back in read_file
    infile_out sends along whatever it recieves from infile
    outfile should receive the name of the final output file that was initialized in variable_map
    outfile_out sends along whatever it recieves from outfile
    inmap should receive a dict of column name: column index that was constructed in variable_map
    inmap_out sends along whatever it recieves from inmap
    outmap should receive a dict of column name: column index that was constructed in variable_map
    outmap_out sends along whatever it recieves from outmap
    columns should receive a static list of column names to take from infile
    widget should receive a rill component as was initialized at the creation of the containing graph
    widget_args should receive a dictionary of port name: data to pass into the contained widget
    sid should receive the current step ID which is just a natural number
    sid_out should send sid+1

    To send the widget_args, use locate_port to find the correct one, then send the data into it.
    """


def locate_port(widget, port_name):
    """Returns the port that is desired. Errors if the port doesn't exist."""
