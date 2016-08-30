from rill import inport, outport, component

@component
@inport("IN")
@outport("OUT")
def Output(IN, OUT):
    print('In Output component\n')
    for p in IN:
        print('processing IN.p... ')
        print(repr(p.get_contents()))
        OUT.send(p)


@component
@inport("IN", type=str)
@inport("TEST", type=str)
@outport("ACC", type=str)
@outport("REJ", type=str)
def StartsWith(IN, TEST, ACC, REJ):
    test_str = TEST.receive_once()

    for p in IN.iter_packets():
        s = p.get_contents()
        if s.startswith(test_str):
            ACC.send(p)
        else:
            REJ.send(p)

@component
@inport("IN", type=str)
@inport("MEASURE", type=int)
@outport("OUT", type=str)
def WordsToLine(IN, MEASURE, OUT):
    measure = MEASURE.receive_once()

    line = ""
    for word in IN.iter_contents():
        if measure and (len(line) + 1 + len(word)) > measure:
            OUT.send(line)
            # restart line
            line = word
        else:
            if line:
                line += " "
            line += word
    if line:
        # remainder
        OUT.send(line)

@component
@outport("OUT", type=str)
@inport("IN", type=str)
def LineToWords(IN, OUT):
    for line in IN.iter_contents():
        words = line.split()
        for word in words:
            print("Sending packet to OUT with: {}\n".format(word))
            OUT.send(word)
