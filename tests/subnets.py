from rill.components.basic import Passthru
from rill.decorators import inport, outport
from rill.engine.subnet import SubNet, SubInSS, SubOutSS




# @subnet
# @outport("OUT")
# @inport("IN")

# promote, relay, forward, export, expose
def submit():

    @inport("IN")
    @outport("OUT")
    class MayaExport(SubNet):
        def define(self):
            p = Passthru("Pass")
            self.export(p.OUT, "OUT")
            self.export(p.IN, "IN")
            self.export(p.WHATEVER, "WHATEVER", create=True)

    @subnet
    @inport("IN")
    @outport("OUT")
    def NukeExport(IN, OUT):
        p = Passthru("Pass")
        OUT.relay(p.OUT)
        IN.relay(p.IN)

    net = Network()
    net.connect(MayaExport.ports.OUT, NukeExport.ports.IN)
    net.go()


@outport("OUT")
@inport("IN")
class PassthruNetSS(SubNet):
    def define(self):
        self.add_component("SUBIN", SubInSS, NAME='IN')
        self.add_component("SUBOUT", SubOutSS, NAME='OUT')
        self.add_component("Pass", Passthru)

        self.connect("SUBIN.OUT", "Pass.IN")
        self.connect("Pass.OUT", "SUBOUT.IN")
