from rill.components.basic import Passthru
from rill.decorators import inport, outport
from rill.engine.subnet import SubGraph, SubInSS, SubOutSS




# @subnet
# @outport("OUT")
# @inport("IN")

# promote, relay, forward, export, expose
def submit():

    @inport("IN")
    @outport("OUT")
    class MayaExport(SubGraph):
        def define(self, net):
            p = Passthru("Pass")
            net.export(p.OUT, "OUT")
            net.export(p.IN, "IN")
            net.export(p.WHATEVER, "WHATEVER", create=True)

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
class PassthruNet(SubGraph):
    @classmethod
    def define(cls, graph):
        graph.add_component("Pass", Passthru)
        graph.export("Pass.OUT", "OUT")
        graph.export("Pass.IN", "IN")


# @outport("OUT")
# @inport("IN")
# class PassthruNetSS(SubGraph):
#     def define(self, net):
#         net.add_component("SUBIN", SubInSS, NAME='IN')
#         net.add_component("SUBOUT", SubOutSS, NAME='OUT')
#         net.add_component("Pass", Passthru)
#
#         net.connect("SUBIN.OUT", "Pass.IN")
#         net.connect("Pass.OUT", "SUBOUT.IN")
