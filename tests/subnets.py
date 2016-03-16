from rill.components.basic import Passthru
from rill.engine.component import inport, outport
from rill.engine.subnet import SubNet, SubInSS, SubOutSS


@outport("OUT")
@inport("IN")
class PassthruNet(SubNet):
    def define(self):
        self.add_component("SUBIN", SubInSS, NAME='IN')
        self.add_component("SUBOUT", SubOutSS, NAME='OUT')
        self.add_component("Pass", Passthru)

        self.connect("SUBIN.OUT", "Pass.IN")
        self.connect("Pass.OUT", "SUBOUT.IN")
