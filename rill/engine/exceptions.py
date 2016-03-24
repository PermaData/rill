class FlowError(Exception):
    pass


class ComponentException(FlowError):
    pass


class PacketValidationError(FlowError):
    pass
