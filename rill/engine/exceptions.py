class FlowError(Exception):
    pass


class NetworkDeadlock(FlowError):
    def __init__(self, message, statuses):
        super(NetworkDeadlock, self).__init__(message)
        self.errors = statuses


class ComponentError(FlowError):
    pass


class TypeHandlerError(FlowError):
    pass


class PacketValidationError(TypeHandlerError):
    pass
