class FlowError(Exception):
    pass


class NetworkDeadlock(FlowError):
    def __init__(self, message, statuses):
        super(NetworkDeadlock, self).__init__(message)
        self.errors = statuses


class ComponentException(FlowError):
    pass


class TypeHandlerException(FlowError):
    pass


class PacketValidationError(TypeHandlerException):
    pass
