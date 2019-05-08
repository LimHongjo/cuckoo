class ProtocolHandler(object):
    """Abstract class for protocol handlers coming out of the analysis."""
    def __init__(self, handler, version=None):
        self.handler = handler
        self.version = version

    def init(self):
        pass

    def close(self):
        pass