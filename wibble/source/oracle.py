
class Oracle(object):
    def __init__(self):
        self.connection = None
        self.logminer_active = False
        self.cursor = None
        self.tables = ()
