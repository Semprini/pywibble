
class Operation(Enum):
    CREATE = 1
    UPDATE = 2
    DELETE = 3
    
    
class CDC(object):
    def __init__(self):
        self.source = None
        self.dest = None
        self.read_change_id()
    
    
    def write_change_id(self):
        with open("change_id.txt","w") as f:
            f.write("{}".format(self.change_id))
            
            
    def read_change_id(self):
        try:
            with open("change_id.txt","r") as f:
                self.change_id = int(f.read())
        except IOError:
            self.change_id = 0
