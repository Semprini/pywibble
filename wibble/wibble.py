import importlib

class Operation(Enum):
    CREATE = 1
    UPDATE = 2
    DELETE = 3
    
    
class CDC(object):
    def __init__(self):
        source_module = importlib.import_module('source.oracle')
        self.source = source_module.Oracle()
        
        dest_module = importlib.import_module('source.oracle')
        self.dest = dest_module.MQ()
        
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


    def run():
        table_changes = {}
        count=0
        
        done = False
        while not done:
            try:
                self.change_id,table,operation,change = self.source.get()
                self.dest.put(table,operation,change)
                self.write_change_id()
                
                if table in table_changes.keys():
                    table_changes[table] += 1
                else:
                    table_changes[table] = 1
                if count % 1000 == 0:
                    print("{} | {}".format(count, table_changes))
            except (KeyboardInterrupt, SystemExit):
                done = True
        self.source.disconnect()
              
              
if __name__ == "main":
    cdc = CDC()
    cdc.run()
