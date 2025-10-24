class Storage:
    def __init__(self):
        self.storage={}
        self.memory=0
    
    def set(self,key,value,expire=None):
        print("value set")

    #get data type
    def _get_data_type(self,value):
        if isinstance(value,str):
            return "string"
        elif isinstance(value,int):
            return "string"
        elif isinstance(value,list):
            return "list"
        elif isinstance(value,set):
            return "set"
        elif isinstance(value,hash):
            return "hash"
        else:
            return "string"