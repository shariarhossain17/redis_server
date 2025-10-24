class Storage:
    def __init__(self):
        self.data={}
        self.memory=0
    
    def set(self,key,value,expireTime=None):
        if key in self.data(key):
            old_val,_,_=self.data[key]
            self.memory-=self._calculate_memory_usage(key,old_val)
        data_type=self._get_data_type(value)
        self.data[key]=(value,data_type,expireTime)
        self.memory+=self._calculate_memory_usage(key,value)


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
        
    #calculate memory
    def _calculate_memory_usage(self,key,value):
        key_size=len(str(key).encode("utf-8"))
        value_size=len(str(key).encode("utf-8"))
        return key_size+value_size+64
