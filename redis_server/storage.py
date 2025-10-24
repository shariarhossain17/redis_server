
import fnmatch
import time


class Storage:
    def __init__(self):
        self.data={}
        self.memory=0
    
    def set(self,key,value,expireTime=None):
        if key in self.data:
            old_val,_,_=self._get_value_from_storage(key)
            self.memory-=self._calculate_memory_usage(key,old_val)
        data_type=self._get_data_type(value)
        self.data[key]=(value,data_type,expireTime)
        self.memory+=self._calculate_memory_usage(key,value)
      
       
    def get(self,key):
        if not self._is_key_valid(key):
            return None
        value,_,_=self._get_value_from_storage(key)
        return value
    
    def delete(self,*keys):
        count=0
        for key in keys:
            if key in self.data:
                value,_,_=self._get_value_from_storage(key)
                self.memory-=self._calculate_memory_usage(key,value)
                del self.data[key]
                count+=1
        return count
    def exist(self,*keys):
        return sum(1 for key in keys if self._is_key_valid(key))
    
    def keys(self,pattern="*"):
        valid_keys=[key for key in self.data.keys() if self._is_key_valid(key)]
        if pattern=="*":
            return valid_keys
        return [key for key in valid_keys if fnmatch.fnmatch(key,pattern)]
    
    def flush(self):
        self.data.clear()
        self.memory=0
    
    def expire(self,key,seconds):
        if not self._is_key_valid(key):
            return False
        value,data_type,_=self._get_value_from_storage(key)
        expiry_time=time.time()+seconds
        self.data[key]=(value,data_type,expiry_time)
        return True
    
    def expire_at(self,key,timestamp):
        if not self._is_key_valid(key):
            return False
        value,data_type,_=self._get_value_from_storage(key)
        self.data[key]=(value,data_type,timestamp)
        return True
    

    def ttl(self,key):
        if not self._is_key_valid(key):
            return -2
        value,_,expiry_time=self._get_value_from_storage(key)
        if expiry_time is None:
            return -1
        remaining=expiry_time-time.time()
        if(remaining<=0):
            self.memory-=self._calculate_memory_usage(key,value)
            del self.data[key]
        return int(remaining)
    

    def pttl(self,key):
        if not self._is_key_valid(key):
            return -2
        value,_,expiry_time=self._get_value_from_storage(key)
        if expiry_time is None:
            return -1
        remaining=expiry_time-time.time()
        if(remaining<=0):
            self.memory-=self._calculate_memory_usage(key,value)
            del self.data[key]
        return int(remaining*1000)
    
    def persist(self,key):
        if not self._is_key_valid(key):
            return False
        value,data_type,_=self._get_value_from_storage(key)
        self.data[key]=(value,data_type,None)
    
    def get_type(self,key):
        if not self._is_key_valid(key):
            return False
        _,data_type,_=self._get_value_from_storage(key)
        return data_type
   
        



    

    def _get_value_from_storage(self,key):
        return self.data[key]

    #check key is valid
    def _is_key_valid(self,key):
        return key in self.data



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
