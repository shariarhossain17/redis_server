
import os
import time
from typing import Dict, any


class PersisTenceConfig:
    def __init__(self,config_dict:Dict[str,any]=None):
        self.config=self._get_default_config()

        if config_dict:
            self.config.update(config_dict)
        

        self._validate_config()


    def _get_default_config(self)->Dict[str,any]:
        return {
            #aof configuration
            'aof_enabled':True,
            'aof_filename':'appendonly.aof',
            'aof_synch_policy':'everysec',
            'aof_rewrite_percentage':100,
            'aof_rewrite_min_size':1024*1024,

            #directory configuration
            'data_dir':'./data',
            'temp_dir':'/data/temp',

            #general setting
            'persistence_enable':True,
            'recover_on_startup':True,
            "max_memory_usage":100*1024*1024
        }
    

    def _validate_config(self)->None:
        valid_synch_policies=["always","everysec","no"]

        if self.config['aof_synch_policy'] not in valid_synch_policies:
            raise ValueError(f"Invalid AOf synch Policy must be on of:{valid_synch_policies}")
        
        if not self.config['aof_filename']:
            raise ValueError("AOF filename cannot be empty")


    def update(self,config_dict:Dict[str,any])->None:
        self.config.update(config_dict)
        self._validate_config()

    def get(self,key:str,default=None):
        return self.config.get(key,default)
    
    def set(self,key:str,value:any)->None:
        self.config[key]=value
        self._validate_config()
    
    def get_all(self)->Dict[str,any]:
        return self.config.copy()
    
    @property
    def aof_enabled(self)->bool:
        return self.config['aof_enabled']
    
    @property
    def aof_filename(self)->str:
        return os.path.join(self.config['data_dir'],self.config['aof_filename'])
    
    @property
    def aof_synch_policy(self)->str:
        return self.config['aof_synch_policy']
    
    @property
    def aof_data_dir(self)->str:
        return self.config['aof_data_dir']
    

    @property
    def data_dir(self)->str:
        return self.config['data_dir']
    
    
    @property
    def temp_dir(self)->str:
        return self.config['temp_dir']
    

    def ensure_directories(self)->None:
        os.makedirs(self.data_dir,exist_ok=True)
        os.makedirs(self.temp_dir,exist_ok=True)

    
    def get_aof_temp_filename(self)->str:
        return os.path.join(self.temp_dir,f"temp-rewrite_aof-{int(time.time())}.aof")
    
    def __repr__(self)->str:
        return f"persistenceConfig({self.config})"
        
    


    