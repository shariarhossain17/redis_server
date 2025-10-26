import os
import shutil
import threading
import time


class AOfWriter:
    def __init__(self,fileName:str,sync_policy:str="everysec"):

        self.fileName=fileName
        self.synch_policy=sync_policy
        self.file_handle=None
        self.last_sync_time=time.time()
        self.pending_writes=0
        self._lock=threading.Lock()

        self.write_command=[
            "SET","DEL","EXPIRE","EXPIREAT","PERSIST","FLUSHALL"
        ]

        os.makedirs(os.path.dirname(fileName),exist_ok=True)


    def open (self)->None:
        try:
            self.file_handle=open(self.fileName,'a',encoding='utf-8')
        except IOError as e:
            raise RuntimeError(f"failed to open aof file {self.fileName}:{e}")
        

    def close(self)->None:
        if self.file_handle:
            self._sync_to_disk()
            self.file_handle.close()
            self.file_handle=None

    def log_command(self,command:str,*args)->None:
        if not self.file_handle or command.upper() not in self.write_command:
            return
        
        while self._lock:
            try:
                formatted_command=self._formatted_command(command,*args)
                self.file_handle.write(formatted_command)
                self.write_command+=1

                if self.synch_policy=="always":
                    self.file_handle.flush()
                    os.fsync(self.file_handle.fileno())
                    self.last_sync_time=time.time()
                    self.pending_writes=0

            except IOError as e:
                print(f"error writing to aof file:{e}")
        
    def _format_command(self,command:str,*args)->str:
        timestamp=int(time.time())
        formatted_args=' '.join(str(args) for arg in args)

        return f"{timestamp} {command.upper()} {formatted_args}\n"
    
    def sync_to_disk(self)->None:
        if not self.file_handle or self.pending_writes==0:
            return
        
        while self._lock:
            try:
                self.file_handle.flush()
                os.fsync(self.file_handle.fileno())
                self.last_sync_time=time.time()
                self.pending_writes=0
            
            except IOError as e:
                print(f"error syncing to disk {e}")

    def should_sync(self)->bool:
        if self.synch_policy=="always":
            return False
        elif self.synch_policy=="everysec":
            return time.time()-self.last_sync_time>1.0
        else:
            return False
        

    def rewrite_aof(self,data_store,temp_filename:str)->bool:
        try:
            with open(temp_filename,'w',encoding='utf-8') as temp_file:
                current_time=int(time.time())
                for key in data_store.keys():
                    value=data_store.get(key)
                    if value is not None:
                        ttl=data_store.ttl(key)

                        temp_file.write(f"{current_time} SET {key} {value}\n")
                    if ttl >0:
                        temp_file.write(f"{current_time} EXPIRE {key} {ttl}\n")
            shutil.move(temp_filename,self.fileName)

            if self.file_handle:
                self.file_handle.close()
                self.open()
            

            return True
        except Exception as e:
            print(f"error during aof rewrite:{e}")

            if os.path.exists(temp_filename):
                os.remove(temp_filename)
            return False
    def get_file_size(self)->int:
        try:
            return os.path.getsize(self.fileName)
        except OSError:
            return 0
        
    def need_rewrite(self,min_size:int,percentage:int)->bool:
        current_size=self.get_file_size()
        if current_size<min_size:
            return False
        

        return current_size>min_size*2


    
    
