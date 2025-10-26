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
        