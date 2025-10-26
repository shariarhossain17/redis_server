import select
import socket
import time

from .command import CommandHandler
from .persistence import PersisTenceConfig
from .storage import Storage


class RedisServer:
    def __init__(self,host="localhost",port=6379,persistenceConfig=None):
        self.host=host
        self.port=(port)
        self.running=False
        self.server_socket=None
        self.clients={}
        self.last_clean_up_time=time.time()
        self.cleanup_interval=0.1
        self.storage=Storage()
        self.commandHandler=CommandHandler(self.storage)

        self.persistence_config=persistenceConfig or PersisTenceConfig()

    def start(self):
        self.server_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        self.server_socket.bind((self.host,self.port))
        self.server_socket.listen()
        self.server_socket.setblocking(False)
        self.running=True
        print(f"redis server running {self.host}:{self.port}")
        self._event_loop()

    def _event_loop(self):
        while self.running:
            try:
                read,_,_=select.select([self.server_socket]+list(self.clients.keys()),[],[],0.05)
                for sock in read:
                    if(sock is self.server_socket):
                        self._accept_client()
                    else:
                        self._handle_client(sock)
                
                cur_time =time.time()
                if cur_time-self.last_clean_up_time>=self.cleanup_interval:
                    self._background_clean_up()
                    self.last_clean_up_time=cur_time
                    
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"event loop error{e}")
        

    def _accept_client(self):
        try:
            client, addr = self.server_socket.accept()
            client.setblocking(False)
            self.clients[client] = {"addr": addr, "buffer": b""}
            print(f"Client connected: {addr}")
        except BlockingIOError:
            pass
        except Exception as e:
            print(f"Client error: {e}")




    
    def _handle_client(self,client):
        try:
            data=client.recv(1024)
            if not  data:
                self._disconnect_client(client)
                return
            self.clients[client]["buffer"]+=data
            self._process_buffer(client)

        except ConnectionError:
            self._disconnect_client(client)
        except Exception as e:
            print(f"client error {e}")
            self._disconnect_client(client)
    

    def _process_buffer(self,client):
        buffer =self.clients[client]["buffer"]

        while b"\n" in buffer:
            command,buffer=buffer.split((b"\n"),1)
            if command:
                try:
                    response=self._process_command(command.decode("utf-8"))
                    client.send(response)
                except Exception as e:
                    print(f"error processing command {e}")
                    err_res=f"-ERR {str(e)}\r\n".encode()
                    client.send(err_res)
        self.clients[client]["buffer"]=buffer

    def _background_clean_up(self):
        try:
            expired_count=self.storage.cleanup_expired_keys()
            if expired_count>0:
                print(f"clean up expire count {expired_count} keys")
        except Exception as e:
            print(f"clean up error {e}")

    def _process_command(self,command):
        parts=command.strip().split()
        print(parts)
        if not parts:
            return b"_ERR empty command"
        return self.commandHandler.execute(parts[0],*parts[1:])
    

    def _disconnect_client(self,client):
        try:
            addr =self.clients.get(client,{}).get("addr","unknown")
            print(f"disconnect client{addr}")
            client.close()
        except Exception as e:
            print(f"Error disconnecting client:{e}")
        
            
        
    def stop(self):
        self.running=False
        for client in list(self.clients.keys()):
            self._disconnect_client(client)
        
        if(self.server_socket):
            self.server_socket.close()
        print("server stop")





    


        