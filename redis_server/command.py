import time

from .response import *


class CommandHandler:
    def __init__(self,storage):
        self.storage=storage
        self.command_count=0
        self.commands={
            "PING":self.ping,
            "ECHO":self.echo,
            "SET":self.set,
            "GET":self.get,
        }

    def execute(self,command,*args):
        cmd=self.commands.get(command.upper())
        print(cmd)
        if cmd:
            return cmd(*args)
        return error(f"unknown command '{command}'")
    
    def ping(self):
        return pong()
    
    def echo(self, *args):
        value = " ".join(args) if args else ""
        return simple_string(value)  
        

    def set(self,*args):
        if len(args)<2:
            return error("wrong number of command for 'set' ")
        key=args[0]
        value=" ".join(args[1:])

        expiry_time=None

        if len(args) >= 4 and args[-2].upper() == "EX":
            try:
                seconds=int(args[-1])
                expiry_time=time.time()+seconds
                value=" ".join(args[1:-2])
            except ValueError:
                return error("invalid expire time in 'set' ")
            
            self.storage.set(key,value,expiry_time)
            return ok()
        
      





        
