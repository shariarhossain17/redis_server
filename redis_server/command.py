from .response import *


class CommandHandler:
    def __self__(self,storage):
        self.storage=storage
        self.command_count=0
        self.commands={
            "PING":self.ping
        }

    def execute(self,command,*args):
        self.command_count+=1
        cmd=self.commands.get(command)
        if cmd:
            return cmd(*args)
        return error(f"unknown command '{command}'")
    
    def ping(self):
        pong()


