from .command import CommandHandler
from .storage import Storage


class RedisServer:
    def __init__(self):
        storage=Storage()
        command=CommandHandler(storage)

        val=command.execute("SET", "mykey", "hello world", "EX", "20")
        print(val)

        