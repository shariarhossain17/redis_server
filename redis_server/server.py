

from .storage import Storage


class RedisServer:
    def __init__(self):
        storage=Storage()
        storage.set("ss",10)