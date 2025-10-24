

from .storage import Storage


class RedisServer:
    def __init__(self):
        storage=Storage()
        storage.set("ss",10)
        del_count =storage.delete("ss")
        print(del_count)
        print(storage.get("ss"))
        exist=storage.exist("ss")
        print(exist)
     