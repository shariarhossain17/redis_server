


from redis_server import RedisServer


def main():
    server=RedisServer()
    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()



if __name__=="__main__":
    main()
