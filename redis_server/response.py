def error(message):
    return f"-ERR {message}\r\n".encode()


def pong():
    return b"+pong\r\n"