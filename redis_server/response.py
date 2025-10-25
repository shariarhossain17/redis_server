def error(message):
    return f"-ERR {message}\r\n"


def pong():
    return b"+pong\r\n"

def simple_string(value):
    return f"+{value}\r\n".encode()

def ok():
    return b"+ok\r\n"