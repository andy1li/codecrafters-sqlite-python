from io import BufferedReader
from struct import unpack
from typing import Generator


def read_varint(stream: BufferedReader) -> int:
    result = 0
    while True:
        byte = stream.read(1)
        if byte == b'':
            raise EOFError

        n = unpack('B', byte)[0]
        result <<= 7
        result |= n & 0b01111111
        if not (n & 0b10000000):
            break
    return result


def read_varints(stream: BufferedReader) -> Generator[int, None, None]:
    while True:
        try:
            yield read_varint(stream)
        except EOFError:
            return
