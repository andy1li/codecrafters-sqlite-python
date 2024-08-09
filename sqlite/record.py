from io import BytesIO
from struct import unpack

from app.utils import read_varint, read_varints

SERIAL_SIZE = [0, 1, 2, 3, 4, 6, 8, 8, 0, 0]


class Record:
    def __init__(self, payload: bytes) -> None:
        header_size = read_varint(BytesIO(payload))
        header = payload[:header_size]
        self._parse_serial_types(header)

        body = payload[header_size:]
        self._parse_values(body)

    def _get_serial_size(self, serial_type: int) -> int:
        if serial_type <= 9:
            return SERIAL_SIZE[serial_type]

        if serial_type >= 12:
            parity_adjustment = 13 if serial_type & 1 else 12
            return (serial_type - parity_adjustment) // 2

        raise ValueError

    def _parse_serial_types(self, header):
        serial_stream = BytesIO(header[1:])
        self._serial_types = read_varints(serial_stream)

    def _parse_values(self, body: bytes):
        self.values, stream = [], BytesIO(body)
        for st in self._serial_types:
            size = self._get_serial_size(st)
            value = stream.read(size)
            if st == 1:
                value = unpack('b', value)[0]
            if isinstance(value, bytes):
                value = value.decode()  # type: ignore
            if value:
                self.values.append(value)
