from io import BytesIO

from app.utils import read_varint
from sqlite.record import Record


class Cell:
    def __init__(self, page_stream: BytesIO, cell_start: int) -> None:
        page_stream.seek(cell_start)
        payload_size = read_varint(page_stream)
        self.row_id = read_varint(page_stream)

        payload = page_stream.read(payload_size)
        self.record = Record(payload)
        # ignore overflow_page_no for now
