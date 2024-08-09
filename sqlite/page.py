from abc import ABC
from enum import Enum
from io import BytesIO
from struct import unpack

from sqlite.cell import Cell


class PageType(Enum):
    INDEX_BTREE_INTERIOR = 0x2
    TABLE_BTREE_INTERIOR = 0x5
    INDEX_BTREE_LEAF = 0xA
    TABLE_BTREE_LEAF = 0xD


CELL_POINTER_SIZE = 2
PAGE_HEADER_SIZE = {PageType.TABLE_BTREE_LEAF: 8}


class Page(ABC):
    @staticmethod
    def parse(page_bytes: bytes, header_start=0) -> 'Page':
        page_stream = BytesIO(page_bytes)
        page_stream.seek(header_start)
        page_type_byte = page_stream.read(1)
        page_type = PageType(unpack('B', page_type_byte)[0])

        match page_type:
            case PageType.TABLE_BTREE_LEAF:
                return TableBTreeLeafPage(page_stream, header_start)
            case _:
                raise ValueError


class TableBTreeLeafPage(Page):
    def __init__(self, page_stream: BytesIO, header_start: int):
        page_stream.seek(header_start)

        page_type = PageType.TABLE_BTREE_LEAF
        header = page_stream.read(PAGE_HEADER_SIZE[page_type])
        (
            _,  # page_type
            _,  # freeblock_start,
            num_cells,
            cells_start,
            _,  # num_frag_free_bytes
        ) = unpack('>BHHHB', header)

        cell_pointers_bytes = page_stream.read(num_cells * CELL_POINTER_SIZE)
        cell_pointers = unpack('>' + 'H' * num_cells, cell_pointers_bytes)
        assert cells_start == cell_pointers[-1]

        self.cells = [Cell(page_stream, cp) for cp in cell_pointers]
