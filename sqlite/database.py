from struct import unpack

from sqlite.page import Page
from sqlite.table import Table

DB_HEADER_SIZE = 100


class Database:
    def __init__(self, filename: str) -> None:
        self._db_file = open(filename, 'rb')
        db_header = self._db_file.read(DB_HEADER_SIZE)
        magic, self._page_size = unpack('>16sH', db_header[:18])
        assert magic == b'SQLite format 3\0'

        self._num_pages = unpack('>I', db_header[28 : 28 + 4])[0]

        self._init_tables()

    def close(self):
        self._db_file.close()

    def count_table(self, command: str):
        tbl_name = command.rpartition(' ')[-1]
        table = self._tables[tbl_name]
        page = self._read_page(table.rootpage)
        print(len(page.cells))

    def info(self):
        print(f'database page size: {self._page_size}')
        print(f'number of tables: {len(self._tables)}')

    def print_tables(self):
        output = ' '.join(t.name for t in self._tables.values())
        print(output)

    def _init_tables(self):
        self._db_file.seek(0)
        root_page = self._db_file.read(self._page_size)
        sqlite_schema = Page.parse(root_page, 100)
        tables = map(Table.parse, sqlite_schema.cells)
        self._tables = {t.name: t for t in tables}

    def _read_page(self, page_no: int):
        i = page_no - 1
        page_bytes = self._db_file.read(self._page_size)
        return Page.parse(page_bytes, 100 if i == 0 else 0)
