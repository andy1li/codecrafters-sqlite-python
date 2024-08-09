import re
from typing import NamedTuple

from sqlite.cell import Cell


class Table(NamedTuple):
    type: str
    name: str
    tbl_name: str
    rootpage: int
    sql: str
    columns: tuple[str, ...]

    @classmethod
    def parse(cls, cell: Cell) -> 'Table':
        values = cell.record.values
        assert len(values) == 5
        assert values[0] == 'table'
        assert values[-1].startswith('CREATE TABLE')

        create_table = re.sub(r'[\n\t]', '', values[-1])
        columns_str = re.search(r'\((.*)\)', create_table).group(1)  # type: ignore
        columns = []
        for c in columns_str.split(','):
            if 'key' in c:
                Table._handle_key(c)
            else:
                c = c.split(' ')[0]
                columns.append(c)

        return cls(*values, tuple(columns))  # type: ignore

    @staticmethod
    def _handle_key(column: str):
        pass
