import logging
import sqlite3
from dataclasses import astuple, make_dataclass

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor, execute_batch


class SQLiteLoader:
    sql = "SELECT name FROM sqlite_master WHERE type='table';"
    datacls = None

    def __init__(self, connection: sqlite3.Connection | _connection) -> None:
        self.cursor = connection.cursor()

    def get_tables_names(self) -> list[str]:
        self.cursor.execute(self.sql)
        tb_names = [tb[0] for tb in self.cursor.fetchall()]
        return tb_names

    def get_column_names(self, table: str) -> list[str]:
        self.cursor.execute(f"SELECT * FROM {table};")
        return [
            d[0] if d[0] not in REPLACE_COL_NAMES else
            REPLACE_COL_NAMES[d[0]] for d in self.cursor.description
        ]

    def _create_table_class(self, table: str) -> None:
        self.datacls = make_dataclass(
            table,
            self.get_column_names(table),
            repr=False, eq=False, frozen=True, match_args=False, slots=True,
        )

    def load_from_table(self, table: str,
                        page_size: int = 200
                        ) -> list[type] | None:
        if not self.datacls:
            self._create_table_class(table)
        data = self.cursor.fetchmany(page_size)
        if not data:
            self.datacls = None
            return None
        return [self.datacls(*row) for row in data]


class PostgresSaver(SQLiteLoader):
    sql = """SELECT table_name FROM information_schema.tables
                            WHERE table_schema = 'content'"""

    def save_data(self, table: str, columns: list[str],
                  data: list[type]) -> None:
        s = ', '.join(['%s']*len(columns))
        query = (
            f'INSERT INTO {table} ({", ".join(columns)}) '
            f'VALUES ({s}) ON CONFLICT (id) DO NOTHING;'
        )
        prep_data = [astuple(row) for row in data]
        execute_batch(self.cursor, query, prep_data)


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    sqlite_loader = SQLiteLoader(connection)
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_table_names = sqlite_loader.get_tables_names()
    pg_table_names = postgres_saver.get_tables_names()
    if not set(sqlite_table_names).issubset(pg_table_names):
        tables = list(set(sqlite_table_names)-set(pg_table_names))
        msg = f'No output {tables=}'
        logging.error(msg)
        raise NameError(msg)
    for table in sqlite_table_names:
        logging.debug(f'Start parsing {table=}')
        columns_order = sqlite_loader.get_column_names(table)
        pg_columns = postgres_saver.get_column_names(table)
        if not set(columns_order).issubset(pg_columns):
            columns = list(set(columns_order)-set(pg_columns))
            msg = f'No {columns=} present in output {table=}'
            logging.error(msg)
            raise NameError(msg)
        while True:
            data = sqlite_loader.load_from_table(table)
            if not data:
                break
            postgres_saver.save_data(table, columns_order, data)
            pg_conn.commit()
        logging.debug(f'Finish parsing {table=}')


if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR)
    dsl = {
        'dbname': 'movies_db',
        'user': 'app',
        'password': '123qwe',
        'host': '127.0.0.1',
        'port': 5432,
        'options': '-c search_path=content',
    }
    REPLACE_COL_NAMES = {
        'updated_at': 'modified',
        'created_at': 'created',
    }
    try:
        with (sqlite3.connect('db.sqlite') as sqlite_conn,
              psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn):
            load_from_sqlite(sqlite_conn, pg_conn)
    except ConnectionError as e:
        logging.exception(e)
        raise
