import logging
import os
import sqlite3

import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor


class DbParser:
    def __init__(self, connection: sqlite3.Connection | _connection) -> None:
        self.cursor = connection.cursor()

    def get_column_names(self, table: str) -> list[str]:
        self.cursor.execute(f'SELECT * FROM {table};')
        return [d[0] for d in self.cursor.description
                if d[0] not in IGNORED_COL_NAMES]

    def get_table_size(self, table: str) -> int:
        self.cursor.execute(f'SELECT count(id) FROM {table};')
        return int(self.cursor.fetchone()[0])

    def gen_row(self, table: str):
        self.cursor.execute(f'SELECT * FROM {table} ORDER BY id;')
        while row := self.cursor.fetchone():
            yield row


def compare_items(s_row: tuple[str], p_row: dict,
                  columns: list[str]) -> None:
    for idx, col in enumerate(columns):
        s_item, p_item = s_row[idx], p_row[col]
        if (s_item or p_item) and s_item != p_item:
            msg = ('Found unconsistency', s_item, p_item)
            logging.error(msg)
            raise ValueError(msg)


def check_dbs(connection: sqlite3.Connection, pg_conn: _connection) -> None:
    s_parser = DbParser(connection)
    p_parser = DbParser(pg_conn)
    for table in check_tables:
        logging.debug(f'Start parsing {table=}')
        if s_parser.get_table_size(table) != p_parser.get_table_size(table):
            raise ValueError('Different table sizes', table)
        columns = s_parser.get_column_names(table)
        for s_row, p_row in zip(s_parser.gen_row(table),
                                p_parser.gen_row(table)):
            logging.debug(f'Compare {s_row=} and {p_row=}')
            compare_items(s_row, p_row, columns)


if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR)
    sqlite_path = '../../db.sqlite'
    load_dotenv()
    dsl = {
        'dbname': os.getenv('DBNAME'),
        'user': os.getenv('USER'),
        'password': os.getenv('PASSWORD'),
        'host': os.getenv('HOST'),
        'port': os.getenv('PORT'),
        'options': '-c search_path=content',
    }
    check_tables = (
        'genre',
        'film_work',
        'person',
        'genre_film_work',
        'person_film_work',
    )
    IGNORED_COL_NAMES = (
        'updated_at',
        'modified',
        'created_at',
        'created',
    )
    try:
        with (sqlite3.connect(sqlite_path) as sqlite_conn,
              psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn):
            check_dbs(sqlite_conn, pg_conn)
    except ConnectionError as e:
        logging.exception(e)
        raise
    finally:
        sqlite_conn.close()
