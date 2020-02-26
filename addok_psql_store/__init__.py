import os
from pgcopy import CopyManager

from psycopg2 import pool, OperationalError, InterfaceError
from psycopg2.extras import execute_values, execute_batch

from addok.config import config


class PSQLStore:
    def __init__(self, *args, **kwargs):
        self.pool = pool.SimpleConnectionPool(minconn=1, maxconn=2,
                                              dsn=config.PG_CONFIG)
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS
            {PG_TABLE} (key VARCHAR COLLATE "C", data bytea)
        '''.format(**config)
        create_index_query = '''
        CREATE UNIQUE INDEX IF NOT EXISTS
            {PG_TABLE}_key_idx ON {PG_TABLE} (key)
        '''.format(**config)
        with self.getconn() as conn, conn.cursor() as curs:
            curs.execute(create_table_query)
            curs.execute(create_index_query)

    def getconn(self):
        # Use pid as connection id so we can reuse the connection within the
        # same process.
        conn = self.pool.getconn(key=os.getpid())
        try:
            c = conn.cursor()
            return conn
        except (OperationalError, InterfaceError) as err:
            self.pool.putconn(conn, key=os.getpid())
            return self.getconn()

    def fetch(self, *keys):
        # Using ANY results in valid SQL if `keys` is empty.
        select_query = '''
        SELECT key, data FROM {PG_TABLE} WHERE key=ANY(%s)
        '''.format(**config)
        with self.getconn() as conn, conn.cursor() as curs:
            curs.execute(select_query, ([key.decode() for key in keys],))
            for key, data in curs.fetchall():
                yield key.encode(), data

    def upsert(self, *docs):
        """
        Use copy_from to load the binary data into db, in case of conflicts,
        we switch to execute_values, with "ON CONFLICT DO NOTHING" only the
        failing row will be ignored instead of the whole chunk (docs)
        :param docs:
        :return:
        """
        with self.getconn() as conn, conn.cursor() as curs:
            mgr = CopyManager(conn, '{PG_TABLE}'.format(**config), ['key', 'data'])
            try:
                mgr.copy(docs) # will raise error if key exists
            except:
                insert_into_query = '''
                INSERT INTO {PG_TABLE} (key, data) VALUES %s
                ON CONFLICT DO NOTHING
                '''.format(**config)
                execute_values(curs, insert_into_query, docs)
            else:
                conn.commit()

    def remove(self, *keys):
        keys = [(key,) for key in keys]
        delete_from_query = '''
        DELETE FROM {PG_TABLE} WHERE key=%s
        '''.format(**config)
        with self.getconn() as conn, conn.cursor() as curs:
            execute_batch(curs, delete_from_query, keys)

    def flushdb(self):
        drop_table_query = '''
        DROP TABLE IF EXISTS {PG_TABLE}
        '''.format(**config)
        with self.getconn() as conn, conn.cursor() as curs:
            curs.execute(drop_table_query)


def preconfigure(config):
    config.DOCUMENT_STORE_PYPATH = 'addok_psql_store.PSQLStore'
    config.PG_CONFIG = 'dbname=addok user=addok host=localhost password=addok'
    config.PG_TABLE = 'addok'
