import os
import pkg_resources

from psycopg2 import pool, OperationalError, InterfaceError
from psycopg2.extras import execute_values, execute_batch
from prometheus_client import Gauge, Info

from addok.config import config

version = pkg_resources.require("addok-psql-store")[0].version

class PSQLStore:
    def __init__(self, *args, **kwargs):
        self.pool = pool.SimpleConnectionPool(minconn=1, maxconn=2,
                                              dsn=config.PG_CONFIG)
        self.metrics_catalog = {
            "build_info": Info('addok_plugin_addok_psql_store', 'Build info of Addok plugin addok_psql_store'),
            "total_records": Gauge('addok_total_records', 'Total count of Addok records'),
        }

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
        Potential performance boost, using copy_from:
        * https://gist.github.com/jsheedy/efa9a69926a754bebf0e9078fd085df6
        * https://gist.github.com/jsheedy/ed81cdf18190183b3b7d

        Or event copy_expert for mixed binary content:
        * http://stackoverflow.com/a/8150329
        """
        insert_into_query = '''
        INSERT INTO {PG_TABLE} (key, data) VALUES %s
            ON CONFLICT DO NOTHING
        '''.format(**config)
        with self.getconn() as conn, conn.cursor() as curs:
            execute_values(curs, insert_into_query, docs)
        

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

    def metrics(self):
        self.metrics_catalog["build_info"].info({'version': version})

        select_query = "SELECT COUNT(*) FROM {PG_TABLE}".format(**config)
        with self.getconn() as conn, conn.cursor() as curs:
            curs.execute(select_query)
            records = curs.fetchall()
            self.metrics_catalog["total_records"].set(records[0][0])


def preconfigure(config):
    config.DOCUMENT_STORE_PYPATH = 'addok_psql_store.PSQLStore'
    config.PG_CONFIG = 'dbname=addok user=addok host=localhost password=addok'
    config.PG_TABLE = 'addok'
