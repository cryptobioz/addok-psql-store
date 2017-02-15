import psycopg2
import psycopg2.pool
import os

from addok.config import config


class PGStore:
    def __init__(self, *args, **kwargs):
        self.pg = psycopg2.pool.PersistentConnectionPool(8,64,config.PG_CONFIG)
        with self.pg.getconn() as conn:
            cur = conn.cursor()
            cur.execute('CREATE TABLE IF NOT EXISTS '+config.PG_TABLE+' (key VARCHAR COLLATE "C", data bytea)')
            cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS '+config.PG_TABLE+'_key_idx ON '+config.PG_TABLE+' (key)')
            conn.commit()
            self.pg.putconn(conn)

    def flushdb(self):
        with PGStore.conn() as conn:
            cur = conn.cursor()
            cur.execute('DROP TABLE IF EXISTS '+config.PG_TABLE)
            conn.commit()

    def conn():
        return(psycopg2.connect(config.PG_CONFIG))

    def fetch(self, *keys):
        if not keys:  # Avoid invalid SQL.
            return
        keys = [key.decode() for key in keys]
        with self.pg.getconn() as conn:
            cur = conn.cursor()
            params = ','.join(cur.mogrify("'%s'" % k).decode("utf-8") for k in keys)
            query = 'SELECT key, data FROM '+config.PG_TABLE+' WHERE key IN ('+params+')'
            cur.execute(query)
            for key, data in cur.fetchall():
                yield key.encode(), data
            self.pg.putconn(conn)

    def upsert(self, *docs):
        with PGStore.conn() as conn:
            cur = conn.cursor()
            values = ','.join(cur.mogrify("(%s,%s)", d).decode("utf-8") for d in docs)
            cur.execute('INSERT INTO '+config.PG_TABLE+' (key,data) VALUES ' + values + ' ON CONFLICT DO NOTHING')
            conn.commit()

    def remove(self, *keys):
        with PGStore.conn() as conn:
            conn.cursor().executemany('DELETE FROM '+config.PG_TABLE+' WHERE key=%s', (keys, ))
            conn.commit()


def preconfigure(config):
    config.DOCUMENT_STORE_PYPATH = 'addok_pg.plugin.PGStore'
    config.PG_CONFIG = 'dbname=addok user=addok host=localhost password=addok'
    config.PG_TABLE = 'addok'
