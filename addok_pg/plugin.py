import psycopg2
import psycopg2.pool
import os

from addok.config import config


class PGStore:
    cur = None

    def __init__(self, *args, **kwargs):
        with PGStore.conn() as conn:
            cur = conn.cursor()
            cur.execute('CREATE TABLE IF NOT EXISTS '+config.PG_TABLE+' (key VARCHAR, data bytea)')
            cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS '+config.PG_TABLE+'_key_idx ON '+config.PG_TABLE+' (key)')
            conn.commit()

    def conn():
        return(psycopg2.connect(config.PG_CONFIG))

    def fetch(self, *keys):
        if not keys:  # Avoid invalid SQL.
            return
        keys = [key.decode() for key in keys]
        with PGStore.conn().cursor() as cur:
            params = ','.join("'%s'" % k for k in keys)
            query = 'SELECT key, data FROM '+config.PG_TABLE+' WHERE key IN ('+params+')'
            cur.execute(query)
            for key, data in cur.fetchall():
                yield key.encode(), data

    def add(self, *docs):
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
    config.DOCUMENT_STORE = 'addok_pg.plugin.PGStore'
    config.PG_CONFIG = 'dbname=addok user=addok host=localhost password=addok'
    config.PG_TABLE = 'addok'
