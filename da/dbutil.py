# -*- coding: utf-8 -*-

# Copyright (c) 2016 Grant AO. All rights reserved.

import sqlalchemy
from sqlalchemy import create_engine
import os

def get_pg_engine():
    return create_engine('postgresql+psycopg2://hulk_gen:q1w2e3r4@nile/fs',  encoding='utf8')


class SqlRunner(object):
    def __init__(self):
        self.engine = get_pg_engine()
        self.connections = {}

    def get_engine(self):
        return self.engine

    def get_connection(self):
        pid = os.getpid()
        if pid in self.connections:
            return self.connections[pid]
        conn = self.engine.connect()
        self.connections[pid] = conn
        return conn

    def dispose(self):
        for conn in self.connections.itervalues():
            conn.close()
        self.engine.dispose()

    def execute(self, stmt, params=None):
        self.get_connection().execute(stmt, params)

    def select(self, stmt, params=None):
        result = self.get_connection().execute(stmt, params)
        row_cnt = result.rowcount
        rows = result.fetchall()
        result.close()
        return rows, row_cnt


def create_table(runner, name, table_ddl):
    stmt = """
        SELECT 1
        FROM pg_class
        WHERE relname = %(name)s
        """
    params = {"name": name}
    _, row_cnt = runner.select(stmt, params)
    if row_cnt > 0:
        print "table was already created: %s" % name
    else:
        try:
            runner.execute(table_ddl)
            print "table was created: %s" % name
        except sqlalchemy.exc.IntegrityError as e:
            print "[except] table was already created: %s" % name
            pass


def table_exists(runner, name):
    sql = """
        SELECT 1 FROM pg_class WHERE relname = %(name)s
    """
    _, cnt = runner.select(sql, {"name": name})
    return cnt == 1
