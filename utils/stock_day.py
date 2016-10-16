# -*- coding: utf-8 -*-

# Copyright (c) 2016 Grant AO. All rights reserved.

import datetime
import tushare as ts

from da.dbutil import get_pg_engine, SqlRunner
from utils import constants
from utils.decorator import est_perf
from utils.constants import INDEX_NAMES, STOCK_DAILY


def get_start_day(runner, name, code):
    stmt = """
        SELECT date
        FROM %s
        WHERE code = '%s'
        ORDER BY date DESC LIMIT 1
    """ % (name, code)
    rows, row_cnt = runner.select(stmt)
    if row_cnt == 0:
        return None
    else:
        return str(rows[0][0] + datetime.timedelta(days=1))


def get_available_days(runner, start_day, end_day):
    stmt = """
        SELECT date
        FROM {name}
        WHERE code = 'sh' AND
              date BETWEEN %(start_day)s AND %(end_day)s
    """.format(name=STOCK_DAILY)
    params = {'start_day': start_day, 'end_day': end_day}
    rows, _ = runner.select(stmt, params)
    return [row[0] for row in rows]


def get_all_stock_codes():
    stock_code_list = set()
    code_df = ts.get_stock_basics()
    for code in code_df.index:
        if code is None and len(code) != 6:
            print "invalid code:", code
        else:
            stock_code_list.add(code)
    stock_code_list.update(INDEX_NAMES)
    return stock_code_list


@est_perf
def refresh_stock_list():
    engine = get_pg_engine()
    code_df = ts.get_stock_basics()
    code_df.to_sql(constants.STOCK_INFO, engine, if_exists='replace')
    engine.dispose()
    runner = SqlRunner()
    runner.execute("CREATE INDEX on %s(code)" % constants.STOCK_INFO)
    runner.dispose()


def stock_latest_day(runner):
    sql = """
        SELECT max(date)
        FROM %s
        WHERE code = 'sh'
        """ % constants.STOCK_DAILY
    rows, _ = runner.select(sql)
    return rows[0][0]


@est_perf
def delete_stock_data():
    runner = SqlRunner()
    runner.execute("DELETE FROM {name} WHERE date = %(day)s".format(name=STOCK_DAILY),
                   {"day": stock_latest_day(runner)})
    runner.dispose()
