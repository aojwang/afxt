# -*- coding: utf-8 -*-

# Copyright (c) 2016 Grant AO. All rights reserved.

from multiprocessing import Pool

import datetime
import tushare as ts

from da import dbutil
from da.dbutil import SqlRunner
from utils import constants
from utils.decorator import est_perf
from utils.stock_day import refresh_stock_list, get_all_stock_codes, get_start_day, delete_stock_data, \
    refresh_stock_concept, stock_open_day, stock_latest_day


class StockIncreaseRatio(object):
    def __init__(self, interval, end_day=None):
        self.runner = SqlRunner()
        self.interval = interval
        self.end_day = end_day if end_day else stock_latest_day(self.runner)
        self.start_day = stock_open_day(self.runner, self.end_day, self.interval)

    def create_table(self):
        if not dbutil.table_exists(self.runner, constants.STOCK_INCREASE):
            dbutil.create_table(self.runner, constants.STOCK_INCREASE, """
               CREATE TABLE {name}
               (
                 code text,
                 start_day date,
                 end_day date,
                 percent float8,
                 high_percent float8,
                 name TEXT,
                 c_name TEXT
               );
               CREATE INDEX on {name} (code);
               CREATE INDEX on {name} (start_day, end_day);
            """.format(name=constants.STOCK_INCREASE))

    def get_ratio(self):
        self.create_table()
        print self.start_day, self.end_day
        sql = """
            DELETE FROM {name}
            WHERE start_day = %(start_day)s and end_day = %(end_day)s;
            INSERT INTO {name}
            SELECT s.code, t.date as start_day, s.date as end_day,
            (s.close - t.close) / t.close as percent, r.high_percent,
            c.name, c.c_name
            FROM
            (
            SELECT code, date, close
            FROM {stock_daily}
            WHERE date = %(end_day)s
            ) s,
            (
            SELECT code, date, close
            FROM {stock_daily}
            WHERE date = %(start_day)s
            ) t,
            (
            SELECT code, (max(high) - min(low)) / min(low) as high_percent
            FROM {stock_daily}
            WHERE date BETWEEN %(start_day)s and %(end_day)s
            GROUP BY code
            ) r,
            {stock_concept} c
            WHERE s.code = t.code and s.code = r.code and r.code = c.code
        """.format(stock_daily=constants.STOCK_DAILY, name=constants.STOCK_INCREASE,
                   stock_concept=constants.STOCK_CONCEPT)
        params = {
            'start_day': self.start_day,
            'end_day': self.end_day
        }
        print sql, params
        self.runner.execute(sql, params)
        self.dispose()

    def dispose(self):
        self.runner.dispose()


if __name__ == "__main__":
    StockIncreaseRatio(5).get_ratio()
    StockIncreaseRatio(5, end_day=datetime.date(2016, 10, 21)).get_ratio()
