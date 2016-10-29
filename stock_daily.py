# -*- coding: utf-8 -*-

# Copyright (c) 2016 Grant AO. All rights reserved.

from multiprocessing import Pool
import tushare as ts

from da import dbutil
from da.dbutil import SqlRunner
from utils.decorator import est_perf
from utils.stock_day import refresh_stock_list, get_all_stock_codes, get_start_day, delete_stock_data


class StockDaily(object):
    def __init__(self):
        self.runner = SqlRunner()

    def create_stock_daily_table(self):
        if not dbutil.table_exists(self.runner, 'stock_daily'):
            dbutil.create_table(self.runner, 'stock_daily', """
               CREATE TABLE stock_daily
               (
                 code text,
                 date date,
                 open double precision,
                 high double precision,
                 close double precision,
                 low double precision,
                 volume double precision,
                 price_change double precision,
                 p_change double precision,
                 ma5 double precision,
                 ma10 double precision,
                 ma20 double precision,
                 v_ma5 double precision,
                 v_ma10 double precision,
                 v_ma20 double precision,
                 turnover double precision
               )
            """)

    def create_stock_index_table(self):
        if not dbutil.table_exists(self.runner, 'stock_index'):
            dbutil.create_table(self.runner, 'stock_index', """
                CREATE TABLE stock_index
                (
                    code TEXT,
                    date DATE,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    volume DOUBLE PRECISION,
                    price_change DOUBLE PRECISION,
                    p_change DOUBLE PRECISION,
                    ma5 DOUBLE PRECISION,
                    ma10 DOUBLE PRECISION,
                    ma20 DOUBLE PRECISION,
                    v_ma5 DOUBLE PRECISION,
                    v_ma10 DOUBLE PRECISION,
                    v_ma20 DOUBLE PRECISION
                );
                CREATE INDEX ON stock_index (code, date);
            """)

    def get_stock_daily_by_code(self, table_name, stock_id, stock_code):
        start_day = get_start_day(self.runner, table_name, stock_code)
        try:
            if start_day:
                df = ts.get_hist_data(stock_id, start=start_day)
            else:
                df = ts.get_hist_data(stock_id)
            if df is None or df.empty:
                print "No need refresh %s, %s" % (stock_id, start_day)
            else:
                df["code"] = stock_code
                df.to_sql(table_name, self.runner.engine, if_exists='append')
                print "generating,", stock_code
        except Exception as e:
            print "wrong code", stock_code
            print e

    def dispose(self):
        self.runner.dispose()


def refresh_stock_daily(code):
    sda = StockDaily()
    sda.create_stock_daily_table()
    sda.get_stock_daily_by_code('stock_daily', code, code)
    sda.dispose()


@est_perf
def refresh_stock_index():
    code_map = {
        'sh': 'SH',
        'sz': 'SZ',
        'hs300': 'HS300',
        'sz50': 'SZ50',
        'zxb': 'ZXB',
        'cyb': 'CYB'
    }
    sda = StockDaily()
    sda.create_stock_index_table()
    for id, code in code_map.iteritems():
        sda.get_stock_daily_by_code('stock_index', id, code)
    sda.dispose()


def refresh_all_stock():
    refresh_stock_list()
    p = Pool(processes=16)
    code_list = get_all_stock_codes()
    p.map(refresh_stock_daily, sorted(code_list))


if __name__ == "__main__":
    # delete_stock_data()
    refresh_all_stock()
