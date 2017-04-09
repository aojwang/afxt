# -*- coding: utf-8 -*-

# Copyright (c) 2016 Grant AO. All rights reserved.

from multiprocessing import Pool
import tushare as ts

from da import dbutil
from da.dbutil import SqlRunner, get_pg_engine
from utils.decorator import est_perf
from utils.stock_day import get_all_stock_codes, stock_latest_day, delete_stock_data
from utils.constants import STOCK_TICK_DATA, STOCK_DAILY, STOCK_TICK_HIST_DATA


class TickData(object):
    def __init__(self):
        self.runner = SqlRunner()

    def drop_stock_tick_table(self, name):
        self.runner.execute("DROP TABLE IF EXISTS %s" % name)

    def create_stock_tick_table(self, name):
        dbutil.create_table(self.runner, name, """
            CREATE TABLE {name}
            (
                index BIGINT,
                name TEXT,
                open FLOAT8,
                pre_close FLOAT8,
                price FLOAT8,
                high FLOAT8,
                low FLOAT8,
                bid FLOAT8,
                ask FLOAT8,
                volume FLOAT8,
                amount FLOAT8,
                b1_v TEXT,
                b1_p TEXT,
                b2_v TEXT,
                b2_p TEXT,
                b3_v TEXT,
                b3_p TEXT,
                b4_v TEXT,
                b4_p TEXT,
                b5_v TEXT,
                b5_p TEXT,
                a1_v TEXT,
                a1_p TEXT,
                a2_v TEXT,
                a2_p TEXT,
                a3_v TEXT,
                a3_p TEXT,
                a4_v TEXT,
                a4_p TEXT,
                a5_v TEXT,
                a5_p TEXT,
                date DATE,
                time TIME,
                code TEXT
            );
           CREATE INDEX on {name} (code)
        """.format(name=name))

    def get_stock_tick_by_code(self, table_name, stock_code):
        try:
            df = ts.get_realtime_quotes(stock_code)
            if df is None or df.empty:
                print "No need refresh %s" % stock_code
            else:
                df["code"] = stock_code
                df.to_sql(table_name, self.runner.engine, if_exists='append')
                df.to_sql(STOCK_TICK_HIST_DATA, self.runner.engine, if_exists='append')
                print "generating,", stock_code
        except Exception as e:
            print "wrong code", stock_code
            print e

    def dispose(self):
        self.runner.dispose()


def refresh_stock_tick_by_code(code):
    sda = TickData()
    sda.get_stock_tick_by_code(STOCK_TICK_DATA, code)
    sda.dispose()


@est_perf
def refresh_today_all():
    df = ts.get_today_all()
    df.to_sql('stock_today_all', get_pg_engine(), if_exists='append')


@est_perf
def refresh_stock_tick():
    sda = TickData()
    sda.drop_stock_tick_table(STOCK_TICK_DATA)
    sda.create_stock_tick_table(STOCK_TICK_DATA)
    sda.create_stock_tick_table(STOCK_TICK_HIST_DATA)
    sda.dispose()
    p = Pool(processes=16)
    code_list = get_all_stock_codes()
    p.map(refresh_stock_tick_by_code, sorted(code_list))


@est_perf
def convert_tick_to_daily():
    sql = """
        INSERT INTO {stock_daily}(
          code, date, open, high, low, close, volume,
          price_change, p_change, ma5, ma10, ma20,
          v_ma5, v_ma10, v_ma20, turnover)
        SELECT s.code, s.date, s.open, s.high, s.low, s.price, s.volume / 100,
          s.price - s.pre_close,
          ((s.price - s.pre_close) / s.pre_close) * 100,
          (t.ma5 + (s.price - t.price5) / 5),
          (t.ma10 + (s.price - t.price10) / 10),
          (t.ma20 + (s.price - t.price20) / 20),
          0, 0, 0, 0
        FROM {stock_tick} s,
           (
             SELECT
               code,
               max(close)
                 FILTER (WHERE rank = 5)  AS price5,
               max(close)
                 FILTER (WHERE rank = 10) AS price10,
               max(close)
                 FILTER (WHERE rank = 20) AS price20,
               max(ma5)
                 FILTER (WHERE rank = 1)  AS ma5,
               max(ma10)
                 FILTER (WHERE rank = 1)  AS ma10,
               max(ma20)
                 FILTER (WHERE rank = 1)  AS ma20
             FROM (
                    SELECT
                      code,
                      close,
                      date,
                      ma5,
                      ma10,
                      ma20,
                      row_number()
                      OVER (PARTITION BY code
                        ORDER BY date DESC) AS rank
                    FROM {stock_daily}
                    WHERE date >= now()::DATE - 90
                  ) t
             GROUP BY code
           ) t
        WHERE s.code = t.code
    """.format(stock_daily=STOCK_DAILY, stock_tick=STOCK_TICK_DATA)
    runner = SqlRunner()
    runner.execute(sql)


if __name__ == "__main__":
    refresh_today_all()
    refresh_stock_tick()
    delete_stock_data()
    convert_tick_to_daily()
