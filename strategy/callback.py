# -*- coding: utf-8 -*-

# Copyright (c) 2016 Grant AO. All rights reserved.

import datetime

from da.dbutil import SqlRunner
from utils import aux_tables
from utils.constants import STOCK_RECOMMEND, STOCK_MAX_CLOSE_DAY, STOCK_INFO, STOCK_PEAK_CLOSE, STOCK_MIN_MAX_DAY, \
    STOCK_DAILY
from utils.stock_day import stock_latest_day


class CallbackAnalysis(object):
    def __init__(self, trend_start_day,
                 pre_start_day, trend_percent, up_percent, trend_end_day=None):
        self.runner = SqlRunner()
        self.trend_start_day = trend_start_day
        self.trend_end_day = trend_end_day if trend_end_day else stock_latest_day(self.runner)
        self.pre_start_day = pre_start_day
        self.pre_end_day = trend_start_day - datetime.timedelta(days=1)
        self.trend_percent = trend_percent
        self.up_percent = up_percent

    def prerequsite(self):
        aux_tables.refresh_stock_min_max_day(self.runner)
        aux_tables.refresh_stock_max_close_day(self.runner, self.pre_start_day, self.pre_end_day)
        aux_tables.refresh_peak_close(self.runner, self.trend_start_day, self.trend_end_day)
        aux_tables.create_recommend_table(self.runner)

    def recommend(self):
        sql = """
            DELETE FROM {name}
            WHERE update_time = now()::DATE AND reason = '{reason}';
            INSERT INTO {name}
            WITH stock_percent AS(
                SELECT t.code, (close_list[1] - close_list[2] + 0.0) / close_list[2] as percent
                FROM (
                    SELECT
                      m.code,
                      array_agg(close ORDER BY date DESC) AS close_list
                    FROM stock_max_close m
                    WHERE rank <= 2
                    GROUP BY m.code
                      ) t
                WHERE (close_list[1] - close_list[2] + 0.0) / close_list[2] <=  %(trend_percent)s
            )
            SELECT DISTINCT m.code, i.name, i.industry, i.esp::FLOAT8, i.pe, p.percent,
                   now()::DATE as update_time, '{reason}' as reason
            FROM {stock_peak} m, {stock_min_max} d, {stock_info} i, stock_percent p, {stock_max} h,
              {stock_daily} e
            WHERE m.code = d.code AND d.min_date <=  %(start_day)s AND m.code = p.code AND
              m.rank = 1 AND m.date BETWEEN %(end_day)s - 14 AND %(end_day)s - 1 AND
              m.code = i.code AND i.name NOT LIKE '%%ST%%' AND
              m.code = h.code AND (h.close - m.close + 0.0) / h.close >= %(up_percent)s AND
              m.code = e.code AND e.date = %(end_day)s AND e.ma5 <= e.close AND e.ma10 <= e.close AND
              e.ma20 <= e.close
            ORDER BY percent DESC ;
            """.format(stock_max=STOCK_MAX_CLOSE_DAY, stock_min_max=STOCK_MIN_MAX_DAY,
                       stock_info=STOCK_INFO, stock_peak=STOCK_PEAK_CLOSE, stock_daily=STOCK_DAILY,
                       name=STOCK_RECOMMEND, reason='callback')
        params = {
            'trend_percent': self.trend_percent,
            'up_percent': self.up_percent,
            'end_day': self.trend_end_day,
            'start_day': self.trend_start_day
        }
        self.runner.execute(sql, params)

def run():
    uta = CallbackAnalysis(datetime.date(2016, 1, 28),
                           datetime.date(2015, 11, 9),
                           0.1, 0.2)
    uta.recommend()

if __name__ == "__main__":
    run()

