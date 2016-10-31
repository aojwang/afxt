# -*- coding: utf-8 -*-

# Copyright (c) 2016 Grant AO. All rights reserved.

import datetime
from da.dbutil import SqlRunner
from utils import aux_tables
from utils.stock_day import stock_latest_day
from utils.constants import STOCK_MAX_CLOSE_DAY, STOCK_MIN_MAX_DAY, STOCK_INFO, STOCK_PEAK_CLOSE, STOCK_RECOMMEND, \
    STOCK_LOW_CLOSE, STOCK_DAILY, STOCK_DAYS


class AscendIncrease(object):
    def __init__(self, trend_start_day, trend_end_day=None):
        self.runner = SqlRunner()
        self.trend_start_day = trend_start_day
        self.trend_end_day = trend_end_day if trend_end_day else stock_latest_day(self.runner)

    def prerequsite(self):
        aux_tables.refresh_stock_days()
        aux_tables.refresh_peak_close(self.runner, self.trend_start_day, self.trend_end_day)
        aux_tables.refresh_low_close(self.runner, self.trend_start_day, self.trend_end_day)
        aux_tables.create_recommend_table(self.runner)

    def recommend(self):
        sql = """
            DELETE FROM {name} WHERE update_time = now()::DATE AND reason = '{reason}';
            INSERT INTO {name}
            SELECT DISTINCT i.code, i.name, i.industry, i.esp, i.pe, 0.0,
                   now()::DATE as update_time, '{reason}' as reason
            FROM (
            SELECT DISTINCT s.code
            FROM
                (
                SELECT l.code, count(*) filter (where l.rank = l.date_rank) as l_cnt,
                      max(l.rank) as l_t_cnt
                FROM {stock_low} l
                GROUP BY l.code
                ) s,
                (
                SELECT p.code, count(*) filter (where p.rank = p.date_rank) as p_cnt,
                   max(p.rank) as p_t_cnt
                FROM {stock_peak} p
                GROUP BY p.code
                ) t
                where s.code = t.code and s.l_cnt = s.l_t_cnt and t.p_cnt = t.p_t_cnt
            ) s,
            {stock_info} i,
            {stock_daily} d,
            {stock_days} t
            WHERE s.code = i.code and s.code = d.code and d.date = %(trend_end)s AND
                  d.close >= d.ma5 and d.close >= d.ma10 and d.close >= d.ma20 AND
                  d.p_change <= 9.0 and s.code = t.code and t.num_days > 30
            """.format(stock_max=STOCK_MAX_CLOSE_DAY, stock_min_max=STOCK_MIN_MAX_DAY, reason='asc',
                       stock_info=STOCK_INFO, stock_peak=STOCK_PEAK_CLOSE, name=STOCK_RECOMMEND,
                       stock_low=STOCK_LOW_CLOSE, stock_daily=STOCK_DAILY,
                       stock_days=STOCK_DAYS)
        params = {
            'trend_end': self.trend_end_day,
            'trend_start': self.trend_start_day
        }
        self.runner.execute(sql, params)


def run():
    uta = AscendIncrease(datetime.date(2016, 5, 30))
    uta.prerequsite()
    uta.recommend()


if __name__ == "__main__":
    run()
