# -*- coding: utf-8 -*-

# Copyright (c) 2016 Grant AO. All rights reserved.

import datetime
from da.dbutil import SqlRunner
from utils import aux_tables
from utils.aux_tables import insert_more_ma_stock_daily
from utils.stock_day import stock_latest_day, stock_open_day
from utils.constants import STOCK_MAX_CLOSE_DAY, STOCK_MIN_MAX_DAY, STOCK_INFO, STOCK_PEAK_CLOSE, STOCK_RECOMMEND, \
    STOCK_LOW_CLOSE, STOCK_DAILY, STOCK_DAYS, STOCK_DAILY_MORE_MA


class LatestAsc(object):
    def __init__(self, trend_start_day, trend_end_day=None):
        self.runner = SqlRunner()
        self.trend_start_day = trend_start_day
        self.trend_end_day = trend_end_day if trend_end_day else stock_latest_day(self.runner)

    def prerequsite(self):
        aux_tables.refresh_stock_days()
        aux_tables.refresh_peak_close(self.runner, self.trend_start_day, self.trend_end_day)
        aux_tables.refresh_low_close(self.runner, self.trend_start_day, self.trend_end_day)
        insert_more_ma_stock_daily()
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
            {stock_days} t,
            (
                SELECT code
                FROM {stock_daily_more}
                WHERE close >= ma5 and close >= ma10 and close >= ma20 AND
                      close >= ma30 and close >= ma60 and close >= ma120 AND
                      date >= %(pre_day)s
                GROUP BY code
                HAVING count(*) = %(pre_days)s
            ) q
            WHERE s.code = i.code and s.code = d.code and s.code = q.code AND d.date = %(trend_end)s AND
                  d.close >= d.ma5 and d.close >= d.ma10 and d.close >= d.ma20 AND
                  d.p_change between 0.0 AND 9.0 and s.code = t.code and t.num_days > 25 and d.open > 0 AND
                  i.name not like '%%ST%%'
            """.format(stock_max=STOCK_MAX_CLOSE_DAY, stock_min_max=STOCK_MIN_MAX_DAY, reason='latest_asc',
                       stock_info=STOCK_INFO, stock_peak=STOCK_PEAK_CLOSE, name=STOCK_RECOMMEND,
                       stock_low=STOCK_LOW_CLOSE, stock_daily=STOCK_DAILY,
                       stock_days=STOCK_DAYS, stock_daily_more=STOCK_DAILY_MORE_MA)
        params = {
            'trend_end': self.trend_end_day,
            'trend_start': self.trend_start_day,
            'pre_day': stock_open_day(self.runner, self.trend_end_day, 3),
            'pre_days': 3
        }
        self.runner.execute(sql, params)


def run():
    uta = LatestAsc(datetime.date(2016, 9, 26))
    uta.prerequsite()
    uta.recommend()


if __name__ == "__main__":
    run()
