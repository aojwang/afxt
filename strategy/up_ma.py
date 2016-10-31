# -*- coding: utf-8 -*-

# Copyright (c) 2016 Grant AO. All rights reserved.


from da.dbutil import SqlRunner
from utils.aux_tables import insert_more_ma_stock_daily
from utils.constants import STOCK_RECOMMEND, STOCK_DAILY_MORE_MA, STOCK_INFO
from utils.stock_day import stock_latest_day


class UpMA(object):
    def __init__(self, end_day=None, push_days=7, up_days=3, volume_increase=2):
        self.runner = SqlRunner()
        self.end_day = end_day if end_day else stock_latest_day(self.runner)
        self.push_days = push_days
        self.up_days = up_days
        self.volume_increase = volume_increase

    def recommend(self):
        sql = """
            DELETE FROM {name} WHERE update_time = now()::DATE and reason = '{reason}';
            INSERT INTO {name}
            WITH ma_close AS (
                select distinct s.code
                from (
                        SELECT code, max_close
                        FROM (
                           select code,
                           sum(volume) filter (where p_change < 0) as cons,
                           sum(volume) filter (where p_change >= 0) as pos,
                           max(close) as max_close,
                           min(close) as min_close
                           from {stock_daily_more}
                           where
                           date >= %(end_day)s - %(push_days)s
                           group by code
                           having count(*) >= %(up_days)s
                           order by code
                           ) t
                           WHERE t.pos >= %(volume_increase)s * t.cons AND
                                 (t.max_close - t.min_close) / t.min_close >= 0.04
                       ) s,
                       {stock_daily_more} t
                       WHERE s.code = t.code AND
                             t.date = %(end_day)s AND
                             t.close >= ma5 AND t.close >= ma10 AND t.close >= ma20 AND
                             t.close >= ma30 AND t.close >= ma60 AND t.close >= s.max_close AND
                             t.p_change <= 5.0 AND (t.high - t.close) / t.open <= 0.01
                )
            SELECT DISTINCT c.code, i.name, i.industry, i.esp, i.pe, 0.0,
                   now()::DATE as update_time, '{reason}' as reason
            FROM {stock_info} i,  ma_close c
            WHERE i.code = c.code
            ORDER BY industry DESC ;
            """.format(name=STOCK_RECOMMEND, reason='up_ma',
                       stock_daily_more=STOCK_DAILY_MORE_MA, stock_info=STOCK_INFO)
        params = {
            'end_day': self.end_day,
            'push_days': self.push_days,
            'up_days': self.up_days,
            'volume_increase': self.volume_increase
        }
        self.runner.execute(sql, params)


if __name__ == "__main__":
    insert_more_ma_stock_daily()
    uta = UpMA()
    uta.recommend()
