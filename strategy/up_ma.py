# -*- coding: utf-8 -*-

# Copyright (c) 2016 Grant AO. All rights reserved.


from da.dbutil import SqlRunner
from utils.aux_tables import insert_more_ma_stock_daily
from utils.constants import STOCK_RECOMMEND, STOCK_DAILY_MORE_MA, STOCK_INFO
from utils.stock_day import stock_latest_day


class UpMA(object):
    def __init__(self, end_day=None, push_days=10, up_days=5, volume_increase=4):
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
              SELECT DISTINCT code
              FROM (
                   select code,
                   sum(volume) filter (where p_change < 0) as cons,
                   sum(volume) filter (where p_change >= 0) as pos,
                   (array_agg(date ORDER BY close DESC))[1] as max_date
                   from {stock_daily_more}
                   where close >= ma5 and close >= ma10 and close >= ma20 and
                   close >= ma30 and close >= ma60 and
                   date >= %(end_day)s - %(push_days)s
                   group by code
                   having count(*) >= %(up_days)s
                   order by code
                   ) s
                where pos >= %(volume_increase)s * cons AND
                          max_date = %(end_day)s
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
    # insert_more_ma_stock_daily()
    uta = UpMA()
    uta.recommend()
