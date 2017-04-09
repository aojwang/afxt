# -*- coding: utf-8 -*-

# Copyright (c) 2016 Grant AO. All rights reserved.

# -*- coding: utf-8 -*-

# Copyright (c) 2016 Grant AO. All rights reserved.
from da.dbutil import SqlRunner
from utils import stock_day


class ContinuousIncrease(object):

    def __init__(self):
        self.runner = SqlRunner()

    def create_table(self):
        stmt = '''
            DROP TABLE IF EXISTS continuous_increase;
            CREATE TABLE continuous_increase (
                industry  TEXT,
                code TEXT,
                name TEXT,
                p_change  FLOAT8[],
                n_days INT,
                update_date DATE DEFAULT NOW()::DATE
            );
        '''
        self.runner.execute(stmt)

    def get_top_n(self, n_days):
        stmt = '''
          DELETE FROM continuous_increase WHERE update_date = NOW()::DATE AND n_days = %(n_days)s;
          INSERT INTO continuous_increase (industry, code, name, p_change, n_days)
          SELECT t.industry, s.code, t.name,
            array_agg(s.p_change order by s.date), %(n_days)s
            FROM stock_daily s, stock_info t, stock_daily sd
            WHERE s.code = t.code AND
                  s.p_change between -3.0 and 5.0 AND
                  s.date >= %(n_plus_one_days_ago)s AND
                  sd.code = s.code AND sd.date = %(latest_day)s AND
                  (sd.high - sd.close) / sd.close <= 0.02 AND
                  sd.close > sd.open AND sd.low <= (sd.close - sd.price_change)
            group by t.name, s.code, t.industry
            having sum((s.p_change >= 1.0)::INT) >= %(n_days)s AND
                   (ARRAY_AGG(s.p_change ORDER BY s.date ASC))[1]BETWEEN -3.0 AND 0 AND
                   (ARRAY_AGG(s.p_change ORDER BY s.date DESC))[1] > 0.0
        '''
        latest_day = stock_day.stock_latest_day(self.runner)
        params = {
            'latest_day': latest_day,
            'n_days': n_days,
            'n_days_ago': stock_day.stock_open_day(self.runner, latest_day, n_days),
            'n_days': n_days,
            'n_plus_one_days_ago': stock_day.stock_open_day(self.runner, latest_day, n_days + 1)
        }
        print stmt, params
        self.runner.execute(stmt, params)


class UpAllMAs(object):

    def __init__(self):
        self.runner = SqlRunner()

    def create_table(self):
        stmt = '''
            DROP TABLE IF EXISTS up_all_mas;
            CREATE TABLE up_all_mas (
                industry  TEXT,
                code TEXT,
                name TEXT,
                p_change FLOAT,
                n_days INT,
                interval INT,
                update_date DATE DEFAULT NOW()::DATE
            );
        '''
        self.runner.execute(stmt)

    def get_top_n(self, n_days, interval):
        stmt = '''
          DELETE FROM up_all_mas WHERE update_date = NOW()::DATE and n_days = %(n_days)s and interval = %(interval)s;
          INSERT INTO up_all_mas (industry, code, name, p_change, n_days, interval)
          SELECT i.industry, i.code, i.name, t.p_change, %(n_days)s, %(interval)s
          FROM
            (
            SELECT code
            FROM stock_daily
            WHERE date >= %(start_date)s and p_change between -3 and 0
            group by code
            having count(*) >= %(n_days)s
            ) s,
            stock_info i,
            stock_daily_more t
            where s.code = t.code and t.date = %(latest_date)s and s.code = i.code
                  t.p_change between 0.1 and 5 and t.close > t.ma5 and t.close > t.ma10 and
                  t.close > t.ma20 and t.close > t.ma30 and t.close > t.ma60 and
                  t.close > t.ma120 and t.close > t.ma250;
        '''
        latest_date = stock_day.stock_latest_day(self.runner)
        params = {
            'latest_date': latest_date,
            'n_days': n_days,
            'start_date': stock_day.stock_open_day(self.runner, latest_date, n_days + interval)
        }
        print stmt, params
        self.runner.execute(stmt, params)


if __name__ == '__main__':
    ci = ContinuousIncrease()
    # ci.create_table()
    ci.get_top_n(2)
    ci.get_top_n(3)
    ci.get_top_n(4)
    ci.get_top_n(5)
    ums = UpAllMAs()
    ums.create_table()
    ums.get_top_n(2, 1)
    ums.get_top_n(2, 2)

