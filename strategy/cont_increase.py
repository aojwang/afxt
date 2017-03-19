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
            array_agg(p_change order by date), %(n_days)s
            FROM stock_daily s, stock_info t
            WHERE s.code = t.code AND
                  s.p_change between 1.0 and 5.0 AND
                  s.date >= %(n_days_ago)s
            group by t.name, s.code, t.industry
            having count(*) >= %(n_days)s
        '''
        latest_day = stock_day.stock_latest_day(self.runner)
        params = {
            'n_days': n_days,
            'n_days_ago': stock_day.stock_open_day(self.runner, latest_day, n_days),
            'n_days': n_days
        }
        print stmt, params
        self.runner.execute(stmt, params)

if __name__ == '__main__':
    ci = ContinuousIncrease()
    ci.create_table()
    ci.get_top_n(2)
    ci.get_top_n(3)
    ci.get_top_n(4)
    ci.get_top_n(5)

