# -*- coding: utf-8 -*-

# Copyright (c) 2016 Grant AO. All rights reserved.
from da.dbutil import SqlRunner
from utils import stock_day


class TopIndustry(object):

    def __init__(self):
        self.runner = SqlRunner()

    def create_table(self):
        stmt = '''
            DROP TABLE IF EXISTS top_industry;
            CREATE TABLE top_industry (
                industry  TEXT,
                date      DATE,
                p_change  FLOAT8,
                positive_stock_ratio FLOAT8,
                top_n_stock TEXT[],
                top_n_change FLOAT8[],
                update_date DATE DEFAULT NOW()::DATE
            );
            DROP TABLE IF EXISTS top_n_industry;
            CREATE TABLE top_n_industry (
                industry  TEXT,
                p_change  FLOAT8[],
                positive_stock_ratio FLOAT8[],
                top_n_stock TEXT[],
                update_date DATE DEFAULT NOW()::DATE
            );
        '''
        self.runner.execute(stmt)

    def get_top_n(self, top_n, n_days, ignore_stock_n_days):
        stmt = '''
          DELETE FROM top_industry WHERE update_date = NOW()::DATE;
          INSERT INTO top_industry (industry, date, p_change, positive_stock_ratio, top_n_stock, top_n_change)
          SELECT industry, date, round(p_change::NUMERIC, 2) as p_change,
                round(((positive_cnt + 0.0) / (positive_cnt + negative_cnt))::NUMERIC, 2),
                 top_n_stock, top_n_change
          FROM (
            SELECT date, industry, avg(p_change) as p_change,
                   (array_agg(name || '(' || code || ')' ORDER BY p_change DESC))[1:%(top_n)s] as top_n_stock,
                   (array_agg(p_change ORDER BY p_change DESC))[1:%(top_n)s] as top_n_change,
                   count(*) FILTER (WHERE p_change > 0) as positive_cnt,
                   count(*) FILTER (WHERE p_change <=0) as negative_cnt
            FROM (
                SELECT d.date, i.industry, d.p_change, i.name, i.code
                FROM stock_info i, stock_daily d
                WHERE i.code = d.code AND
                      d.date BETWEEN %(start_day)s AND %(latest_day)s AND
                      i.code not in (
                        SELECT code
                        FROM stock_daily
                        WHERE date BETWEEN %(ignore_n_days_ago)s AND %(latest_day)s AND p_change > 9.5
                        GROUP BY code
                        HAVING count(*) >= %(ignore_stock_n_days)s
                      )
            ) t
            GROUP BY date, industry
          ) s;
          DELETE FROM top_n_industry WHERE update_date = NOW()::DATE;
          INSERT INTO top_n_industry (industry, p_change, positive_stock_ratio, top_n_stock)
          SELECT i.industry, i.p_change, i.positive_stock_ratio,
                 t.top_n_stock
          FROM
          (
          SELECT industry, array_agg(p_change order by date desc) as p_change,
                 array_agg(positive_stock_ratio order by date desc) as positive_stock_ratio
          FROM top_industry
          GROUP BY industry
          ) i,
          (
          SELECT industry,
              (array_agg(top_n_stock::TEXT || '+' || stock_cnt::TEXT ORDER BY stock_cnt DESC))[1:%(top_n)s] as top_n_stock
          FROM (
              SELECT industry, top_n_stock, count(*) as stock_cnt
              FROM (
                   SELECT industry, unnest(top_n_stock) as top_n_stock
                   FROM top_industry
                   WHERE date BETWEEN %(top_n_days)s AND %(latest_day)s
              ) s
              GROUP BY industry, top_n_stock
          ) m
          GROUP BY industry
          ) t
          WHERE i.industry = t.industry
        '''
        latest_day = stock_day.stock_latest_day(self.runner)
        params = {
            'top_n': top_n,
            'n_days': n_days,
            'latest_day': latest_day,
            'top_n_days': stock_day.stock_open_day(self.runner, latest_day, top_n),
            'start_day': stock_day.stock_open_day(self.runner, latest_day, n_days),
            'ignore_stock_n_days': ignore_stock_n_days,
            'ignore_n_days_ago': stock_day.stock_open_day(self.runner, latest_day, ignore_stock_n_days),
        }
        self.runner.execute(stmt, params)
        # import pprint
        # pprint.pprint(rows)

if __name__ == '__main__':
    ti = TopIndustry()
    ti.create_table()
    ti.get_top_n(5, 30, 3)
