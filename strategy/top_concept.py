# -*- coding: utf-8 -*-

# Copyright (c) 2016 Grant AO. All rights reserved.
from da.dbutil import SqlRunner
from utils import stock_day


class TopConcept(object):

    def __init__(self):
        self.runner = SqlRunner()

    def create_table(self):
        stmt = '''
            DROP TABLE IF EXISTS top_concept;
            CREATE TABLE top_concept (
                concept  TEXT,
                date      DATE,
                p_change  FLOAT8,
                positive_stock_ratio FLOAT8,
                top_n_stock TEXT[],
                top_n_change FLOAT8[],
                update_date DATE DEFAULT NOW()::DATE
            );
            DROP TABLE IF EXISTS top_n_concept;
            CREATE TABLE top_n_concept (
                concept  TEXT,
                p_change  FLOAT8[],
                positive_stock_ratio FLOAT8[],
                top_n_stock TEXT[],
                update_date DATE DEFAULT NOW()::DATE
            );
        '''
        self.runner.execute(stmt)

    def get_top_n(self, top_n, n_days):
        stmt = '''
          DELETE FROM top_concept WHERE update_date = NOW()::DATE;
          INSERT INTO top_concept (concept, date, p_change, positive_stock_ratio, top_n_stock, top_n_change)
          SELECT concept, date, round(p_change::NUMERIC, 2) as p_change,
                round(((positive_cnt + 0.0) / (positive_cnt + negative_cnt))::NUMERIC, 2),
                 top_n_stock, top_n_change
          FROM (
            SELECT date, concept, avg(p_change) as p_change,
                   (array_agg(name || '(' || code || ')' ORDER BY p_change DESC))[1:5] as top_n_stock,
                   (array_agg(p_change ORDER BY p_change DESC))[1:5] as top_n_change,
                   count(*) FILTER (WHERE p_change > 0) as positive_cnt,
                   count(*) FILTER (WHERE p_change <=0) as negative_cnt
            FROM (
                SELECT d.date, i.c_name as concept, d.p_change, i.name, i.code
                FROM stock_concept i, stock_daily d
                WHERE i.code = d.code AND
                      d.date BETWEEN %(start_day)s AND %(latest_day)s AND
                      i.code not in (
                        SELECT code
                        FROM stock_daily
                        WHERE date BETWEEN %(start_day)s AND %(latest_day)s AND p_change > 9.5
                        GROUP BY code
                        HAVING count(*) >= 2
                      )
            ) t
            GROUP BY date, concept
          ) s;
          DELETE FROM top_n_concept WHERE update_date = NOW()::DATE;
          INSERT INTO top_n_concept (concept, p_change, positive_stock_ratio, top_n_stock)
          SELECT i.concept, i.p_change, i.positive_stock_ratio,
                 t.top_n_stock
          FROM
          (
          SELECT concept, array_agg(p_change order by date desc) as p_change,
                 array_agg(positive_stock_ratio order by date desc) as positive_stock_ratio
          FROM top_concept
          GROUP BY concept
          ) i,
          (
          SELECT concept,
              (array_agg(top_n_stock::TEXT || '+' || stock_cnt::TEXT ORDER BY stock_cnt DESC))[1:5] as top_n_stock
          FROM (
              SELECT concept, top_n_stock, count(*) as stock_cnt
              FROM (
                   SELECT concept, unnest(top_n_stock) as top_n_stock
                   FROM top_concept
              ) s
              GROUP BY concept, top_n_stock
          ) m
          GROUP BY concept
          ) t
          WHERE i.concept = t.concept
        '''
        latest_day = stock_day.stock_latest_day(self.runner)
        params = {
            'top_n': top_n,
            'n_days': n_days,
            'latest_day': latest_day,
            'start_day': stock_day.stock_open_day(self.runner, latest_day, n_days)
        }
        self.runner.execute(stmt, params)
        # import pprint
        # pprint.pprint(rows)

if __name__ == '__main__':
    ti = TopConcept()
    ti.create_table()
    ti.get_top_n(20, 10)