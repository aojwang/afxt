# coding=utf-8

import datetime

from da.dbutil import SqlRunner


class UpTrendAnalysis(object):
    def __init__(self, trend_start_day,
                 pre_start_day, trend_percent, up_percent):
        self.trend_start_day = trend_start_day
        self.pre_start_day = pre_start_day
        self.pre_end_day = trend_start_day - datetime.timedelta(days=1)
        self.runner = SqlRunner()
        self.trend_percent = trend_percent
        self.up_percent = up_percent

    def stock_min_close(self):
        sql = """
            DROP TABLE IF EXISTS stock_min_close;
            CREATE TABLE stock_min_close AS
              select code, date, close, min_close,
                row_number() OVER (PARTITION BY code ORDER BY close DESC) as rank
              from (
                select code, date, close,
                  min(close) over ten_window as min_close
                from stock_daily
                where date >= %(trend_start)s
                WINDOW ten_window AS (partition by code ORDER BY date ROWS BETWEEN 10 PRECEDING AND 10 FOLLOWING)
                   ) t
              where (close = min_close);
            """
        self.runner.execute(sql, {"trend_start": self.trend_start_day})

    def prerequsite(self):
        self.stock_min_close()

    def latest_day(self):
        sql = 'SELECT max(date) FROM stock_daily'
        rows, _ = self.runner.select(sql)
        return rows[0][0]

    def recommend(self):
        sql = """
            DELETE FROM stock_recommend WHERE update_time = now()::DATE and reason = 'up_trend_low';
            INSERT INTO stock_recommend
            WITH stock_percent AS(
                SELECT t.code, (close_list[1] - close_list[2] + 0.0) / close_list[2] as percent
                FROM (
                    SELECT
                      m.code,
                      array_agg(close ORDER BY date DESC) AS close_list
                    FROM stock_min_close m
                    WHERE rank <= 2
                    GROUP BY m.code
                      ) t
                WHERE (close_list[1] - close_list[2] + 0.0) / close_list[2] <= %(trend_percent)s
            )
            SELECT DISTINCT m.code, i.name, i.industry, i.esp, i.pe, p.percent,
                   now()::DATE as update_time, 'up_trend_low' as reason
            FROM stock_min_close m, stock_min_max_date d, stock_info i, stock_percent p, stock_highest h
            WHERE m.code = d.code AND d.min_date <= %(trend_start)s AND m.code = p.code AND
              m.rank = 1 AND (m.date BETWEEN %(latest_day)s - 10 AND %(latest_day)s - 1) AND m.code = i.code AND i.name NOT LIKE '%%ST%%' AND
              m.code = h.code and (h.close - m.close + 0.0) / h.close >= %(up_percent)s
            ORDER BY percent DESC ;
            """
        params = {
            'trend_percent': self.trend_percent,
            'up_percent': self.up_percent,
            'latest_day': self.latest_day(),
            'trend_start': self.trend_start_day
        }
        print self.latest_day()
        self.runner.execute(sql, params)


if __name__ == "__main__":
    uta = UpTrendAnalysis(datetime.date(2016, 1, 28),
                          datetime.date(2015, 11, 9),
                          0.1, 0.2)
    # uta.prerequsite()
    uta.recommend()
