# coding=utf-8

import datetime
import itertools
import operator

from da import dbutil
from da.dbutil import SqlRunner


class UpTrendVolumeAnalysis(object):
    def __init__(self, start_day, end_day, volume_cnt, neg_volume_percent):
        self.start_day = start_day
        self.end_day = end_day
        self.runner = SqlRunner()
        self.volume_cnt = volume_cnt
        self.neg_volume_percent = neg_volume_percent

    def latest_day(self):
        sql = 'SELECT max(date) FROM stock_daily'
        rows, _ = self.runner.select(sql)
        return rows[0][0]

    def check_percent(self, a_list):
        result = []
        for x, y in zip(a_list, a_list[1:]):
            result.append(abs((y - x + 0.0) / x))
        return self.neg_volume_percent >= any(result)

    def check_volume(self, neg_list, pos_list):
        greater_cnt = 0
        for item in pos_list:
            if item < any(neg_list):
                greater_cnt += 1
        return greater_cnt <= self.volume_cnt

    def recommend(self):
        sql = """
            SELECT code, date, p_change, volume
            FROM stock_daily
            WHERE date BETWEEN %(start_day)s AND %(end_day)s
            ORDER BY code, date
            """
        params = {
            'start_day': self.start_day,
            'end_day': self.end_day
        }
        rows, cnt = self.runner.select(sql, params)
        rec_codes = []
        available_days = dbutil.get_available_days(self.runner, self.start_day, self.end_day)
        for code, info in itertools.groupby(rows, key=operator.itemgetter(0)):
            volume = list(info)
            if len(volume) < len(available_days) - 3:
                continue
            neg_volume = [v[3] for v in volume if v[2] < 0]
            pos_volume = [v[3] for v in volume if v[2] >= 0]

            if self.check_volume(neg_volume, pos_volume) and \
                self.check_percent(neg_volume):
                rec_codes.append(code)
                print code, neg_volume, pos_volume, [v[2] for v in volume]


        sql = """
            SELECT t.code, i.name, (max_close - min_close) / min_close FROM (
            SELECT code, max(close) FILTER (WHERE date = %(min_day)s) as min_close,
                    max(high) FILTER (WHERE date = %(max_day)s) as max_close,
                    max(p_change) FILTER (where date = %(min_day)s) as p_change
            FROM stock_daily
            WHERE date in (%(min_day)s, %(max_day)s) and code in %(code_tuple)s
            GROUP by code
            ) t, stock_info i
            where p_change < 9.9 and t.code = i.code
            ORDER BY (max_close - min_close) / min_close DESC
        """
        params = {
            'min_day': self.end_day + datetime.timedelta(days=1),
            'max_day': self.latest_day(),
            'code_tuple': tuple(rec_codes)
        }
        rows, _ = self.runner.select(sql, params)
        import pprint
        pprint.pprint(rows)

if __name__ == "__main__":
    uta = UpTrendVolumeAnalysis(datetime.date(2016, 9, 26),
                          datetime.date(2016, 10, 14), 2, 1)
    uta.recommend()
