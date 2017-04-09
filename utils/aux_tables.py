# -*- coding: utf-8 -*-

# Copyright (c) 2016 Grant AO. All rights reserved.
import datetime

from da.dbutil import create_table, SqlRunner
from utils import constants
from utils.constants import STOCK_DAILY_MORE_MA, STOCK_DAILY, STOCK_DAYS
from utils.decorator import est_perf
from utils.stock_day import stock_latest_day


@est_perf
def create_recommend_table(runner):
    sql = """
        CREATE TABLE {name}
        (
            code TEXT,
            name TEXT,
            industry TEXT,
            esp DOUBLE PRECISION,
            pe DOUBLE PRECISION,
            percent DOUBLE PRECISION,
            update_time DATE,
            reason TEXT
        );
    """.format(name=constants.STOCK_RECOMMEND)
    create_table(runner, constants.STOCK_RECOMMEND, sql)


@est_perf
def refresh_stock_min_max_day(runner):
    sql = """
        DROP TABLE IF EXISTS {name};
        CREATE TABLE {name} AS
          SELECT code, min(date) as min_date, max(date) as max_date, count(*) as days
          FROM {stock_table_name}
          GROUP BY code;
        """.format(name=constants.STOCK_MIN_MAX_DAY, stock_table_name=constants.STOCK_DAILY)
    runner.execute(sql)


@est_perf
def refresh_stock_max_close_day(runner, start_day, end_day):
    sql = """
          DROP TABLE IF EXISTS {name};
          CREATE TABLE {name} AS
            SELECT s.code, max(t.date) as date, s.close
            FROM (
                   SELECT
                     code,
                     max(close) AS close
                   FROM stock_daily
                   WHERE date BETWEEN %(start_day)s AND %(end_day)s
                   GROUP BY code
                 ) s, {stock_table_name} t
            WHERE s.code = t.code and s.close = t.close AND
                  t.date BETWEEN  %(start_day)s AND %(end_day)s
            GROUP BY s.code, s.close;
          """.format(name=constants.STOCK_MAX_CLOSE_DAY, stock_table_name=constants.STOCK_DAILY)
    params = {
        "start_day": start_day,
        "end_day": end_day
    }
    runner.execute(sql, params)


@est_perf
def refresh_stock_min_close_day(runner, start_day, end_day):
    sql = """
          DROP TABLE IF EXISTS {name};
          CREATE TABLE {name} AS
            SELECT s.code, min(t.date) as date, s.close
            FROM (
                   SELECT
                     code,
                     max(close) AS close
                   FROM {stock_table_name}
                   WHERE date BETWEEN %(start_day)s AND %(end_day)s
                   GROUP BY code
                 ) s, {stock_table_name} t
            WHERE s.code = t.code and s.close = t.close AND
                  t.date BETWEEN  %(start_day)s AND %(end_day)s
            GROUP BY s.code, s.close;
          """.format(name=constants.STOCK_MIN_CLOSE_DAY, stock_table_name=constants.STOCK_DAILY)
    params = {
        "start_day": start_day,
        "end_day": end_day
    }
    runner.execute(sql, params)


@est_perf
def refresh_peak_close(runner, start_day, end_day):
    sql = """
        DROP TABLE IF EXISTS {name};
        CREATE TABLE {name} AS
          select code, date, close, max_close,
            row_number() OVER (PARTITION BY code ORDER BY close DESC) as rank,
            row_number() OVER (PARTITION BY code ORDER BY date DESC) as date_rank
          from (
            select code, date, close,
              max(close) over ten_window as max_close
            from {stock_table_name}
            where date BETWEEN %(start_day)s AND %(end_day)s
            WINDOW ten_window AS (partition by code ORDER BY date
            ROWS BETWEEN {left_interval} PRECEDING AND {right_interval} FOLLOWING)
               ) t
          where (close = max_close);
        """.format(name=constants.STOCK_PEAK_CLOSE, stock_table_name=constants.STOCK_DAILY,
                   left_interval=constants.PEAK_LEFT_INTERVAL, right_interval=constants.PEAK_RIGHT_INTERVAL)
    runner.execute(sql, {'start_day': start_day, 'end_day': end_day})


@est_perf
def refresh_low_close(runner, start_day, end_day):
    sql = """
        DROP TABLE IF EXISTS {name};
        CREATE TABLE {name} AS
          select code, date, close, min_close,
            row_number() OVER (PARTITION BY code ORDER BY close DESC) as rank,
            row_number() OVER (PARTITION BY code ORDER BY date DESC) as date_rank
          from (
            select code, date, close,
              min(close) over ten_window as min_close
            from {stock_table_name}
            where date BETWEEN %(start_day)s AND %(end_day)s
            WINDOW ten_window AS (partition by code ORDER BY date
            ROWS BETWEEN {left_interval} PRECEDING AND {right_interval} FOLLOWING)
               ) t
          where (close = min_close);
        """.format(name=constants.STOCK_LOW_CLOSE, stock_table_name=constants.STOCK_DAILY,
                   left_interval=constants.PEAK_LEFT_INTERVAL, right_interval=constants.PEAK_RIGHT_INTERVAL)
    runner.execute(sql, {'start_day': start_day, 'end_day': end_day})


@est_perf
def refresh_more_ma_stock_daily():
    runner = SqlRunner()
    sql = """
        DROP TABLE IF EXISTS {name};
        CREATE TABLE {name} AS
        SELECT *, avg(close) OVER ma30_win as ma30,
        avg(close) OVER ma60_win as ma60,
        avg(close) OVER ma120_win as ma120,
        avg(close) OVER ma250_win as ma250
        FROM {stock_daily}
        WINDOW ma30_win AS (partition by code ORDER BY date
        ROWS 29 PRECEDING),
        ma60_win AS (partition by code ORDER BY date
        ROWS 59 PRECEDING),
        ma120_win AS (partition by code ORDER BY date
        ROWS 119 PRECEDING),
        ma250_win AS (partition by code ORDER BY date
        ROWS 249 PRECEDING);
        CREATE INDEX on {name} (code);
        CREATE INDEX on {name} (date);
    """.format(name=STOCK_DAILY_MORE_MA, stock_daily=STOCK_DAILY)
    runner.execute(sql)
    runner.dispose()


def _get_latest_day(name):
    runner = SqlRunner()
    rows, cnt = runner.select("""
        SELECT max(date)
        FROM {name}
        WHERE code = 'sh'
    """.format(name=name))
    runner.dispose()
    return rows[0][0] if rows[0][0] else datetime.date(1900, 1, 1)


@est_perf
def delete_stock_more_data():
    runner = SqlRunner()
    runner.execute("DELETE FROM {name} WHERE date = %(day)s".format(name=STOCK_DAILY_MORE_MA),
                   {"day": stock_latest_day(runner)})
    runner.dispose()


@est_perf
def insert_more_ma_stock_daily():
    stock_daily_latest = _get_latest_day(STOCK_DAILY)
    stock_daily_more_latest = _get_latest_day(STOCK_DAILY_MORE_MA)
    print stock_daily_latest, stock_daily_more_latest
    if stock_daily_latest == stock_daily_more_latest:
        print "no need refresh"
        return
    sql = """
        INSERT INTO {name}
        SELECT *
        FROM (
        SELECT *, avg(close) OVER ma30_win as ma30,
          avg(close) OVER ma60_win as ma60,
          avg(close) OVER ma120_win as ma120,
          avg(close) OVER ma250_win as ma250
        FROM {stock_daily}
        WHERE date >= %(more_latest)s - 100
        WINDOW ma30_win AS (partition by code ORDER BY date
        ROWS 29 PRECEDING),
        ma60_win AS (partition by code ORDER BY date
        ROWS 59 PRECEDING),
        ma120_win AS (partition by code ORDER BY date
        ROWS 119 PRECEDING),
        ma250_win AS (partition by code ORDER BY date
        ROWS 249 PRECEDING)
        ) t
        WHERE date > %(more_latest)s
    """.format(name=STOCK_DAILY_MORE_MA, stock_daily=STOCK_DAILY)
    runner = SqlRunner()
    runner.execute(sql, {'more_latest': stock_daily_more_latest})
    runner.dispose()


def refresh_stock_days():
    runner = SqlRunner()
    runner.execute("""
        DROP TABLE IF EXISTS {name};
        CREATE TABLE {name} AS
        SELECT code, count(*) as num_days
        FROM {stock_daily}
        GROUP BY code
    """.format(stock_daily=STOCK_DAILY, name=STOCK_DAYS))
    runner.dispose()


if __name__ == "__main__":
    refresh_more_ma_stock_daily()
    insert_more_ma_stock_daily()
