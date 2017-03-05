# -*- coding: utf-8 -*-

# Copyright (c) 2016 Grant AO. All rights reserved.
from da.dbutil import SqlRunner
from utils.email import SendEmail


def execute_stmt(stmt):
    runner = SqlRunner()
    rows, _ = runner.select(stmt)
    return rows

@SendEmail('up trend')
def send_up_trend_email():
    stmt = '''
        SELECT name, code
        FROM stock_recommend
        WHERE date = now()::DATE AND reason = 'up_trend'
    '''
    return execute_stmt(stmt)

@SendEmail('callback')
def send_callback_email():
    stmt = '''
        SELECT name, code
        FROM stock_recommend
        WHERE date = now()::DATE AND reason = 'callback'
    '''
    return execute_stmt(stmt)

@SendEmail('continuous increase')
def send_cont_increase_email():
    stmt = '''
        SELECT name, code
        FROM continuous_increase
        WHERE update_date = now()::DATE
        ORDER BY n_days DESC
    '''
    return execute_stmt(stmt)


if __name__ == '__main__':
    send_up_trend_email()
    send_callback_email()
    send_cont_increase_email()
