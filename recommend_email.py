# -*- coding: utf-8 -*-

# Copyright (c) 2016 Grant AO. All rights reserved.
from da.dbutil import SqlRunner
from utils.email import SendEmail


def execute_stmt(stmt):
    runner = SqlRunner()
    rows, _ = runner.select(stmt)
    return rows

@SendEmail('up_trend')
def send_up_trend_email():
    stmt = '''
        SELECT name, code
        FROM stock_recommend
        WHERE update_time = now()::DATE AND reason = 'up_trend'
    '''
    result = execute_stmt(stmt)
    print 'up_trend'
    print result
    return result


@SendEmail('callback')
def send_callback_email():
    stmt = '''
        SELECT name, code
        FROM stock_recommend
        WHERE update_time = now()::DATE AND reason = 'callback'
    '''
    result = execute_stmt(stmt)
    print 'callback'
    print result
    return result


@SendEmail('increase')
def send_cont_increase_email():
    stmt = '''
        SELECT name, code
        FROM continuous_increase
        WHERE update_date = now()::DATE
        ORDER BY n_days DESC
    '''
    result = execute_stmt(stmt)
    print 'cont_increase'
    print result
    return result

if __name__ == '__main__':
    import sys
    reload(sys)  # Reload does the trick!
    sys.setdefaultencoding('UTF8')

    send_up_trend_email()
    send_callback_email()
    send_cont_increase_email()
