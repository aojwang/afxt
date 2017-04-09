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
        SELECT industry, name, code, round(percent::numeric, 2)::TEXT || '%%'
        FROM stock_recommend
        WHERE update_time = now()::DATE AND reason = 'up_trend'
    '''
    result = execute_stmt(stmt)
    print 'up_trend'
    return result


@SendEmail('callback')
def send_callback_email():
    stmt = '''
        SELECT industry, name, code, round(percent::numeric, 2)::TEXT || '%%'
        FROM stock_recommend
        WHERE update_time = now()::DATE AND reason = 'callback'
    '''
    result = execute_stmt(stmt)
    print 'callback'
    return result


@SendEmail('increase')
def send_cont_increase_email():
    stmt = '''
        SELECT industry, name, code, array_to_string(p_change, '%%,') || '%%'
        FROM continuous_increase
        WHERE update_date = now()::DATE
        ORDER BY n_days DESC, p_change[array_upper(p_change, 1)] DESC
    '''
    result = execute_stmt(stmt)
    print result
    print 'cont_increase'
    return result

@SendEmail('up_all_mas')
def send_upallmas_email():
    stmt = '''
        SELECT industry, name, code,round(p_change::numeric, 2)::TEXT || '%%'
        FROM up_all_mas
        WHERE update_date = now()::DATE
        ORDER BY n_days DESC, interval ASC
    '''
    result = execute_stmt(stmt)
    print 'up_all_mas'
    return result


if __name__ == '__main__':
    import sys
    reload(sys)  # Reload does the trick!
    sys.setdefaultencoding('UTF8')

    send_up_trend_email()
    send_callback_email()
    send_cont_increase_email()
    send_upallmas_email()
