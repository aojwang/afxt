# -*- coding: utf-8 -*-

# Copyright (c) 2016 Grant AO. All rights reserved.
from da.dbutil import SqlRunner
from utils.email_ import SendEmail


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
    print 'up_trend', len(result)
    return result


@SendEmail('callback')
def send_callback_email():
    stmt = '''
        SELECT industry, name, code, round(percent::numeric, 2)::TEXT || '%%'
        FROM stock_recommend
        WHERE update_time = now()::DATE AND reason = 'callback'
    '''
    result = execute_stmt(stmt)
    print 'callback', len(result)
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
    print 'cont increase', len(result)
    return result

@SendEmail('up_all_mas')
def send_upallmas_email():
    stmt = '''
     SELECT  industry, name, code, p_change
     FROM (
        SELECT n_days, industry, name, code,round(p_change::numeric, 2)::TEXT || '%%' as p_change, min(interval) as interval
        FROM up_all_mas
        WHERE update_date = now()::DATE
        GROUP BY  industry, name, code, p_change, n_days
     ) t
     ORDER BY n_days DESC, interval ASC, p_change DESC
    '''
    result = execute_stmt(stmt)
    print 'up_all_mas', len(result)
    return result


if __name__ == '__main__':
    import sys
    reload(sys)  # Reload does the trick!
    sys.setdefaultencoding('UTF8')

    send_up_trend_email()
    send_callback_email()
    send_cont_increase_email()
    send_upallmas_email()
