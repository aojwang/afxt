# -*- coding: utf-8 -*-

# Copyright (c) 2016 Grant AO. All rights reserved.
import functools
import datetime
import django.core.mail


CONT_INCREASE_HEADER = u'''
连续上涨买入（{date}）

连续上涨天数大于等于3天的股票（1％～5%），越靠前的股票出现次数越多！股票有可能重复出现，比如最近3次和4次都上涨！
'''


CALLBACK_HEADER = u'''
突破趋势回调买入（{date})

突破趋势回调买入：以上证指数为参考，自股灾以来最低点（2638）开始计算波段，期间有若干个顶，突破这些顶为突破趋势，然后回调，且回调幅度小于15%，并且最新收盘价在ma5/10之上。从2015-11-09到2016-01-27得到前期高点，选择那些突破趋势，离前期高点至少有20%的上涨空间，得到以下股票:
'''


UP_TREND_HEADER = u'''
突破趋势买入（{date})

突破趋势买入：以上证指数为参考，自股灾以来最低点（2638）开始计算波段，期间有若干个顶，突破这些顶（最新收盘价高于这些顶）为突破趋势，并且超越上次高点涨幅小于10%。从2015-11-09到2016-01-27得到前期高点，选择那些突破趋势，并且离前期高点至少有20%的上涨空间。得到以下股票:
'''

def send_mail(subject, message, from_email, recipient_list,
              fail_silently=False, auth_user=None, auth_password=None,
              connection=None):
    try:
        django.core.mail.send_mail(subject, message, from_email, recipient_list, fail_silently,
                  auth_user, auth_password, connection)
    except Exception, e:
        raise e


class SendEmail(object):
    def __init__(self, _type):
        self._type = _type
        self.header = {
            'callback': CALLBACK_HEADER,
            'up_trend': UP_TREND_HEADER,
            'increase': CONT_INCREASE_HEADER,
        }.get(self._type)

    @staticmethod
    def _send_email(subject, message):
        send_mail(subject,
                  message,
                  'hi@nile.org',
                  ['aojw2008@gmail.com']
                  )

    def _get_code_trade(self, code):
        if code.startswith('6'):
            return 'SH'
        return 'SZ'

    def _get_xueqiu_site(self, code):
        return ''.join(['https://xueqiu.com/S/', self._get_code_trade(code), code])

    def _gen_body(self, stock_name_codes):
        result = [self.header.format(date=str(datetime.date.today()))]
        i = 0
        for industry, name, code, p_change, in stock_name_codes:
            if i < 3:
                result.append(''.join(['[', industry, ']', '[', p_change, ']:', '$', name, '(', self._get_code_trade(code), code, ')$']))
                i += 1
            result.append(''.join(['[', industry, ']', '[', p_change, ']:', '\t', ' : '.join([name, self._get_xueqiu_site(code)])]))
            
        return '\n'.join(result)

    def __call__(self, original_func):
        @functools.wraps(original_func)
        def decorated(*args, **kwargs):
            subject, message = '', ''
            try:
                stock_name_codes = original_func(*args, **kwargs)
                message = self._gen_body(stock_name_codes)
                subject = 'daily update (S)' + str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M'))
            except Exception as e:
                message = str(e)
                subject = 'daily update (F)' + str(datetime.date.today())
            finally:
                self._send_email(subject, message)

        return decorated
