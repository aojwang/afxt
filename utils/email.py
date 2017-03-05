# -*- coding: utf-8 -*-

# Copyright (c) 2016 Grant AO. All rights reserved.
import functools

import django.core.mail


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
        self._type = type

    @staticmethod
    def _send_email(subject, message):
        send_mail(subject,
                  message,
                  'aojw@appannie.com',
                  'grant@appannie.com'
                  )

    def _get_xueqiu_site(self, code):
        if code.startswith('6'):
            return 'https://xueqiu.com/S/SH' + code
        return 'https://xueqiu.com/S/SZ' + code

    def _gen_body(self, stock_name_codes):
        result = []
        for name, code, in stock_name_codes:
            result.append(' : '.join([name, self._get_xueqiu_site(code)]))
        return '\n'.join([self._type] + result)

    def __call__(self, original_func):
        @functools.wraps(original_func)
        def decorated(*args, **kwargs):
            subject, message = '', ''
            try:
                stock_name_codes = original_func(*args, **kwargs)
                message = self._gen_body(stock_name_codes)
                subject = 'daily update (S)'
            except Exception as e:
                message = str(e)
                subject = 'daily update (F)'
            finally:
                self._send_email(subject, message)

        return decorated