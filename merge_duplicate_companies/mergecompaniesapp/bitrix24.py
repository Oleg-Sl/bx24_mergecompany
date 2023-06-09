#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Wrapper over Bitrix24 cloud API"""
import time
from json import loads
from logging import info
from time import sleep
from requests import adapters, post, exceptions
from multidimensional_urlencode import urlencode

# Retries for API request
adapters.DEFAULT_RETRIES = 10

import os
import json


from . import service


class Bitrix24:
    api_url = 'https://%s/rest/%s.json'
    oauth_url = 'https://oauth.bitrix.info/oauth/token/'
    timeout = 60

    def __init__(self):
        """Create Bitrix24 API object
        :param domain: str Bitrix24 domain
        :param auth_token: str Auth token
        :param refresh_token: str Refresh token
        :param client_id: str Client ID for refreshing access tokens
        :param client_secret: str Client secret for refreshing access tokens
        """

        token_data = service.get_token()
        self.domain = token_data.get("domain", None)
        self.auth_token = token_data.get("auth_token", None)
        self.refresh_token = token_data.get("refresh_token", None)

        settings = service.get_settings_app()
        self.client_id = settings.get("client_id", None)
        self.client_secret = settings.get("client_secret", None)

    def call(self, method, params1=None, params2=None, params3=None, params4=None):
        """Call Bitrix24 API method
        :param method: Method name
        :param params1: Method parameters 1
        :param params2: Method parameters 2. Needed for methods with determinate consequence of parameters
        :param params3: Method parameters 3. Needed for methods with determinate consequence of parameters
        :param params4: Method parameters 4. Needed for methods with determinate consequence of parameters
        :return: Call result
        """
        if method == '' or not isinstance(method, str):
            raise Exception('Empty Method')

        if method == 'batch' and 'prepared' not in params1:
            params1['cmd'] = self.prepare_batch(params1['cmd'])
            params1['prepared'] = True

        encoded_parameters = ''

        # print params1
        for i in [params1, params2, params3, params4, {'auth': self.auth_token}]:
            if i is not None:
                if 'cmd' in i:
                    i = dict(i)
                    encoded_parameters += self.encode_cmd(i['cmd']) + '&' + urlencode({'halt': i['halt']}) + '&'
                else:
                    encoded_parameters += urlencode(i) + '&'

        r = {}

        try:
            # request url
            url = self.api_url % (self.domain, method)

            # print("url = ", url)
            print("encoded_parameters = ", encoded_parameters)
            # Make API request
            r = post(url, data=encoded_parameters, timeout=self.timeout)
            # Decode response
            result = loads(r.text)
        except ValueError:
            result = dict(error='Error on decode api response [%s]' % r.text)
        except exceptions.ReadTimeout:
            result = dict(error='Timeout waiting expired [%s sec]' % str(self.timeout))
        except exceptions.ConnectionError:
            result = dict(error='Max retries exceeded [' + str(adapters.DEFAULT_RETRIES) + ']')

        if 'error' in result and result['error'] in ('NO_AUTH_FOUND', 'expired_token'):
            result = self.refresh_tokens()
            if result is not True:
                return result
            # Repeat API request after renew token
            result = self.call(method, params1, params2, params3, params4)
        elif 'error' in result and result['error'] in ['QUERY_LIMIT_EXCEEDED', ]:
            # Suspend call on two second, wait for expired limitation time by Bitrix24 API
            print('SLEEP = ', result)
            sleep(2)
            return self.call(method, params1, params2, params3, params4)

        return result

    def refresh_tokens(self):
        """Refresh access tokens
        :return:
        """
        r = {}
        try:
            # Make call to oauth server
            r = post(
                self.oauth_url,
                params={'grant_type': 'refresh_token', 'client_id': self.client_id, 'client_secret': self.client_secret,
                        'refresh_token': self.refresh_token})
            result = loads(r.text)

            # Renew access tokens
            self.auth_token = result['access_token']
            self.refresh_token = result['refresh_token']
            self.expires_in = result['expires_in']
            service.update_tokens_in_file(self.auth_token, self.expires_in, self.refresh_token)

            info(['Tokens', self.auth_token, self.refresh_token])
            return True
        except (ValueError, KeyError):
            result = dict(error='Error on decode oauth response [%s]' % r.text)
            return result

    @staticmethod
    def prepare_batch(params):
        """
        Prepare methods for batch call
        :param params: dict
        :return: dict
        """
        if not isinstance(params, dict):
            raise Exception('Invalid \'cmd\' structure')

        batched_params = dict()

        # for call_id in sorted(params.keys()):
        for call_id in params.keys():
            if not isinstance(params[call_id], list):
                raise Exception('Invalid \'cmd\' method description')

            method = params[call_id].pop(0)
            if method == 'batch':
                raise Exception('Batch call cannot contain batch methods')

            temp = ''
            for i in params[call_id]:
                temp += urlencode(i) + '&'

            if temp:
                batched_params[call_id] = method + '?' + temp
            else:
                batched_params[call_id] = method

        return batched_params

    @staticmethod
    def encode_cmd(cmd):
        """Resort batch cmd by request keys and encode it
        :param cmd: dict List methods for batch request with request ids
        :return: str
        """
        cmd_encoded = ''

        for i in sorted(cmd.keys()):
            cmd_encoded += urlencode({'cmd': {i: cmd[i]}}) + '&'

        return cmd_encoded

    def batch(self, params):
        """Batch calling without limits. Method automatically prepare method for batch calling
        :param params:
        :return:
        """
        if 'halt' not in params or 'cmd' not in params:
            return dict(error='Invalid batch structure')

        result = dict()

        result['result'] = dict(
            result_error={},
            result_total={},
            result={},
            result_next={},
        )
        count = 0
        batch = dict()
        # for request_id in sorted(params['cmd'].keys()):
        for request_id in params['cmd'].keys():
            batch[request_id] = params['cmd'][request_id]
            count += 1
            if len(batch) == 49 or count == len(params['cmd']):
                temp = self.call('batch', {'halt': params['halt'], 'cmd': batch})
                for i in temp['result']:
                    if i in result['result'] and len(temp['result'][i]) > 0:
                        result['result'][i] = self.merge_two_dicts(temp['result'][i], result['result'][i])
                batch = dict()

        return result

    @staticmethod
    def merge_two_dicts(x, y):
        """Given two dicts, merge them into a new dict as a shallow copy."""
        z = x.copy()
        z.update(y)
        return z

    def call_2(self, params):
        try:
            url = self.api_url % (self.domain, "batch")
            url += '?auth=' + self.auth_token
            headers = {
                'Content-Type': 'application/json',
            }
            r = post(url, data=json.dumps(params), headers=headers, timeout=self.timeout)
            result = loads(r.text)
        except ValueError:
            result = dict(error='Error on decode api response [%s]' % r.text)
        except exceptions.ReadTimeout:
            result = dict(error='Timeout waiting expired [%s sec]' % str(self.timeout))
        except exceptions.ConnectionError:
            result = dict(error='Max retries exceeded [' + str(adapters.DEFAULT_RETRIES) + ']')

        if 'error' in result and result['error'] in ('NO_AUTH_FOUND', 'expired_token'):
            result = self.refresh_tokens()
            if result is not True:
                return result
            result = self.call_2(params)
        elif 'error' in result and result['error'] in ['QUERY_LIMIT_EXCEEDED', ]:
            print('SLEEP = ', result)
            sleep(2)
            return self.call_2(params)

        return result

    def batch_2(self, params):
        if 'halt' not in params or 'cmd' not in params:
            return dict(error='Invalid batch structure')

        return self.call_2(params)

    def call_3(self, method, params):
        try:
            url = self.api_url % (self.domain, method)
            url += '?auth=' + self.auth_token
            headers = {
                'Content-Type': 'application/json',
            }
            r = post(url, data=json.dumps(params), headers=headers, timeout=self.timeout)
            result = json.loads(r.text)
        except ValueError:
            pass
            result = dict(error='Error on decode api response [%s]' % r.text)
        except exceptions.ReadTimeout:
            result = dict(error='Timeout waiting expired [%s sec]' % str(self.timeout))
        except exceptions.ConnectionError:
            result = dict(error='Max retries exceeded [' + str(adapters.DEFAULT_RETRIES) + ']')

        if 'error' in result and result['error'] in ('NO_AUTH_FOUND', 'expired_token'):
            result_update_token = self.refresh_tokens()
            if result_update_token is not True:
                return result
            result = self.call_3(method, params)
        elif 'error' in result and result['error'] in ['QUERY_LIMIT_EXCEEDED', ]:
            time.sleep(2)
            return self.call_3(method, params)

        return result
