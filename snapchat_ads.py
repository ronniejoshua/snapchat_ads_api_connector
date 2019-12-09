import requests
from requests_oauthlib import OAuth2Session
import datetime as dt
import functools
import time
import re

INFORMATION = """
'debug_message': 'Unsupported Stats Query: Timeseries queries with DAY granularity cannot query time intervals of 
more than 32 days' 
"""


class SnapchatAPI(object):
    _insert_time = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    redirect_uri = 'my_redirect_uri'
    authorize_url = 'https://accounts.snapchat.com/login/oauth2/authorize'
    scope = ['snapchat-marketing-api']
    access_token_url = 'https://accounts.snapchat.com/login/oauth2/access_token'
    protected_url = 'https://adsapi.snapchat.com/v1/me/organizations'
    BASE_URL = 'https://adsapi.snapchat.com/v1/'
    state = '0123456789876543210'

    def __init__(self, client_id: str, client_secret: str, org_id: str):
        self._client_id = client_id
        self._client_secret = client_secret
        self._org_id = org_id
        self.access_token = None
        self.refresh_token = None
        self.token_type = None

    def authenticate(self):
        oauth = OAuth2Session(client_id=self._client_id, redirect_uri=self.redirect_uri, scope=self.scope,
                              state=self.state)
        authorization_url, state = oauth.authorization_url(self.authorize_url)
        print('Please go to {0} and authorize access.'.format(authorization_url))
        authorization_response = input('Enter the full callback URL: ')
        # Exchange the one time use code for an Access Token & Refresh Token
        requested_tokens = oauth.fetch_token(self.access_token_url,
                                             authorization_response=authorization_response,
                                             client_secret=self._client_secret,
                                             scope=self.scope)

        self.access_token = requested_tokens['access_token']
        self.token_type = requested_tokens['token_type']
        self.refresh_token = requested_tokens['refresh_token']
        expires_in = requested_tokens['expires_in']
        _scope = requested_tokens['scope']
        expires_at = requested_tokens['expires_at']
        return self.access_token, self.token_type, self.refresh_token, expires_in, _scope, expires_at

    def get_access_token(self, refresh_token):
        resp = requests.post(
            self.access_token_url,
            data={
                'client_id': self._client_id,
                'client_secret': self._client_secret,
                'code': refresh_token,
                'grant_type': 'refresh_token'
            }
        )
        return resp.json()['access_token']

    def get_all_account(self, org_id: str, access_token: str) -> dict:
        PATH = 'organizations/{0}/adaccounts'.format(org_id)
        URI = self.BASE_URL + PATH
        headers = {'Authorization': 'Bearer {}'.format(access_token)}
        resp = requests.get(URI, headers=headers)
        all_account_ids = [dd['adaccount']['id'] for dd in resp.json()['adaccounts']]
        return resp.json(), all_account_ids

    def get_account_dictionary(self, account_id: str, access_token: str) -> dict:
        PATH = 'adaccounts/{0}'.format(account_id)
        URI = self.BASE_URL + PATH
        headers = {'Authorization': 'Bearer {}'.format(access_token)}
        resp = requests.get(URI, headers=headers)
        return resp.json()

    def get_campaign_dictionary(self, ad_account_id: str, access_token: str):
        PATH = 'adaccounts/{0}/campaigns'.format(ad_account_id)
        URI = self.BASE_URL + PATH
        headers = {'Authorization': 'Bearer {}'.format(access_token)}
        resp = requests.get(URI, headers=headers)
        all_campaign_ids = [dd['campaign']['id'] for dd in resp.json()['campaigns']]
        datalist = [dd['campaign'] for dd in resp.json()['campaigns']]
        list_datadict = []
        for cmpgn_dd in datalist:
            row = {
                '_insert_time': self._insert_time,
                'campaign_id': cmpgn_dd.get('id'),
                'name': cmpgn_dd.get('name'),
                'ad_account_id': cmpgn_dd.get('ad_account_id'),
                'updated_at': self.parse_date(cmpgn_dd.get('updated_at'), "%Y-%m-%dT%H:%M:%S.%fZ"),
                'created_at': self.parse_date(cmpgn_dd.get('created_at'), "%Y-%m-%dT%H:%M:%S.%fZ"),
                'status': cmpgn_dd.get('status'),
                'daily_budget_micro': cmpgn_dd.get('daily_budget_micro'),
                'objective': cmpgn_dd.get('objective'),
                'ios_app_id': cmpgn_dd.get('measurement_spec').get('ios_app_id'),
                'android_app_url': cmpgn_dd.get('measurement_spec').get('android_app_url'),
                'start_time': self.parse_date(cmpgn_dd.get('start_time'), "%Y-%m-%dT%H:%M:%S.%fZ")
            }
            list_datadict.append(row)

        return list_datadict, all_campaign_ids

    def get_ads_squad_dictionary(self, ad_account_id: str, access_token: str) -> dict:
        PATH = 'adaccounts/{0}/adsquads'.format(ad_account_id)
        URI = self.BASE_URL + PATH
        headers = {'Authorization': 'Bearer {}'.format(access_token)}
        resp = requests.get(URI, headers=headers)
        all_adsquads_ids = [dd['adsquad']['id'] for dd in resp.json()['adsquads']]
        datalist = [dd['adsquad'] for dd in resp.json()['adsquads']]
        list_datadict = []
        for adsquad_dd in datalist:
            row = {
                '_insert_time': self._insert_time,
                'ad_account_id': ad_account_id,
                'campaign_id': adsquad_dd.get('campaign_id'),
                'adsquad_id': adsquad_dd.get('id'),
                'name': adsquad_dd.get('name'),
                'updated_at': self.parse_date(adsquad_dd.get('updated_at'), "%Y-%m-%dT%H:%M:%S.%fZ"),
                'created_at': self.parse_date(adsquad_dd.get('created_at'), "%Y-%m-%dT%H:%M:%S.%fZ"),
                'status': adsquad_dd.get('status'),
                'type': adsquad_dd.get('type'),
                'placement': adsquad_dd.get('placement'),
                'billing_event': adsquad_dd.get('billing_event'),
                'bid_micro': adsquad_dd.get('bid_micro'),
                'auto_bid': adsquad_dd.get('auto_bid'),
                'target_bid': adsquad_dd.get('target_bid'),
                'daily_budget_micro': adsquad_dd.get('daily_budget_micro'),
                'start_time': self.parse_date(adsquad_dd.get('start_time'), "%Y-%m-%dT%H:%M:%S.%fZ"),
                'optimization_goal': adsquad_dd.get('optimization_goal'),
                'delivery_constraint': adsquad_dd.get('delivery_constraint'),
                'pacing_type': adsquad_dd.get('pacing_type')
            }
            list_datadict.append(row)
        return list_datadict, all_adsquads_ids

    def get_ad_dictionary(self, ad_account_id: str, access_token: str) -> dict:
        PATH = 'adaccounts/{0}/ads'.format(ad_account_id)
        URI = self.BASE_URL + PATH
        headers = {'Authorization': 'Bearer {}'.format(access_token)}
        resp = requests.get(URI, headers=headers)
        list_ads_id = [dd['ad']['id'] for dd in resp.json()['ads']]
        datalist = [dd['ad'] for dd in resp.json()['ads']]
        list_datadict = []
        for ad_dd in datalist:
            row = {
                '_insert_time': self._insert_time,
                'ad_account_id': ad_account_id,
                'ad_id': ad_dd.get('id'),
                'name': ad_dd.get('name'),
                'ad_squad_id': ad_dd.get('ad_squad_id'),
                'creative_id': ad_dd.get('creative_id'),
                'updated_at': self.parse_date(ad_dd.get('updated_at'), "%Y-%m-%dT%H:%M:%S.%fZ"),
                'created_at': self.parse_date(ad_dd.get('created_at'), "%Y-%m-%dT%H:%M:%S.%fZ"),
                'status': ad_dd.get('status'),
                'type': ad_dd.get('type'),
                'render_type': ad_dd.get('render_type'),
                'review_status': ad_dd.get('review_status')
            }
            list_datadict.append(row)
        return list_datadict, list_ads_id

    def get_accounts_stats(self, ad_account_id: str, access_token: str, start_datetime: str, end_datetime: str) -> dict:
        print(start_datetime, end_datetime)
        PATH = 'adaccounts/{0}/stats'.format(ad_account_id)
        URI = self.BASE_URL + PATH
        fields = 'spend'
        headers = {'Authorization': 'Bearer {}'.format(access_token)}
        payload = {
            'granularity': 'DAY',
            'start_time': start_datetime,
            'end_time': end_datetime,
            'fields': fields
        }
        resp = requests.get(URI, params=payload, headers=headers)
        return sum([dd['stats']['spend'] / 10 ** 6 for dd in resp.json()['timeseries_stats'][0]['timeseries_stat'][
            'timeseries']])

    def get_non_zero_campaigns(self, access_token: str, start_datetime: str, end_datetime: str, all_campaign_ids:
    list) -> list:
        non_zero_campaign_ids = []
        for campaign_id in all_campaign_ids:
            PATH = 'campaigns/{0}/stats'.format(campaign_id)
            URI = self.BASE_URL + PATH
            fields = 'impressions'
            headers = {'Authorization': 'Bearer {}'.format(access_token)}
            payload = {
                'granularity': 'DAY',
                'start_time': start_datetime,
                'end_time': end_datetime,
                'fields': fields
            }
            resp = requests.get(URI, params=payload, headers=headers)
            non_zero_campaign_ids.append(resp.json()['timeseries_stats'][0]['timeseries_stat']['id']) if sum(
                [dd['stats']['impressions'] / 10 ** 6 for dd in
                 resp.json()['timeseries_stats'][0]['timeseries_stat']['timeseries']]) > 0 else None

        return non_zero_campaign_ids

    def get_non_zero_ad_squads(self, access_token: str, start_datetime: str, end_datetime: str, non_zero_campaigns:
    list) -> dict:
        non_zero_ad_squads_ids = []
        for campaign_id in non_zero_campaigns:
            PATH = 'campaigns/{0}/adsquads'.format(campaign_id)
            URI = self.BASE_URL + PATH
            headers = {'Authorization': 'Bearer {}'.format(access_token)}
            resp = requests.get(URI, headers=headers)
            all_adsquads_ids = [dd['adsquad']['id'] for dd in resp.json()['adsquads']]
            for ad_squad_id in all_adsquads_ids:
                PATH = 'adsquads/{0}/stats'.format(ad_squad_id)
                URI = self.BASE_URL + PATH
                fields = 'impressions'
                headers = {'Authorization': 'Bearer {}'.format(access_token)}
                payload = {
                    'granularity': 'DAY',
                    'start_time': start_datetime,
                    'end_time': end_datetime,
                    'fields': fields
                }
                resp = requests.get(URI, params=payload, headers=headers)
                non_zero_ad_squads_ids.append(resp.json()['timeseries_stats'][0]['timeseries_stat']['id']) if sum(
                    [dd['stats']['impressions'] / 10 ** 6 for dd in
                     resp.json()['timeseries_stats'][0]['timeseries_stat']['timeseries']]) > 0 else None
        return non_zero_ad_squads_ids

    def get_non_zero_ads_ids(self, access_token: str, non_zero_ad_squad_ids: list) -> list:
        all_adsquads_ids = []
        for ad_squad in non_zero_ad_squad_ids:
            PATH = 'adsquads/{0}/ads'.format(ad_squad)
            URI = self.BASE_URL + PATH
            headers = {'Authorization': 'Bearer {}'.format(access_token)}
            resp = requests.get(URI, headers=headers)
            all_adsquads_ids.extend([dd['ad']['id'] for dd in resp.json()['ads']])
        return all_adsquads_ids

    def get_ads_stats(self, ad_account_id: str, access_token: str, start_datetime: str, end_datetime: str,
                      all_ad_ids: list) -> int:
        ads_stats_list = list()
        for ad_id in all_ad_ids:
            PATH = 'ads/{0}/stats'.format(ad_id)
            URI = self.BASE_URL + PATH
            fields = 'android_installs,attachment_avg_view_time_millis,attachment_impressions,attachment_quartile_1,' \
                     'attachment_quartile_2,attachment_quartile_3,attachment_total_view_time_millis,' \
                     'attachment_view_completion,avg_screen_time_millis,avg_view_time_millis,impressions,' \
                     'ios_installs,quartile_1,quartile_2,quartile_3,screen_time_millis,spend,swipe_up_percent,swipes,' \
                     'total_installs,video_views,view_completion,view_time_millis,conversion_purchases,' \
                     'conversion_purchases_value,conversion_save,conversion_start_checkout,conversion_add_cart,' \
                     'conversion_view_content,conversion_add_billing,conversion_searches,' \
                     'conversion_level_completes,conversion_app_opens,conversion_page_views,attachment_frequency,' \
                     'attachment_uniques,frequency,uniques'
            headers = {'Authorization': 'Bearer {}'.format(access_token)}
            payload = {
                'granularity': 'DAY',
                'start_time': start_datetime,
                'end_time': end_datetime,
                'fields': fields
            }
            resp = requests.get(URI, params=payload, headers=headers)
            ads_stats_list.extend([dict(
                {
                    **{'_insert_time': self._insert_time, 'ad_account_id': ad_account_id, 'ad_id': ad_id},
                    'start_time': self.parse_date_regex(dd.get('start_time')),
                    'end_time': self.parse_date_regex(dd.get('end_time')),
                    **dd['stats']
                }
            ) for dd in resp.json()['timeseries_stats'][0]['timeseries_stat']['timeseries']])
        #     total_spend.append(sum([dd['stats']['spend'] / 10 ** 6 for dd in resp.json()['timeseries_stats'][0][
        #         'timeseries_stat']['timeseries']]))
        # return sum(total_spend)
        return ads_stats_list

    # Using Existing Implementation
    def get_ad_squads_stats(self, ad_account_id: str, access_token: str, start_datetime: str, end_datetime: str,
                            all_ad_squads_ids: list):
        ad_squads_stats_list = list()
        for ad_squad_id in all_ad_squads_ids:
            PATH = 'adsquads/{0}/stats'.format(ad_squad_id)
            URI = self.BASE_URL + PATH
            fields = 'android_installs,attachment_avg_view_time_millis,attachment_impressions,attachment_quartile_1,' \
                     'attachment_quartile_2,attachment_quartile_3,attachment_total_view_time_millis,' \
                     'attachment_view_completion,avg_screen_time_millis,avg_view_time_millis,impressions,' \
                     'ios_installs,quartile_1,quartile_2,quartile_3,screen_time_millis,spend,swipe_up_percent,swipes,' \
                     'total_installs,video_views,view_completion,view_time_millis,conversion_purchases,' \
                     'conversion_purchases_value,conversion_save,conversion_start_checkout,conversion_add_cart,' \
                     'conversion_view_content,conversion_add_billing,conversion_searches,' \
                     'conversion_level_completes,conversion_app_opens,conversion_page_views,attachment_frequency,' \
                     'attachment_uniques,frequency,uniques'
            headers = {'Authorization': 'Bearer {}'.format(access_token)}
            payload = {
                'granularity': 'DAY',
                'start_time': start_datetime,
                'end_time': end_datetime,
                'fields': fields
            }
            resp = requests.get(URI, params=payload, headers=headers)
            ad_squads_stats_list.extend([dict(
                {
                    **{'_insert_time': self._insert_time, 'ad_account_id': ad_account_id, 'ad_squad_id': ad_squad_id},
                    'start_time': self.parse_date_regex(dd.get('start_time')),
                    'end_time': self.parse_date_regex(dd.get('end_time')),
                    **dd['stats']
                }
            ) for dd in resp.json()['timeseries_stats'][0]['timeseries_stat']['timeseries']])
        return ad_squads_stats_list

    @staticmethod
    def flatten_json(y):
        out = {}

        def flatten(x, name=''):
            if type(x) is dict:
                for a in x:
                    flatten(x[a], name + a + '_')
            elif type(x) is list:
                i = 0
                for a in x:
                    flatten(a, name + str(i) + '_')
                    i += 1
            else:
                out[name[:-1]] = x

        flatten(y)
        return out

    @staticmethod
    def parse_date(date_string: str, date_format: str) -> str:
        """
        '2019-04-12T00:00:00.000-07:00' --> "%Y-%m-%dT%H:%M:%S.%f%z"
        '2019-04-28T07:25:39.668Z' --> "%Y-%m-%dT%H:%M:%S.%fZ"
        """
        req_date = dt.datetime.strptime(date_string, date_format)
        return req_date.strftime("%Y-%m-%d")

    @staticmethod
    def parse_date_regex(date_string: str) -> str:
        """
        :return:
        """
        date = re.split('(\d{4}-\d{2}-\d{2})', date_string)
        return [res for res in date if len(res) == 10][0]

    @staticmethod
    def create_dates(lb_window=29, days_skip=0):
        today = dt.datetime.utcnow()
        # END DATE IS NOT INCLUSIVE - TAKE IT TILL TODAY
        _end_datetime = today - dt.timedelta(0 + days_skip)
        _start_datetime = _end_datetime - dt.timedelta(lb_window)

        # Clock Forward
        end_datetime = _end_datetime.strftime("%Y-%m-%d") + "T00:00:00.000000-0700"
        start_datetime = _start_datetime.strftime("%Y-%m-%d") + "T00:00:00.000000-0700"

        # Clock Back []
        # end_datetime = _end_datetime.strftime("%Y-%m-%d") + "T00:00:00.000000-0800"
        # start_datetime = _start_datetime.strftime("%Y-%m-%d") + "T00:00:00.000000-0800"

        return {'start_datetime': start_datetime, 'end_datetime': end_datetime}
