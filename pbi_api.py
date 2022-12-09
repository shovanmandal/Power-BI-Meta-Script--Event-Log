import helpers

import msal
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from datetime import datetime, timedelta, date, time
from typing import List, Dict
from urllib.parse import urljoin
import logging
from time import sleep
from tqdm import tqdm

AUTH_BASE_URL = f'https://login.microsoftonline.com/'
PBI_SCOPE = ['https://analysis.windows.net/powerbi/api/.default']
GRAPH_SCOPE = ['https://graph.microsoft.com/.default']
BASE_URL = 'https://api.powerbi.com'
TIMEOUT = 30
SLEEP_TIME = 1
CONCUR_REQ = 5

log = logging.getLogger(__name__)


class PBIApi:
    def __init__(self, tenant_id, client_id, client_secret):
        authority = AUTH_BASE_URL + tenant_id
        self.msal_app = msal.ConfidentialClientApplication(client_id,
                                                           authority=authority,
                                                           client_credential=client_secret,)
        # Set up requests session and custom retry handler - this will allow sane retries
        self.session = Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=3,  # 1.5, 3.0, 6.0, 12.0 seconds
            status_forcelist=[429, 500, 502, 503, 504]
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry_strategy))

    def get_full_monty(self) -> Dict[str, List]:
        data = self.get_modified_ws_data()
        data['users'] = self.get_all_ws_users()
        data['capacities'] = self.get_all_capacities()
        data['userDetails'] = self.get_ad_user_details()
        data = data | self.get_o365_groups()
        return data

    def get_modified_ws_data(self, modified_date_utc: datetime = None) -> (Dict[str, List], None):
        ws_list = self.get_modified_workspaces(modified_date_utc)
        if len(ws_list) == 0:
            log.info('No workspaces returned')
            return None

        # Group workspaces into batches of 100
        group_amt = 100
        ws_groups = [ws_list[i:i + group_amt] for i in range(0, len(ws_list), group_amt)]
        print(f'Received list of {len(ws_list)} workspaces, breaking into {len(ws_groups)} batches')

        all_ws = {'datasourceInstances': []}
        in_prog = []
        pbar = tqdm(desc='Fetching Workspaces Batches', total=len(ws_groups))
        while ws_groups:
            while len(in_prog) < CONCUR_REQ and ws_groups:
                grp = ws_groups.pop()
                in_prog.append((self.req_ws_info(grp), grp))  # Store scan_id and grp as tuple
            scan_id, grp = in_prog.pop()
            data = self.wait_ws_info(scan_id)
            if 'datasourceInstances' in data:  # There can be batches w/ no datasources (all myws)
                all_ws['datasourceInstances'].extend(data['datasourceInstances'])  # Add datasourceInstances data
            helpers.normalize_json_dicts(data['workspaces'], all_ws, 'groups')  # Add workspace data
            pbar.update(1)
        pbar.close()
        print('Data fetch complete!')
        return all_ws

    def get_modified_workspaces(self, modified_date_utc: datetime = None) -> List[str]:
        url = '/v1.0/myorg/admin/workspaces/modified'
        if modified_date_utc:
            url = f'{url}?modifiedSince={modified_date_utc.isoformat()}0Z'
        data = self._get_url(url, PBI_SCOPE)
        log.debug(f'Returned from API {len(data)} workspaces')
        ws_list = [i['id'] for i in data]

        # I have no idea why this is required, but it appears when you include a date, there can be dups
        prior_num = len(ws_list)
        ws_list = list(dict.fromkeys(ws_list))  # Remove any dups
        if prior_num != len(ws_list):
            log.info(f'Removed {prior_num-len(ws_list)} duplicate workspaces')

        log.info(f'Returning {len(ws_list)} unique workspaces')
        return ws_list

    def req_ws_info(self, workspaces: List[str]) -> str:
        token = self._get_or_refresh_token(PBI_SCOPE)
        if len(workspaces) > 100:
            raise ValueError('Too many workspaces requested')
        resp = self.session.post(
            urljoin(BASE_URL, '/v1.0/myorg/admin/workspaces/getInfo?lineage=True&datasourceDetails=True'),
            data={"workspaces": workspaces},
            headers={'Authorization': 'Bearer ' + token},
            timeout=TIMEOUT
        )
        resp.raise_for_status()
        return resp.json()['id']

    def wait_ws_info(self, scan_id: str) -> Dict[str, List]:
        req_status = None
        while True:
            req_status = self._get_scan_status(scan_id)
            log.debug(f'Scan ID {scan_id}: {req_status}')
            if req_status == 'Succeeded':
                break
            else:
                log.debug(f'Sleep needed, sleeping for {SLEEP_TIME} seconds')
                sleep(SLEEP_TIME)
        return self._get_scan_result(scan_id)

    # This is the ideal way to go, however the API limits this to 200 ws requests / hour
    # Created the groups method to circumvent this
    def get_many_ws_users(self, ws_list: List[str]) -> List[Dict[str, str]]:
        print(f'Looking up {len(ws_list)} workspaces for users')
        all_users = []
        for num, ws in enumerate(ws_list, start=1):
            users = self.get_ws_users(ws)
            if users:
                for u in users:
                    u['groupId'] = ws
                    all_users.append(u)
            if num % 10 == 0:
                print(f'Finished {num}/{len(ws_list)} workspaces')
        return all_users

    # This is the ideal way to go, however the API limits this to 200 ws requests / hour
    # Created the groups method to circumvent this
    def get_ws_users(self, ws_id: str) -> (List[Dict], None):
        url = f'/v1.0/myorg/admin/groups/{ws_id}/users'
        data = self._get_url(url, PBI_SCOPE)
        if 'value' not in data:
            raise ValueError(f'No data returned for {url}')
        elif len(data['value']) == 0:
            return None
        return data['value']

    def get_all_ws_users(self) -> List[Dict]:
        log.info(f'Getting all users (via groups)')
        print(f'Getting all users (via groups)')
        all_grps = self.get_groups(['users'])
        all_users = []
        for g in all_grps:
            for user in g['users']:
                user['groupId'] = g['id']
            all_users.extend(g['users'])
        return all_users

    # Expand can be any of 'users, reports, dashboards, datasets, dataflows, workbooks'
    def get_groups(self, expand: List[str] = None) -> List[Dict]:
        first_url = f'/v1.0/myorg/admin/groups?$top=5000'
        if expand:
            all_options = ['users', 'reports', 'dashboards', 'datasets', 'dataflows', 'workbooks']
            if len(set(expand).difference(all_options)) > 0:
                raise ValueError(f'Expand options must be in: {all_options}')
            first_url = f'{first_url}&$expand={",".join(expand)}'
        data = []
        prior_query_count = 0
        total_records = 99999
        while len(data) < total_records:
            if prior_query_count > 0:
                url = f'{first_url}&$skip={5000*prior_query_count}'
            else:
                url = first_url
            batch_data = self._get_url(url, PBI_SCOPE)
            log.info(f'Getting Groups {5000*prior_query_count+1} to {5000*(prior_query_count+1)}')
            print(f'Getting Groups {5000*prior_query_count+1} to {5000*(prior_query_count+1)}')
            total_records = batch_data['@odata.count']
            data.extend(batch_data['value'])
            prior_query_count += 1
        return data

    def get_all_capacities(self) -> List[Dict]:
        print('Getting all capacities')
        url = '/v1.0/myorg/admin/capacities'
        data = self._get_url(url, PBI_SCOPE)
        if 'value' not in data:
            raise ValueError(f'No data returned for {url}')
        return data['value']

    def get_ad_user_details(self) -> List[Dict]:
        print('Getting all AD User Details')
        num_users = self._get_url('https://graph.microsoft.com/v1.0/users/$count', GRAPH_SCOPE, extra_headers={'consistencylevel': 'eventual'})
        url = 'https://graph.microsoft.com/v1.0/users?$expand=manager&$select=id,displayName,givenName,surname,userPrincipalName,mail,userType,jobTitle,department'
        users = []
        pbar = tqdm(desc='Fetching AD Users', total=num_users)
        while url:
            data = self._get_url(url, GRAPH_SCOPE)
            users.extend(data['value'])
            url = data.get('@odata.nextLink', None)
            pbar.update(len(data['value']))
        pbar.close()

        for user in users:
            if 'manager' in user:
                user['managerId'] = user['manager']['id']
                del user['manager']
        print(f'Received {len(users)} total AD users')
        return users

    def get_o365_groups(self) -> Dict[str, List[Dict]]:
        print('Getting O365 Groups')
        groups_url = "https://graph.microsoft.com/v1.0/groups?$filter=groupTypes/any(c:c+eq+'Unified')"
        groups = []
        while groups_url:
            resp = self._get_url(groups_url, GRAPH_SCOPE)
            groups.extend(resp['value'])
            groups_url = resp.get('@odata.nextLink', None)
        print(f'Returned {len(groups)} O365 Groups')

        members = []
        print('Getting Owners and Members for all O365 Groups')
        for group in tqdm(groups, desc='Fetching O365 Group Members'):
            keys = list(group.keys())
            for key in keys:  # Remove all empty items in resp - is a lot
                if group[key] is None or isinstance(group[key], list):
                    del group[key]
            members.extend(self.get_o365_group_members(group['id']))
        return {'o365Groups': groups, 'o365GroupMembers': members}

    def get_o365_group_members(self, group_id) -> List[Dict]:
        url_base = f'https://graph.microsoft.com/v1.0/groups/{group_id}'
        # Owners get processed first
        resp = self._get_url(f'{url_base}/owners?$select=id,name,mail', GRAPH_SCOPE)
        if len(resp.keys()) > 2:
            raise ValueError(f'Too many members, need to iterate here: {resp.keys()}')
        members = resp['value']
        owner_ids = [i['id'] for i in members]
        for memb in members:
            memb['groupUserAccessRight'] = 'Owner'
            memb['groupId'] = group_id
            del memb['@odata.type']
        # Process members
        memb_url = f'{url_base}/members?$select=id,name,mail'
        while memb_url:
            resp = self._get_url(memb_url, GRAPH_SCOPE)
            memb_url = resp.get('@odata.nextLink', None)
            for memb in resp['value']:
                if memb['id'] not in owner_ids:
                    memb['groupId'] = group_id
                    memb['groupUserAccessRight'] = 'Member'
                    members.append(memb)
                    del memb['@odata.type']
        return members

    def get_max_pbi_logs(self) -> List[Dict[str, Dict]]:
        num_days = 30
        return self.get_powerbi_multiday(date.today()+timedelta(days=-num_days), num_days)

    # This will export the MAX that the API will provide up until the day prior 23:59:59.999 (all yesterday)
    def get_powerbi_multiday(self, start_date: date, days: int) -> List[Dict[str, Dict]]:
        all_logs = []
        for add_days in range(days):
            all_logs.extend(self.get_powerbi_logs_day(start_date + timedelta(days=add_days)))
        return all_logs

    # helper func to pull logs for a full day
    def get_powerbi_logs_day(self, day: date) -> List[Dict[str, Dict]]:
        return self.get_powerbi_logs(datetime.combine(day, time.min), datetime.combine(day, time.max))

    # this can only pull the max of one day - API limitation
    def get_powerbi_logs(self, start_utc: datetime, end_utc: datetime) -> List[Dict[str, Dict]]:
        print(f'Getting Power BI Logs for {start_utc} to {end_utc}')
        # if end_utc < datetime.utcnow() - timedelta(days=30):
        #     return []
        url = '/v1.0/myorg/admin/activityevents'
        url = f"{url}?startDateTime='{helpers.format_req_date(start_utc)}'&endDateTime='{helpers.format_req_date(end_utc)}'"
        activity = []
        req_count = 0
        while url is not None:
            req_count += 1
            data = self._get_url(url, PBI_SCOPE)
            log.debug(f'Request #{req_count} on {start_utc} to {end_utc} returned {len(data["activityEventEntities"])}')
            activity.extend(data['activityEventEntities'])
            url = data.get('continuationUri', None)
        print(f'Received log data, got {len(activity)} records in {req_count} requests')
        return activity

    def create_ws_usage_report(self, log_data: List[Dict[str, Dict]], ws_filter: List[str]) -> List[Dict[str, Dict]]:
        report = []
        cols = (
            'WorkspaceId',
            'WorkSpaceName',
            'UserKey',
            'UserId',
            'ReportType',
            'ReportName',
            'ReportId',
            'Operation',
            'ObjectId',
            'DistributionMethod',
            'DashboardName',
            'DashboardId',
            'CreationTime',
            'ConsumptionMethod',
            'AppReportId',
            'AppName',
            'ActivityId',
            'Activity'
        )
        for record in log_data:
            if record.get('WorkspaceId', 'xxx') in ws_filter:
                report.append({k: record[k] for k in cols if k in record})
        return report

    def _get_or_refresh_token(self, scopes: List[str]) -> str:
        result = None
        # Attempt getting token from cache
        result = self.msal_app.acquire_token_silent(scopes=scopes, account=None)

        # No token available - get from AAD
        if not result:
            log.debug('Getting new token from AAD')
            result = self.msal_app.acquire_token_for_client(scopes=scopes)
            print('Successfully received MS access token from AAD')
            log.info('Successfully received MS access token from AAD')

        if "access_token" not in result:
            raise RuntimeError(f'Error: {result.get("error")}; Error_Desc: {result.get("error_description")}')

        return result['access_token']

    def _get_url(self, url: str, scopes: List[str], extra_headers: Dict = None):
        token = self._get_or_refresh_token(scopes)
        full_url = urljoin(BASE_URL, url)
        headers = {'Authorization': 'Bearer ' + token}
        if extra_headers:
            headers.update(extra_headers)
        log.debug(f'Getting this URL: {full_url}')
        try:
            resp = self.session.get(full_url, headers=headers, timeout=TIMEOUT)
        except Exception as e:  # TODO: Needs to be fixed to properly handle retry error failures
            log.info(f'Max retries for {full_url}')
            raise e
        # log.debug(f'GET Response: {resp.text}')
        resp.raise_for_status()
        return resp.json()

    def _get_scan_status(self, scan_id: str) -> str:
        status = self._get_url(f'/v1.0/myorg/admin/workspaces/scanStatus/{scan_id}', PBI_SCOPE)['status']
        if status not in ('NotStarted', 'Running', 'Succeeded'):
            raise ValueError(f'Not expecting this status when checking WS: {status}')
        return status

    def _get_scan_result(self, scan_id: str) -> Dict[str, List]:
        return self._get_url(f'/v1.0/myorg/admin/workspaces/scanResult/{scan_id}', PBI_SCOPE)
