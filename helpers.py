from typing import List, Dict
from json import dumps
from datetime import timedelta


def get_all_keys(ld: List[Dict]) -> List[str]:
    all_keys = set()
    for d in ld:
        for key in d.keys():
            all_keys.add(key)
    return sorted(list(all_keys))


# Convert nested JSON List(Dict) structure into a single depth object w/ reference to their parentId
# This is a recursive function
def normalize_json_dicts(ld: List[Dict], output_dict: Dict[str, List], parent_name: str):
    for d in ld:
        for key in list(d.keys()):
            if isinstance(d[key], dict):
                d[key] = dumps(d[key])  # Convert any remaining dicts to strings for easier encoding
            if isinstance(d[key], list):
                for d2 in d[key]:
                    d2[f'{parent_name[:-1]}Id'] = d['id']  # Add relationship to parent in child
                    if key == 'dataflows':
                        d2['id'] = d2.pop('objectId')  # Fix inconsistent naming of dataflows ID - come on MS!
                normalize_json_dicts(d[key], output_dict, key)
                del d[key]  # Now that we have made recursive call, we can remove lower levels
        # Add to output dict
        if parent_name in output_dict:
            output_dict[parent_name].append(d)
        else:
            output_dict[parent_name] = [d]


# Return string representation of a datetime object
# if end is True, will remove 0.001 ms so that the date can be exclusive
def format_req_date(datetime_utc, end: bool=None) -> str:
    if end:
        datetime_utc = datetime_utc - timedelta(microseconds=1000)
    return f'{datetime_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]}Z'

'''
Need:
  * datasourceInstances
  * workspaces (groups)
    * dashboards
      * dashboardTiles
    * dataflows
      * datasourceUsages
      * upstreamDataflows
    * datasets
      * datasourceUsages
      * upstreamDataflows
    * reports
'''
