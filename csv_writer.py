import helpers

import csv
from typing import List, Dict
from pathlib import Path
import os.path


def write_all_dicts(out_dir: str, dl: Dict[str, List]) -> None:
    print('Writing all data out to CSV')
    Path(out_dir).mkdir(parents=False, exist_ok=True)
    for key in dl.keys():
        file_name = f'{key}.csv'
        print(f'    {file_name}')
        write_dict_table(filename=os.path.join(out_dir, file_name), table=dl[key])
    print('Writing out data complete')


def write_dict_table(filename: str, table: List[Dict]) -> None:
    keys = helpers.get_all_keys(table)
    with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
        dict_writer = csv.DictWriter(f, fieldnames=keys, dialect='excel')
        dict_writer.writeheader()
        dict_writer.writerows(table)
