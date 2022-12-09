#!/usr/bin/python3
import pbi_api
import csv_writer
import config

import logging
from datetime import date

if config.LOG_NAME:
    logging.basicConfig(filename=config.LOG_NAME, filemode='w')
else:
    logging.basicConfig()
log = logging.getLogger()
log.setLevel(config.LOG_LEVEL)


def main():
    log.info('Starting Application')
    api = pbi_api.PBIApi(config.TENANT_ID,
                         config.CLIENT_ID,
                         config.CLIENT_SECRET)

    wsid_list = [
        '31d7c6cf-7645-4202-9a73-44092fc67455',
        'da7b55c9-fa52-47c6-b652-bd976477a1df',
        '687550ea-121b-46bb-9eac-879bb8617c2f'
                 ]

    data = {}
    full_logs = api.get_max_pbi_logs()
    data[f'logs-{date.today().isoformat()}'] = full_logs
    data[f'logs-OPM-export-{date.today().isoformat()}'] = api.create_ws_usage_report(full_logs, wsid_list)

    # start = time.time()
    # data = api.get_full_monty()
    # print(f'Took {time.time()-start} seconds to run the full monty')

    # Write out all the data
    if config.CSV_OUT_DIR:
        csv_writer.write_all_dicts(config.CSV_OUT_DIR, data)


if __name__ == '__main__':
    main()
