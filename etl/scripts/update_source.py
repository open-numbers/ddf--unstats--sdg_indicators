# -*- coding: utf-8 -*-

import os.path as osp
import requests as req


API_BASE = "https://unstats.un.org"


def api_path(api_base, *args):
    res = api_base
    for arg in args:
        if arg.startswith('/'):
            res = osp.join(res, arg[1:])
        else:
            res = osp.join(res, arg)
    return res


def get_all_series():
    j = req.get(api_path(API_BASE, "/SDGAPI", "/v1/sdg/Series/List"), params={'allreleases': False}).json()
    return j


def main():
    j = get_all_series()
    csv_api = api_path(API_BASE, "/SDGAPI", "/v1/sdg/Series/DataCSV")
    for v in j:
        code = v['code']

        f = f'../source/{code}.csv'
        csv = req.post(csv_api, data={'seriesCodes': code}, stream=True)

        with open(f'../source/{code}.csv', 'wb') as f:
            for chunk in csv.iter_content(chunk_size=1024):
                f.write(chunk)
                f.flush()


if __name__ == '__main__':
    main()
    print('Done.')
