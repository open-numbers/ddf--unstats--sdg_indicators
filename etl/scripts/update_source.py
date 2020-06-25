# -*- coding: utf-8 -*-

import glob
import os
import os.path as osp
import requests as req
from multiprocessing import Pool
from ddf_utils.factory.common import download
from functools import partial


API_BASE = "https://unstats.un.org"


def cleanup_source():
    for f in glob.glob("../source/*csv"):
        os.remove(f)

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


def run_download(v):
    code = v['code']
    f = f'../source/{code}.csv'
    csv_api = api_path(API_BASE, "/SDGAPI", "/v1/sdg/Series/DataCSV")
    download(csv_api, f, method='POST', post_data={'seriesCodes': code}, progress_bar=False, resume=False)
    print(f"{f} downloaded")


def main():
    all_ser = get_all_series()

    with Pool(2) as p:
        p.map(run_download, all_ser)


if __name__ == '__main__':
    cleanup_source()
    main()
    print('Done.')
