import time
from pprint import pprint
from typing import Optional

import requests
from ditk import logging

from scrappers.utils import get_requests_session


def get_profile(pid: int, session: Optional[requests.Session] = None):
    session = session or get_requests_session()
    while True:
        logging.info(f'Get profile of {pid!r} ...')
        resp = session.get(f'https://api.personality-database.com/api/v1/profile/{pid}')
        if resp.status_code == 403:
            logging.warning('Get 403, wait for some time.')
            time.sleep(0.5)
            continue
        else:
            resp.raise_for_status()
            break

    return resp.json()


def get_comments(pid: int, sort: str = 'HOT', offset: int = 0, range: str = 'all',
                 limit: int = 100, version: str = 'W3', session: Optional[requests.Session] = None):
    session = session or get_requests_session()
    while True:
        logging.info(f'Get profile comments of {pid!r} ...')
        resp = session.get(
            f'https://api.personality-database.com/api/v1/comments/{pid}',
            params={
                'sort': sort,
                'offset': str(offset),
                'range': range,
                'limit': str(limit),
                'version': version,
            }
        )
        if resp.status_code == 403:
            logging.warning('Get 403, wait for some time.')
            time.sleep(0.5)
            continue
        else:
            resp.raise_for_status()
            break

    return resp.json()['comments']


if __name__ == '__main__':
    logging.try_init_root(level=logging.INFO)
    pprint(get_profile(pid=35282))
    pprint(get_comments(pid=35282))
