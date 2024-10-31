import time
from pprint import pprint
from typing import Optional

import requests
from ditk import logging

from scrappers.utils import get_requests_session


def get_profile(pid: int, max_retries: int = 5, session: Optional[requests.Session] = None):
    session = session or get_requests_session()
    tries = 0
    while True:
        logging.info(f'Get profile of {pid!r} ...')
        resp = session.get(f'https://api.personality-database.com/api/v1/profile/{pid}')
        if resp.status_code == 403:
            sleep_time = 2 ** tries
            tries += 1
            if tries <= max_retries:
                logging.warning(f'Get 403 ({tries}/{max_retries}), wait for {sleep_time}s.')
                time.sleep(sleep_time)
                continue
            else:
                logging.error('Out of retries, raised.')
                resp.raise_for_status()
        else:
            resp.raise_for_status()
            break

    return resp.json()


def get_comments(pid: int, sort: str = 'HOT', offset: int = 0, range: str = 'all',
                 limit: int = 100, version: str = 'W3',
                 max_retries: int = 5, session: Optional[requests.Session] = None):
    session = session or get_requests_session()
    tries = 0
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
            sleep_time = 2 ** tries
            tries += 1
            if tries <= max_retries:
                logging.warning(f'Get 403 ({tries}/{max_retries}), wait for {sleep_time}s.')
                time.sleep(sleep_time)
                continue
            else:
                logging.error('Out of retries, raised.')
                resp.raise_for_status()
        else:
            resp.raise_for_status()
            break

    return resp.json()['comments']


if __name__ == '__main__':
    logging.try_init_root(level=logging.INFO)
    pprint(get_profile(pid=35282))
    pprint(get_comments(pid=35282))
