from pprint import pprint
from typing import Optional

import requests
from ditk import logging

from scrappers.utils import get_requests_session


def iter_pmx_from_cursor(pid: int = 1, category: int = 0, limit: int = 100,
                         init_cursor: str = '0', session: Optional[requests.Session] = None):
    session = session or get_requests_session()
    cursor = init_cursor

    while True:
        logging.info(f'Query PID: {pid!r}, category: {category!r}, cursor: {cursor!r}, limit: {limit!r} ...')
        resp = session.get(
            f'https://api.personality-database.com/api/v2/personalities/{pid}/profiles',
            params={
                'category': str(category),
                'nextCursor': cursor,
                'limit': str(limit),
            }
        )
        resp.raise_for_status()

        yield from resp.json()['profiles']
        if not resp.json().get('profiles'):
            break
        cursor = resp.json()['cursor']['nextCursor']


logging.try_init_root(level=logging.INFO)
for item in iter_pmx_from_cursor(init_cursor='2000', pid=10, limit=200):
    pprint(item)
