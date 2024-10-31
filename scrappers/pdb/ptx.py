from typing import Optional

import requests
from ditk import logging

from scrappers.utils import get_requests_session

_TYPEID_MAP = {
    1: 'ISTJ',
    2: 'ESTJ',
    3: 'ISFJ',
    4: 'ESFJ',
    5: 'ESFP',
    6: 'ISFP',
    7: 'ESTP',
    8: 'ISTP',
    9: 'INFJ',
    10: 'ENFJ',
    11: 'INFP',
    12: 'ENFP',
    13: 'INTP',
    14: 'ENTP',
    15: 'INTJ',
    16: 'ENTJ'
}

_DEST_MAP = {
    'actors': 'actors-list',
    'anime': 'anime-characters',
    'books': 'books-list',
    'celebrity': 'famous-people',
    'kpop': 'kpop-idols',
    'songs': 'songs-list'
}


def iter_ptx_from_cursor(typeid: int = 1, dest: str = 'anime', limit: int = 100,
                         init_cursor: str = '', max_no_new_page: int = 10, session: Optional[requests.Session] = None):
    session = session or get_requests_session()
    destination = f'{_TYPEID_MAP[typeid].lower()}-{_DEST_MAP.get(dest, dest)}'
    cursor = init_cursor
    exist_ids = set()
    no_new_count = 0

    while True:
        logging.info(
            f'Query typeid: {typeid!r}, destination: {destination!r}, cursor: {cursor!r}, limit: {limit!r} ...')
        resp = session.get(
            f'https://api.personality-database.com/api/v2/types/{typeid}/profiles',
            params={
                'destination': destination,
                'limit': str(limit),
                'nextCursor': cursor,
            }
        )
        resp.raise_for_status()

        data = resp.json()['data']
        has_new_item = False
        for item in data['results']:
            if item['id'] not in exist_ids:
                yield item
                has_new_item = True
                exist_ids.add(item['id'])

        if not has_new_item:
            no_new_count += 1
            logging.info(f'No new items found in this page ({no_new_count}/{max_no_new_page}) ...')
        else:
            no_new_count = 0
        if no_new_count >= max_no_new_page:
            logging.info('No new items, quit.')
            break
        cursor = data['cursor']['nextCursor']
        if not cursor:
            break


if __name__ == '__main__':
    logging.try_init_root(level=logging.INFO)
    exist_ids = set()
    for item in iter_ptx_from_cursor(typeid=16, dest='celebrity'):
        exist_ids.add(item['id'])
        print(item)
        print(len(exist_ids))
