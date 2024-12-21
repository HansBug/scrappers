import json
import re
from pprint import pprint
from typing import Optional
from urllib.parse import urljoin

import requests
from ditk import logging
from hbutils.system import urlsplit
from pyquery import PyQuery as pq
from tqdm import tqdm

from scrappers.utils import get_requests_session


def list_template_page(page_id: int, session: Optional[requests.Session] = None):
    logging.info(f'Accessing template list page #{page_id} ...')
    session = session or get_requests_session()
    resp = session.get('https://imgflip.com/memetemplates', params={'page': str(page_id)})
    resp.raise_for_status()

    retval = []
    for item in pq(resp.text)('#mt-boxes-wrap .mt-box').items():
        title = item('h3.mt-title a').text().strip()

        meme_url = urljoin(resp.url, item('h3.mt-title a').attr('href'))
        thumbnail_url = urljoin(resp.url, item('.mt-img-wrap img').attr('src'))
        caption_url = urljoin(resp.url, item('a.mt-caption').attr('href'))
        if urlsplit(caption_url).path_segments[1] == 'gif-maker':
            logging.warning(f'Item {title!r} is a gif-maker, skipped.')
            continue
        assert urlsplit(caption_url).path_segments[1] == 'memegenerator', \
            f'Invalid caption url: {caption_url!r}'
        pid = list(filter(bool, urlsplit(caption_url).path_segments))[-1]
        retval.append({
            'pid': pid,
            'title': title,
            'meme_url': meme_url,
            'thumbnail_url': thumbnail_url,
            'caption_url': caption_url,
        })

    return retval


def iter_all_templates(session: Optional[requests.Session] = None):
    session = session or get_requests_session()
    l, r = 1, 2
    while list_template_page(r, session=session):
        l <<= 1
        r <<= 1

    while l < r:
        m = (l + r + 1) // 2
        if list_template_page(m, session=session):
            l = m
        else:
            r = m - 1

    max_page = l
    logging.info(f'Max page is {l}.')
    for i in tqdm(range(1, max_page + 1), desc='Iter template pages'):
        yield from list_template_page(i, session=session)


def to_base36(num):
    """
    将数字转换为36进制字符串
    """
    chars = '0123456789abcdefghijklmnopqrstuvwxyz'
    result = ''
    while num:
        num, remainder = divmod(num, 36)
        result = chars[remainder] + result
    return result if result else '0'


def _process_item(item):
    item['alt_names'] = item.pop('altNames') or None
    item['default_settings'] = item.get('default_settings') or None
    item['url_name'] = item.pop('url_name') or None
    if item['default_settings']:
        item['default_settings'] = json.loads(item['default_settings'])
    item['height'] = item.pop('h')
    item['width'] = item.pop('w')
    assert item['safe_for_work'] in {'yes', 'no'}
    item['is_sfw'] = item.pop('safe_for_work') == 'yes'
    return item


def get_generator_info(caption_url: str, session: Optional[requests.Session] = None):
    session = session or get_requests_session()
    resp = session.get(caption_url)
    resp.raise_for_status()

    url_name = list(filter(bool, urlsplit(caption_url).path_segments))[-1]

    page = pq(resp.text)
    for script_item in page('script').items():
        if 'memes=' in script_item.text():
            matching = re.findall(r'usermemeID=(?P<id>\d+);', script_item.text())
            assert matching
            id_ = json.loads(matching[0])

            matching = re.findall(r'memes=(?P<json>[^;]+)', script_item.text())
            assert matching
            v = json.loads(matching[0])
            if isinstance(v, dict):
                v = list(v.values())
            for vitem in v:
                vitem = _process_item(vitem)
                if vitem['url_name'] == url_name or vitem['id'] == id_:
                    if vitem['url_name']:
                        vitem['image_url'] = f'https://imgflip.com/s/meme/{vitem["url_name"]}.{vitem["file_type"]}'
                    else:
                        vitem['image_url'] = f'https://i.imgflip.com/{to_base36(id_)}.{vitem["file_type"]}'
                    return vitem

    return None


if __name__ == '__main__':
    logging.try_init_root(level=logging.INFO)
    # pprint(list_template_page(1))
    # pprint(get_generator_info('Rob-In-The-Hood'))
    # pprint(get_generator_info('Horse-Drawing'))
    # pprint(get_generator_info('Drake-Hotline-Bling'))

    pprint(get_generator_info('https://imgflip.com/memegenerator/77045868/Pawn-Stars-Best-I-Can-Do'))
    # pprint(get_generator_info('https://imgflip.com/memegenerator/Ancient-Aliens'))

    # for item in iter_all_templates():
    #     print(item['url_name'], item['image_url'])
    #     pass
