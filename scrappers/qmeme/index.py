import json
import math
import os
import time
from typing import Optional
from urllib.parse import urljoin

import pandas as pd
import requests
from ditk import logging
from hbutils.string import plural_word
from hbutils.system import urlsplit, TemporaryDirectory
from hfutils.cache import delete_detached_cache
from hfutils.operate import get_hf_client, get_hf_fs, upload_directory_as_directory
from hfutils.utils import number_to_tag
from pyquery import PyQuery as pq
from pyrate_limiter import Limiter, Rate, Duration
from tqdm import tqdm

from scrappers.utils import get_requests_session

_ROOT = 'http://www.quickmeme.com'


def get_meme_list(session: Optional[requests.Session] = None):
    session = session or get_requests_session()
    resp = session.get(urljoin(_ROOT, '/memes/Lonely-Computer-Guy/page/1/'))
    resp.raise_for_status()

    page = pq(resp.text)
    for item in page('#memelist li a').items():
        url = urljoin(resp.url, item.attr('href'))
        title = item.text().strip()
        yield title, url


def get_meme_from_page(base_url: str, last_id: Optional[int] = None,
                       page_no: int = 1, session: Optional[requests.Session] = None):
    session = session or get_requests_session()
    base_url = base_url.rstrip('/')
    url = f'{base_url}/page/{page_no}'
    logging.info(f'Accessing page: {url!r} ...')
    resp = session.get(url, params={'inf': '1'} if not last_id else {'inf': '1', 'i': str(last_id)})
    resp.raise_for_status()

    page = pq(resp.text)
    retval = []
    for item in page('#posts .post').items():
        segs = item.attr('id').split('-', maxsplit=1)
        if len(segs) < 2:
            continue
        prefix, id_text = segs
        assert prefix == 'post', f'Invalid post id - {item.attr("id")!r}'

        id = int(id_text)
        title = item('.post-title').text().strip()
        url = urljoin(resp.url, item('.post-title a').attr('href'))
        assert urlsplit(url).path_segments[0] == '', f'Invalid item url: {url!r}'
        type_ = urlsplit(url).path_segments[1]
        idx = urlsplit(url).path_segments[2]
        image_url = urljoin(resp.url, item('.img-holder img').attr('src'))
        image_alt = item('.img-holder img').attr('alt')
        share_count = int(item('.sharecounts strong').text().replace(',', ''))
        retval.append({
            'id': id,
            'type': type_,
            'idx': idx,
            'title': title,
            'page_url': url,
            'image_url': image_url,
            'image_alt': image_alt,
            'share_count': share_count,
        })
    return retval


def find_max_page_count(base_url: str, session: Optional[requests.Session] = None):
    session = session or get_requests_session()
    l, r = 1, 2
    while get_meme_from_page(base_url, page_no=r, session=session):
        l <<= 1
        r <<= 1

    while l < r:
        m = (l + r + 1) // 2
        if get_meme_from_page(base_url, page_no=m, session=session):
            l = m
        else:
            r = m - 1

    return l


def list_all_from_page(base_url: str, session: Optional[requests.Session] = None):
    logging.info(f'Processing {base_url!r} ...')
    session = session or get_requests_session()
    max_page_count = find_max_page_count(base_url, session=session)
    logging.info(f'Max page of {base_url!r} is {max_page_count!r} ...')

    last_id = None
    for i in tqdm(range(1, max_page_count + 1), desc=f'Accessing {base_url!r} ...'):
        for item in get_meme_from_page(base_url, last_id, i, session=session):
            yield item
            last_id = item['id']


def sync(repository: str, max_time_limit: float = 50 * 60, upload_time_span: float = 30,
         deploy_span: float = 5 * 60, proxy_pool: Optional[str] = None):
    start_time = time.time()
    delete_detached_cache()
    hf_upload_rate = Rate(1, int(math.ceil(Duration.SECOND * upload_time_span)))
    hf_upload_limiter = Limiter(hf_upload_rate, max_delay=1 << 32)

    hf_client = get_hf_client()
    hf_fs = get_hf_fs()

    if not hf_client.repo_exists(repo_id=repository, repo_type='dataset'):
        hf_client.create_repo(repo_id=repository, repo_type='dataset', private=True)
        attr_lines = hf_fs.read_text(f'datasets/{repository}/.gitattributes').splitlines(keepends=False)
        attr_lines.append('*.json filter=lfs diff=lfs merge=lfs -text')
        attr_lines.append('*.csv filter=lfs diff=lfs merge=lfs -text')
        hf_fs.write_text(
            f'datasets/{repository}/.gitattributes',
            os.linesep.join(attr_lines),
        )

    if hf_client.file_exists(
            repo_id=repository,
            repo_type='dataset',
            filename='table.parquet',
    ):
        df = pd.read_parquet(hf_client.hf_hub_download(
            repo_id=repository,
            repo_type='dataset',
            filename='table.parquet',
        ))
        records = df.to_dict('records')
        exist_ids = set(df['id'])
    else:
        records = []
        exist_ids = set()

    if hf_client.file_exists(
            repo_id=repository,
            repo_type='dataset',
            filename='meta.json',
    ):
        meta_info = json.loads(hf_fs.read_text(f'datasets/{repository}/meta.json'))
        exist_groups = set(meta_info['exist_groups'])
    else:
        exist_groups = set()

    _last_update, has_update = None, False
    _total_count = len(records)

    def _deploy(force=False):
        nonlocal _last_update, has_update, _total_count

        if not has_update:
            return
        if not force and _last_update is not None and _last_update + deploy_span > time.time():
            return

        with TemporaryDirectory() as td:
            parquet_file = os.path.join(td, 'table.parquet')
            df_records = pd.DataFrame(records)
            df_records = df_records.sort_values(by=['id'], ascending=[False])
            df_records.to_parquet(parquet_file, engine='pyarrow', index=False)

            with open(os.path.join(td, 'meta.json'), 'w') as f:
                json.dump({
                    'exist_groups': sorted(exist_groups),
                }, f)

            with open(os.path.join(td, 'README.md'), 'w') as f:
                print('---', file=f)
                print('license: other', file=f)
                print('language:', file=f)
                print('- en', file=f)
                print('tags:', file=f)
                print('- meme', file=f)
                print('size_categories:', file=f)
                print(f'- {number_to_tag(len(df_records))}', file=f)
                print('annotations_creators:', file=f)
                print('- no-annotation', file=f)
                print('source_datasets:', file=f)
                print('- quickmeme.com', file=f)
                print('---', file=f)
                print('', file=f)

                print('## Records', file=f)
                print(f'', file=f)
                df_records_shown = df_records[:50]
                print(f'{plural_word(len(df_records), "record")} in total. '
                      f'Only {plural_word(len(df_records_shown), "record")} shown.', file=f)
                print(f'', file=f)
                print(df_records_shown.to_markdown(index=False), file=f)
                print(f'', file=f)

            hf_upload_limiter.try_acquire('hf upload limit')
            upload_directory_as_directory(
                repo_id=repository,
                repo_type='dataset',
                local_directory=td,
                path_in_repo='.',
                message=f'Add {plural_word(len(df_records) - _total_count, "new record")} into index',
            )
            has_update = False
            _last_update = time.time()
            _total_count = len(df_records)

    session = get_requests_session(timeout=15)
    if proxy_pool:
        logging.info(f'Proxy pool {proxy_pool!r} enabled.')
        session.proxies.update({
            'http': proxy_pool,
            'https': proxy_pool
        })

    for gtitle, base_url in tqdm(list(get_meme_list(session=session)), desc='List Pages'):
        if start_time + max_time_limit < time.time():
            break
        if gtitle in exist_groups:
            logging.info(f'Group {gtitle!r} already exist, skipped.')
            continue

        for item in list_all_from_page(base_url=base_url, session=session):
            if item['id'] in exist_ids:
                logging.warning(f'Item {item["id"]!r} already exist, skipped.')
                continue

            logging.info(f'Item {item["id"]!r} confirmed.')
            records.append({
                **item,
                'group_title': gtitle,
                'group_url': base_url,
            })
            exist_ids.add(item['id'])
            has_update = True
            _deploy(force=False)

        exist_groups.add(gtitle)
        has_update = True
        _deploy(force=False)

    _deploy(force=True)


if __name__ == '__main__':
    logging.try_init_root(level=logging.INFO)
    sync(
        repository='datacollection/quickmeme_index',
        max_time_limit=25 * 60 * 60,
        proxy_pool=os.environ['PP_SITE'],
    )
