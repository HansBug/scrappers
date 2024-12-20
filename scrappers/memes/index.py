import math
import os
import time
from tempfile import TemporaryDirectory
from typing import Optional

import numpy as np
import pandas as pd
import requests
from ditk import logging
from hbutils.string import plural_word
from hfutils.cache import delete_detached_cache
from hfutils.operate import get_hf_client, get_hf_fs, upload_directory_as_directory
from hfutils.utils import number_to_tag
from pyrate_limiter import Rate, Limiter, Duration

from ..utils import get_requests_session


def _get_index_by_offset(offset: int = 0, session: Optional[requests.Session] = None):
    logging.info(f'Get page offset {offset!r} ...')
    session = session or get_requests_session()
    resp = session.get(
        'https://knowyourmeme.com/memes/all',
        params={
            'sort': 'newest',
            'offset': int(offset),
        },
    )
    resp.raise_for_status()
    for g in resp.json()['groups']:
        yield from g['items']


def _to_list(x):
    if isinstance(x, list):
        return x
    elif isinstance(x, np.ndarray):
        return x.tolist()
    elif isinstance(x, pd.Series):
        return x.tolist()
    else:
        return x


def sync(repository: str, max_time_limit: float = 50 * 60, upload_time_span: float = 30,
         deploy_span: float = 5 * 60, sync_mode: bool = False):
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

    if hf_fs.exists(f'datasets/{repository}/table.parquet'):
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

    if hf_fs.exists(f'datasets/{repository}/tags.parquet'):
        df_tags = pd.read_parquet(hf_client.hf_hub_download(
            repo_id=repository,
            repo_type='dataset',
            filename='tags.parquet',
        ))
        d_tags = {item['data']: item for item in df_tags.to_dict('records')}
    else:
        d_tags = {}

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

            tags_file = os.path.join(td, 'tags.parquet')
            df_tags = pd.DataFrame(list(d_tags.values()))
            df_tags = df_tags.sort_values(['count', 'data'], ascending=[False, True])
            df_tags.to_parquet(tags_file, engine='pyarrow', index=False)

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
                print('- knowyourmeme.com', file=f)
                print('---', file=f)
                print('', file=f)

                print('## Records', file=f)
                print(f'', file=f)
                df_records_shown = df_records[:50][
                    ['id', 'category', 'type', 'comments_count', 'favorites_count', 'image', 'link',
                     'title', 'summary', 'tags', 'created_at', 'updated_at']]
                df_records_shown['tags'] = df_records_shown['tags'].map(_to_list)
                print(f'{plural_word(len(df_records), "record")} in total. '
                      f'Only {plural_word(len(df_records_shown), "record")} shown.', file=f)
                print(f'', file=f)
                print(df_records_shown.to_markdown(index=False), file=f)
                print(f'', file=f)

                print('## Tags', file=f)
                print(f'', file=f)
                print(f'{plural_word(len(df_tags), "tag")} in total.', file=f)
                print(f'', file=f)
                df_tags_shown = df_tags[:50]
                print(df_tags_shown.to_markdown(index=False), file=f)
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

    session = get_requests_session()
    session.headers.update({
        'Cookie': '_ga=GA1.1.1171191360.1729229221; _cb=DagxJTCLoz6tD_aFZv; compass_uid=204d25fb-3668-46f8-9166-a329e71b2977; pushly.user_puuid_TgtvAlfG=Kjx6rmU0byTkBcm4K2hloOeBsEr2Vq6l; _pnlspid_TgtvAlfG=32133; _hjSessionUser_4936301=eyJpZCI6IjE5YTdjNGFkLTAwMjktNWNjYi1iMzQyLWQ4ODMwMDE3NGJhMiIsImNyZWF0ZWQiOjE3MjkyMjkyMjE2NjcsImV4aXN0aW5nIjp0cnVlfQ==; _y=9e187072-B9DB-4331-307A-8EF2CCF7AAD6; _shopify_y=9e187072-B9DB-4331-307A-8EF2CCF7AAD6; _hjSessionUser_1004046=eyJpZCI6IjlhYTU2Nzk3LWIwY2UtNTRmYy04NWUwLTU0OTY0MDZiZTY1OCIsImNyZWF0ZWQiOjE3Mjk1MjY0OTkxMjIsImV4aXN0aW5nIjp0cnVlfQ==; _pnss_TgtvAlfG=blocked; _ga_5FPLDLE8C6=GS1.1.1730216484.7.0.1730216484.0.0.0; split=%7B%22split%3A213803%22%3A%22redesign%22%7D; split_all_domain=%7B%22split%3A213803%22%3A%22redesign%22%7D; ___nrbi=%7B%22firstVisit%22%3A1729229221%2C%22userId%22%3A%22204d25fb-3668-46f8-9166-a329e71b2977%22%2C%22userVars%22%3A%5B%5D%2C%22futurePreviousVisit%22%3A1733141908%2C%22timesVisited%22%3A6%7D; _hjSession_1004046=eyJpZCI6IjhjMzFhYTYyLTAxYjUtNDljZS04MmNiLTIwYThlYzU3YTM2MiIsImMiOjE3MzMxNDE5MTEyNTcsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=; ___nrbic=%7B%22previousVisit%22%3A1730200942%2C%22currentVisitStarted%22%3A1733141908%2C%22sessionId%22%3A%22bb41b6a7-eb03-407d-9ab7-67a65646b42f%22%2C%22sessionVars%22%3A%5B%5D%2C%22visitedInThisSession%22%3Atrue%2C%22pagesViewed%22%3A3%2C%22landingPage%22%3A%22https%3A//knowyourmeme.com/memes/all%22%2C%22referrer%22%3A%22%22%2C%22lpti%22%3Anull%7D; _chartbeat2=.1729229221474.1733141951004.0000000000000001.hNuNCC5CNJIkSeVVCJvfyFBsMuwg.1; _cb_svref=external; _pn_TgtvAlfG=eyJzdWIiOnsidWRyIjowLCJpZCI6IktqeDZybVUwYnlUa0JjbTRLMmhsb09lQnNFcjJWcTZsIiwic3MiOi0xfSwibHVhIjoxNzMzMTQxOTIzODUwfQ; _awl=2.1733141960.5-0ec718646045104bee6f0440222cb7ab-6763652d617369612d6561737431-1; _ga_5FPLDLE8C6=GS1.1.1733141908.8.1.1733141969.0.0.0; _chartbeat5=309|1659|%2Fcategories%2Fmeme|https%3A%2F%2Fknowyourmeme.com%2Fcategories%2Fmeme%2Fpage%2F2%3Fsort%3Dnewest%26status%3Dall|Be3E7zZ24C6CAxpsuBBlDykBFOf2Z||c|Be3E7zZ24C6CAxpsuBBlDykBFOf2Z|knowyourmeme.com|',
        'Referer': 'https://knowyourmeme.com/memes/all',
        'content-type': 'application/json',
    })

    offset = 0
    empty_page_count = 0
    while True:
        if start_time + max_time_limit < time.time():
            break

        page_items = list(_get_index_by_offset(offset=offset, session=session))
        has_new_item = False
        for item in page_items:
            if start_time + max_time_limit < time.time():
                break
            if item['id'] in exist_ids:
                logging.info(f'Post {item["id"]!r} already exist, skipped.')
                continue

            logging.info(f'Post {item["id"]!r} confirmed.')
            tag_list = []
            for tag_item in item['tags']:
                if tag_item['data'] not in d_tags:
                    d_tags[tag_item['data']] = {
                        **tag_item,
                        'count': 0,
                    }
                d_tags[tag_item['data']]['count'] += 1
                tag_list.append(tag_item['data'])
            records.append({**item, 'tags': tag_list})
            exist_ids.add(item['id'])
            has_update = True
            has_new_item = True

        _deploy(force=False)

        offset += len(page_items)
        if not page_items:
            break
        if not has_new_item:
            empty_page_count += 1
        else:
            empty_page_count = 0
        if empty_page_count >= 10:
            logging.info('Quit due to sync model.')
            break

    _deploy(force=True)


if __name__ == '__main__':
    logging.try_init_root(level=logging.INFO)
    session = get_requests_session()
    session.headers.update({
        'Cookie': '_ga=GA1.1.1171191360.1729229221; _cb=DagxJTCLoz6tD_aFZv; compass_uid=204d25fb-3668-46f8-9166-a329e71b2977; pushly.user_puuid_TgtvAlfG=Kjx6rmU0byTkBcm4K2hloOeBsEr2Vq6l; _pnlspid_TgtvAlfG=32133; _hjSessionUser_4936301=eyJpZCI6IjE5YTdjNGFkLTAwMjktNWNjYi1iMzQyLWQ4ODMwMDE3NGJhMiIsImNyZWF0ZWQiOjE3MjkyMjkyMjE2NjcsImV4aXN0aW5nIjp0cnVlfQ==; _y=9e187072-B9DB-4331-307A-8EF2CCF7AAD6; _shopify_y=9e187072-B9DB-4331-307A-8EF2CCF7AAD6; _hjSessionUser_1004046=eyJpZCI6IjlhYTU2Nzk3LWIwY2UtNTRmYy04NWUwLTU0OTY0MDZiZTY1OCIsImNyZWF0ZWQiOjE3Mjk1MjY0OTkxMjIsImV4aXN0aW5nIjp0cnVlfQ==; _pnss_TgtvAlfG=blocked; _ga_5FPLDLE8C6=GS1.1.1730216484.7.0.1730216484.0.0.0; split=%7B%22split%3A213803%22%3A%22redesign%22%7D; split_all_domain=%7B%22split%3A213803%22%3A%22redesign%22%7D; ___nrbi=%7B%22firstVisit%22%3A1729229221%2C%22userId%22%3A%22204d25fb-3668-46f8-9166-a329e71b2977%22%2C%22userVars%22%3A%5B%5D%2C%22futurePreviousVisit%22%3A1733141908%2C%22timesVisited%22%3A6%7D; _hjSession_1004046=eyJpZCI6IjhjMzFhYTYyLTAxYjUtNDljZS04MmNiLTIwYThlYzU3YTM2MiIsImMiOjE3MzMxNDE5MTEyNTcsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=; ___nrbic=%7B%22previousVisit%22%3A1730200942%2C%22currentVisitStarted%22%3A1733141908%2C%22sessionId%22%3A%22bb41b6a7-eb03-407d-9ab7-67a65646b42f%22%2C%22sessionVars%22%3A%5B%5D%2C%22visitedInThisSession%22%3Atrue%2C%22pagesViewed%22%3A3%2C%22landingPage%22%3A%22https%3A//knowyourmeme.com/memes/all%22%2C%22referrer%22%3A%22%22%2C%22lpti%22%3Anull%7D; _chartbeat2=.1729229221474.1733141951004.0000000000000001.hNuNCC5CNJIkSeVVCJvfyFBsMuwg.1; _cb_svref=external; _pn_TgtvAlfG=eyJzdWIiOnsidWRyIjowLCJpZCI6IktqeDZybVUwYnlUa0JjbTRLMmhsb09lQnNFcjJWcTZsIiwic3MiOi0xfSwibHVhIjoxNzMzMTQxOTIzODUwfQ; _awl=2.1733141960.5-0ec718646045104bee6f0440222cb7ab-6763652d617369612d6561737431-1; _ga_5FPLDLE8C6=GS1.1.1733141908.8.1.1733141969.0.0.0; _chartbeat5=309|1659|%2Fcategories%2Fmeme|https%3A%2F%2Fknowyourmeme.com%2Fcategories%2Fmeme%2Fpage%2F2%3Fsort%3Dnewest%26status%3Dall|Be3E7zZ24C6CAxpsuBBlDykBFOf2Z||c|Be3E7zZ24C6CAxpsuBBlDykBFOf2Z|knowyourmeme.com|',
        'Referer': 'https://knowyourmeme.com/memes/all',
        'content-type': 'application/json',
    })
    from pprint import pprint
    pprint(list(_get_index_by_offset(session=session, offset=80)))
    # sync(
    #     repository='datacollection/memes_index',
    #     sync_mode=True,
    # )
