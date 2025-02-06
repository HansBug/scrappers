import math
import os
import time
from threading import Lock
from typing import Optional

import pandas as pd
from ditk import logging
from hbutils.string import plural_word
from hbutils.system import TemporaryDirectory
from hfutils.cache import delete_detached_cache
from hfutils.operate import get_hf_client, get_hf_fs, upload_directory_as_directory
from hfutils.utils import number_to_tag
from pyrate_limiter import Limiter, Rate, Duration

from .page import iter_all_templates, get_generator_info
from ..utils import parallel_call, get_requests_session


def sync(repository: str, upload_time_span: float = 30,
         deploy_span: float = 5 * 60, proxy_pool: Optional[str] = None):
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
        d_records = {item['id']: item for item in df.to_dict('records')}
        exist_caption_urls = set(df['caption_url'])
    else:
        d_records = {}
        exist_caption_urls = set()

    _last_update, has_update = None, False
    _total_count = len(d_records)

    def _deploy(force=False):
        nonlocal _last_update, has_update, _total_count

        if not has_update:
            return
        if not force and _last_update is not None and _last_update + deploy_span > time.time():
            return

        with TemporaryDirectory() as td:
            parquet_file = os.path.join(td, 'table.parquet')
            df_records = pd.DataFrame(list(d_records.values()))
            df_records = df_records.sort_values(by=['id'], ascending=[False])
            df_records.to_parquet(parquet_file, engine='pyarrow', index=False)

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
                print('- imgflip.com', file=f)
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

    session = get_requests_session()
    if proxy_pool:
        logging.info(f'Proxy pool {proxy_pool!r} enabled.')
        session.proxies.update({
            'http': proxy_pool,
            'https': proxy_pool
        })

    lock = Lock()

    def _fn(item):
        nonlocal has_update
        if item['caption_url'] in exist_caption_urls:
            logging.warning(f'Meme template {item["caption_url"]!r} already exist, skipped.')
            return

        vitem = get_generator_info(caption_url=item['caption_url'], session=session)
        with lock:
            current_item = {**item, **vitem}
            logging.info(f'Writing item #{current_item["id"]!r}, url_name: {current_item["url_name"]!r} ...')
            if current_item['id'] not in d_records:
                d_records[current_item['id']] = current_item
                exist_caption_urls.add(current_item['caption_url'])
                has_update = True
            if not d_records[current_item['id']]['alt_names'] and current_item['alt_names']:
                d_records[current_item['id']]['alt_names'] = current_item['alt_names']
                has_update = True
            if not d_records[current_item['id']]['default_settings'] and current_item['default_settings']:
                d_records[current_item['id']]['default_settings'] = current_item['default_settings']
                has_update = True
            _deploy(force=False)

    parallel_call(iter_all_templates(session=session), _fn, desc='Iter all items')

    _deploy(force=True)


if __name__ == '__main__':
    logging.try_init_root(level=logging.INFO)
    sync(
        repository='datacollection/imgflip_index',
        proxy_pool=os.environ['PP_SITE'],
    )
