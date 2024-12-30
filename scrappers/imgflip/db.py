import json
import math
import mimetypes
import os
import random
import tarfile
from threading import Lock
from typing import Optional

import pandas as pd
from ditk import logging
from hbutils.string import plural_word
from hbutils.system import TemporaryDirectory, urlsplit
from hfutils.cache import delete_detached_cache
from hfutils.index import tar_create_index_for_directory
from hfutils.operate import get_hf_client, get_hf_fs, upload_directory_as_directory
from hfutils.utils import hf_normpath, number_to_tag
from imgutils.data import load_image
from pyrate_limiter import Rate, Limiter, Duration

from scrappers.utils import get_requests_session, parallel_call, download_file


def sync(src_repo: str, dst_repo: str, upload_time_span: float = 30,
         batch_size: int = 1000, max_workers: int = 32, proxy_pool: Optional[str] = None):
    delete_detached_cache()
    hf_upload_rate = Rate(1, int(math.ceil(Duration.SECOND * upload_time_span)))
    hf_upload_limiter = Limiter(hf_upload_rate, max_delay=1 << 32)

    hf_client = get_hf_client()
    hf_fs = get_hf_fs()

    if not hf_client.repo_exists(repo_id=dst_repo, repo_type='dataset'):
        hf_client.create_repo(repo_id=dst_repo, repo_type='dataset', private=True)
        attr_lines = hf_fs.read_text(f'datasets/{dst_repo}/.gitattributes').splitlines(keepends=False)
        attr_lines.append('*.json filter=lfs diff=lfs merge=lfs -text')
        attr_lines.append('*.csv filter=lfs diff=lfs merge=lfs -text')
        hf_fs.write_text(
            f'datasets/{dst_repo}/.gitattributes',
            os.linesep.join(attr_lines),
        )

    if hf_client.file_exists(
            repo_id=dst_repo,
            repo_type='dataset',
            filename='table.parquet',
    ):
        df_records = pd.read_parquet(hf_client.hf_hub_download(
            repo_id=dst_repo,
            repo_type='dataset',
            filename='table.parquet',
        ))
        records = df_records.to_dict('records')
        exist_ids = set(df_records['id'])
    else:
        records = []
        exist_ids = set()

    if hf_client.file_exists(
            repo_id=dst_repo,
            repo_type='dataset',
            filename='meta.json',
    ):
        meta_info = json.loads(hf_fs.read_text(f'datasets/{dst_repo}/meta.json'))
        max_volume_id = meta_info['max_volume_id']
    else:
        max_volume_id = 0

    df_src_records = pd.read_parquet(hf_client.hf_hub_download(
        repo_id=src_repo,
        repo_type='dataset',
        filename='table.parquet',
    ))
    df_src_records = df_src_records[~df_src_records['id'].isin(exist_ids)]
    df_src_records['random'] = [random.random() for _ in range(len(df_src_records))]
    df_src_records = df_src_records.sort_values(by=['random'], ascending=[True])
    del df_src_records['random']

    session = get_requests_session()
    if proxy_pool:
        logging.info(f'Proxy pool {proxy_pool!r} enabled.')
        session.proxies.update({
            'http': proxy_pool,
            'https': proxy_pool
        })

    for i in range(int(math.ceil(len(df_src_records) / batch_size))):
        df_block = df_src_records[i * batch_size:(i + 1) * batch_size]

        with TemporaryDirectory() as upload_dir:
            max_volume_id += 1
            tar_file = os.path.join(upload_dir, 'images', f'{max_volume_id // 1000}', f'{max_volume_id % 1000:03d}.tar')
            os.makedirs(os.path.dirname(tar_file), exist_ok=True)

            new_image_count = 0
            with tarfile.open(tar_file, 'a:') as tar:
                lock = Lock()

                def _fn(item):
                    nonlocal new_image_count
                    with TemporaryDirectory() as td:
                        _, ext = os.path.splitext(urlsplit(item['image_url']).filename.lower())
                        filename = f'{item["id"]}{ext}'
                        dst_file = os.path.join(td, filename)
                        download_file(
                            item['image_url'],
                            filename=dst_file,
                            session=session,
                        )

                        image = load_image(dst_file)
                        image.load()

                        mimetype, _ = mimetypes.guess_type(filename)
                        with lock:
                            tar.add(dst_file, filename)
                            records.append({
                                **item,
                                'mimetype': mimetype,
                                'file_size': os.path.getsize(dst_file),
                                'filename': filename,
                                'ext': ext,
                                'volume_file': hf_normpath(os.path.relpath(tar_file, upload_dir)),
                                'mode': image.mode,
                                'width': image.width,
                                'height': image.height,
                            })
                            exist_ids.add(item['id'])
                            new_image_count += 1

                parallel_call(df_block.to_dict('records'), _fn, desc=f'Sync Block #{max_volume_id}')

            if new_image_count > 0:
                tar_create_index_for_directory(upload_dir)
            else:
                os.remove(tar_file)

            df_records = pd.DataFrame(records)
            df_records = df_records.sort_values(by=['id'], ascending=[False])
            df_records.to_parquet(os.path.join(upload_dir, 'table.parquet'), index=False)

            with open(os.path.join(upload_dir, 'meta.json'), 'w') as f:
                json.dump({
                    'max_volume_id': max_volume_id,
                }, f)

            with open(os.path.join(upload_dir, 'README.md'), 'w') as f:
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
                repo_id=dst_repo,
                repo_type='dataset',
                local_directory=upload_dir,
                path_in_repo='.',
                message=f'Add {plural_word(new_image_count, "new image")} into index',
            )


if __name__ == '__main__':
    logging.try_init_root(logging.INFO)
    sync(
        src_repo='datacollection/imgflip_index',
        dst_repo='datacollection/imgflip',
        batch_size=2000,
    )
