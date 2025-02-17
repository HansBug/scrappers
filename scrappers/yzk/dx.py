import math
import os
import shutil

import pandas as pd
from ditk import logging
from hfutils.cache import delete_detached_cache
from hfutils.operate import get_hf_client, get_hf_fs
from pyrate_limiter import Rate, Limiter, Duration


def sync(src_repo: str, dst_repo: str, upload_time_span: float = 30):
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

    df_src_pages = pd.read_parquet(hf_client.hf_hub_download(
        repo_id=src_repo,
        repo_type='dataset',
        filename='pages.parquet'
    ))

    if hf_client.file_exists(
            repo_id=dst_repo,
            repo_type='dataset',
            filename='pages.parquet',
    ):
        df_pages = pd.read_parquet(hf_client.hf_hub_download(
            repo_id=dst_repo,
            repo_type='dataset',
            filename='pages.parquet',
        ))
        records = df_pages.to_dict('records')
        exist_ids = set(df_pages['id'])
    else:
        records = []
        exist_ids = set()

    df_src_pages = df_src_pages[~df_src_pages['id'].isin(exist_ids)]
    df_src_pages = df_src_pages.sort_values(by=['id'], ascending=[True])

    assert shutil.which('yt-dlp'), \
        'No yt-dlp found, you should install it follow these instructions: https://github.com/yt-dlp/yt-dlp'

    print(df_src_pages)


if __name__ == '__main__':
    logging.try_init_root(logging.INFO)
    repo = 'datacollection/yzk_xl'
    sync(
        src_repo=f'{repo}_index',
        dst_repo=f'{repo}_full'
    )
