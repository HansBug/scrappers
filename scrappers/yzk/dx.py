import glob
import math
import os
import shutil
import subprocess
import time
from typing import Optional

import pandas as pd
from ditk import logging
from hbutils.string import plural_word
from hbutils.system import TemporaryDirectory
from hfutils.cache import delete_detached_cache
from hfutils.operate import get_hf_client, get_hf_fs, upload_directory_as_directory
from hfutils.utils import hf_normpath, number_to_tag
from pyrate_limiter import Rate, Limiter, Duration
from tqdm import tqdm

from scrappers.utils import check_video_integrity, get_video_metadata


def sync(src_repo: str, dst_repo: str, upload_time_span: float = 30, deploy_span: float = 5 * 60,
         cookie_file: Optional[str] = None):
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
        df_records = pd.read_parquet(hf_client.hf_hub_download(
            repo_id=dst_repo,
            repo_type='dataset',
            filename='pages.parquet',
        ))
        records = df_records.to_dict('records')
        exist_ids = set(df_records['id'])
    else:
        records = []
        exist_ids = set()

    df_src_pages = df_src_pages[~df_src_pages['id'].isin(exist_ids)]
    df_src_pages = df_src_pages.sort_values(by=['id'], ascending=[True])

    assert shutil.which('yt-dlp'), \
        'No yt-dlp found, you should install it follow these instructions: https://github.com/yt-dlp/yt-dlp'

    with TemporaryDirectory() as upload_dir:

        def _clean_dir():
            for fn in os.listdir(upload_dir):
                if os.path.isfile(os.path.join(upload_dir, fn)):
                    os.remove(os.path.join(upload_dir, fn))
                elif os.path.isdir(os.path.join(upload_dir, fn)):
                    shutil.rmtree(os.path.join(upload_dir, fn))

        _last_update, has_update = None, False
        _total_count = len(records)

        def _deploy(force=False):
            nonlocal _last_update, has_update, _total_count

            if not has_update:
                return
            if not force and _last_update is not None and _last_update + deploy_span > time.time():
                return

            # main code
            parquet_file = os.path.join(upload_dir, 'pages.parquet')
            df_records = pd.DataFrame(records)
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
                print('- youzhik.com', file=f)
                print('---', file=f)
                print('', file=f)

                print('## Records', file=f)
                print(f'', file=f)
                df_records_shown = df_records[:50]
                print(f'{plural_word(len(df_records), "record")} in total. '
                      f'Only {plural_word(len(df_records_shown), "record")} shown.', file=f)
                print(f'', file=f)
                print(df_records_shown[[
                    'id', 'title', 'page_url', 'video_source_url',
                    "filename", "file_size", "mimetype", "width", "height",
                    "total_frames", "fps", "codec", "profile", "duration_seconds", "bit_rate", "format_name",
                    "audio_codec", "audio_channels", "audio_sample_rate"
                ]].to_markdown(index=False),
                      file=f)
                print(f'', file=f)

            hf_upload_limiter.try_acquire('hf upload limit')
            upload_directory_as_directory(
                repo_id=dst_repo,
                repo_type='dataset',
                local_directory=td,
                path_in_repo='.',
                message=f'Add {plural_word(len(df_records) - _total_count, "new record")} into index',
            )

            has_update = False
            _last_update = time.time()
            _total_count = len(df_records)
            _clean_dir()

        for item in tqdm(df_src_pages.to_dict('records')):
            logging.info(f'Processing item #{item["id"]}, video_url: {item["video_source_url"]!r} ...')

            with TemporaryDirectory() as td:
                try:
                    cmd = [shutil.which('yt-dlp')]
                    if cookie_file:
                        cmd.extend(['--cookies', cookie_file])
                    cmd.append(item['video_source_url'])
                    logging.info(f'Running {cmd!r} ...')
                    p = subprocess.run(cmd, cwd=td)
                    p.check_returncode()
                except Exception:
                    logging.exception(f'Error found when downloading #{item["id"]}, skipped.')
                    continue

                files = glob.glob(os.path.join(td, '*'))
                assert len(files) == 1, f'Non-unique files: {files!r}'
                video_file = files[0]

                logging.info(f'Video file found: {video_file!r}, checking it ...')
                check_video_integrity(video_file)

                logging.info('Getting its metadata ...')
                meta = get_video_metadata(video_file)

                filename = os.path.basename(video_file)
                _, ext = os.path.splitext(filename)
                dst_video_file = os.path.join(upload_dir, 'videos', f'{item["id"]}{ext}')
                os.makedirs(os.path.dirname(dst_video_file), exist_ok=True)
                shutil.move(video_file, dst_video_file)

                row = {
                    **item,
                    **meta,
                    'file_in_repo': hf_normpath(os.path.relpath(dst_video_file, upload_dir)),
                    'filename': filename,
                    'file_size': os.path.getsize(dst_video_file),
                }
                records.append(row)
                exist_ids.add(row['id'])

            _deploy(force=False)

        _deploy(force=True)


if __name__ == '__main__':
    logging.try_init_root(logging.INFO)
    repo = 'datacollection/yzk_xl'
    sync(
        src_repo=f'{repo}_index',
        dst_repo=f'{repo}_full',
        cookie_file='cookies.txt',
    )
