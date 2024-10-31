import math
import os
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from typing import Optional

import pandas as pd
from ditk import logging
from hbutils.string import plural_word
from hbutils.system import TemporaryDirectory
from hfutils.cache import delete_detached_cache
from hfutils.operate import get_hf_client, get_hf_fs, upload_directory_as_directory
from hfutils.utils import number_to_tag
from pyrate_limiter import Rate, Limiter, Duration
from tqdm import tqdm

from .profile import get_profile, get_comments
from ..utils import get_requests_session


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

    df_src = pd.read_parquet(hf_client.hf_hub_download(
        repo_id=src_repo,
        repo_type='dataset',
        filename='table.parquet',
    ))

    if hf_fs.exists(f'datasets/{dst_repo}/table.parquet'):
        df_dst = pd.read_parquet(hf_client.hf_hub_download(
            repo_id=dst_repo,
            repo_type='dataset',
            filename='table.parquet',
        ))
        records = df_dst.to_dict('records')
        exist_ids = set(df_dst['id'])
        _total_count = len(df_dst)
    else:
        records = []
        exist_ids = set()
        _total_count = 0

    if hf_fs.exists(f'datasets/{dst_repo}/comments.parquet'):
        df_dst_comments = pd.read_parquet(hf_client.hf_hub_download(
            repo_id=dst_repo,
            repo_type='dataset',
            filename='comments.parquet',
        ))
        comments = df_dst_comments.to_dict('records')
        exist_comment_ids = set(df_dst_comments['id'])
        _total_comments_count = len(df_dst_comments)
    else:
        comments = []
        exist_comment_ids = set()
        _total_comments_count = 0

    session = get_requests_session()
    if proxy_pool:
        logging.info(f'Proxy pool {proxy_pool!r} enabled.')
        session.proxies.update({
            'http': proxy_pool,
            'https': proxy_pool
        })

    df_src = df_src[~df_src['id'].isin(exist_ids)]
    for batch_id in range(int(math.ceil(len(df_src) / batch_size))):
        df_block = df_src[batch_id * batch_size:(batch_id + 1) * batch_size]
        logging.info(f'Syncing block #{batch_id!r} ...')

        tp = ThreadPoolExecutor(max_workers=32)
        has_update = False
        pg = tqdm(desc=f'Block #{batch_id}', total=len(df_block))
        lock = Lock()

        def _run(ritem):
            nonlocal has_update, comments, records, exist_comment_ids
            try:
                vitem = get_profile(ritem['id'], session=session)
                comment_items = get_comments(ritem['id'], session=session)
                with lock:
                    logging.info(f'access {ritem["id"]!r} --> {vitem["id"]!r}')
                    if vitem['id'] not in exist_ids:
                        records.append(vitem)
                        exist_ids.add(vitem['id'])
                        has_update = True
                        for comment_item in comment_items:
                            if comment_item['id'] not in exist_comment_ids:
                                comments.append(comment_item)
                                exist_comment_ids.add(comment_item['id'])
                                has_update = True
            except Exception as err:
                logging.warning(f'Error occurred when running #{ritem["id"]!r} - {err!r}')
                raise
            finally:
                pg.update()

        for item in df_block.to_dict('records'):
            tp.submit(_run, item)

        tp.shutdown(wait=True)

        if not has_update:
            continue

        with TemporaryDirectory() as td:
            parquet_file = os.path.join(td, 'table.parquet')
            df_records = pd.DataFrame(records)
            df_records = df_records.sort_values(by=['id'], ascending=[False])
            df_records.to_parquet(parquet_file, engine='pyarrow', index=False)

            comment_parquet_file = os.path.join(td, 'comments.parquet')
            df_comments = pd.DataFrame(comments)

            df_comments = df_comments.sort_values(by=['profile_id', 'id'], ascending=[False, False])
            df_comments.to_parquet(comment_parquet_file, engine='pyarrow', index=False)

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
                print('- personality-database.com', file=f)
                print('---', file=f)
                print('', file=f)

                print('## Records', file=f)
                print(f'', file=f)
                df_records_shown = df_records[:50][
                    [
                        'id',  # 唯一标识符
                        'mbti_profile',
                        'category',  # 分类名称
                        'subcategory',  # 子分类名称
                        'mbti_profile',  # MBTI配置文件名称
                        'personality_type',  # 个性类型描述
                        'contributor',  # 贡献者名称
                        'contributor_create_date',  # 贡献者创建日期
                        'total_vote_counts',  # 总投票数
                        'vote_count',  # MBTI投票数
                        'vote_count_mbti',  # MBTI投票数
                        'vote_count_enneagram',  # Enneagram投票数
                        'comment_count',  # 评论数量
                        'watch_count',  # 观看数量
                        'is_active',  # 是否活跃
                        'is_approved',  # 是否已批准
                        'profile_image_url',  # 配置文件图片URL
                        'mbti_type',  # MBTI类型
                        'alt_subcategory',  # 替代子分类
                    ]
                ]
                print(f'{plural_word(len(df_records), "record")} in total. '
                      f'Only {plural_word(len(df_records_shown), "record")} shown.', file=f)
                print(f'', file=f)
                print(df_records_shown.to_markdown(index=False), file=f)
                print(f'', file=f)

                print('## Comments', file=f)
                print(f'', file=f)
                df_comments_shown = df_comments[:50][
                    [
                        'id',  # 数据项唯一标识符
                        'profile_id',  # 用户资料ID
                        'username',  # 用户名
                        'user_mbti',  # 用户MBTI类型
                        'create_date',  # 创建日期
                        'update_date',  # 更新日期
                        'reply_count',  # 回复数
                        'user_title',  # 用户头衔
                        'user_standing',  # 用户社区地位
                        'user_contribution',  # 用户贡献
                        'is_active',  # 是否活跃
                        'allow_voting',  # 是否允许投票
                        'vote_count',  # 投票数
                        'pic_path',  # 图片路径
                        'is_pro_user',  # 是否为专业用户
                        'page_owner_id',  # 页面所有者ID
                        'is_hidden'  # 是否隐藏
                    ]
                ]
                print(f'{plural_word(len(df_comments), "comment")} in total. '
                      f'Only {plural_word(len(df_comments_shown), "comment")} shown.', file=f)
                print(f'', file=f)
                print(df_comments_shown.to_markdown(index=False), file=f)
                print(f'', file=f)

            hf_upload_limiter.try_acquire('hf upload limit')
            upload_directory_as_directory(
                repo_id=dst_repo,
                repo_type='dataset',
                local_directory=td,
                path_in_repo='.',
                message=f'Add {plural_word(len(df_records) - _total_count, "new record")}, '
                        f'with {plural_word(len(df_comments) - _total_comments_count, "new comment")} into index',
            )
            _total_count = len(df_records)
            _total_comments_count = len(df_comments)


if __name__ == '__main__':
    logging.try_init_root(level=logging.INFO)
    sync(
        src_repo='datacollection/pdb_index',
        dst_repo='datacollection/pdb_profiles',
        max_workers=32,
        batch_size=1000,
        proxy_pool=os.environ['PP_SITE'],
    )
