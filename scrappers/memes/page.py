import math
import os
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import Tag
from ditk import logging
from hbutils.string import underscore, plural_word
from hbutils.system import TemporaryDirectory
from hfutils.cache import delete_detached_cache
from hfutils.operate import get_hf_client, get_hf_fs, upload_directory_as_directory
from hfutils.utils import number_to_tag
from markdownify import MarkdownConverter
from pyquery import PyQuery as pq
from pyrate_limiter import Rate, Limiter, Duration
from tqdm import tqdm

from scrappers.memes.index import _to_list
from ..utils import get_requests_session


class CustomConverter(MarkdownConverter):
    """
    Create a custom MarkdownConverter that adds two newlines after an image
    """

    def __init__(self, page_url: Optional[str] = None, **options):
        super().__init__(**options)
        self.page_url = page_url

    def convert_a(self, el: Tag, text, convert_as_inline):
        if el['href'] and self.page_url:
            el['href'] = urljoin(self.page_url, el['href'])
        return super().convert_a(el, text, convert_as_inline)


def to_md(html, page_url: Optional[str] = None, **options):
    return CustomConverter(page_url=page_url, **options).convert(html)


def get_page_text(page_url, session: Optional[requests.Session] = None):
    session = session or get_requests_session()
    resp = session.get(page_url)
    resp.raise_for_status()
    page = pq(resp.text)

    main_body_html = page('#entry_body section.bodycopy')
    main_body_md = to_md(main_body_html.outer_html(), page_url=resp.url)

    tags_html = page('#entry_body aside.left')
    infos = {}
    for dt_item in tags_html('dt').items():
        dd_item = dt_item.next('dd')
        key = underscore(dt_item.text().strip())
        infos[key] = {
            'key': dt_item.text().strip(),
            'text': dd_item.text().strip(),
            'md': to_md(dd_item.outer_html(), page_url=resp.url).strip(),
            'raw': dd_item.outer_html(),
        }

    refs = []
    for pitem in page('h2#external-references').next('.references')('p[id]').items():
        if not pitem('sup'):
            continue

        sup_ids = list(map(int, re.findall(r'\[(\d+)]', pitem('sup').text().strip())))
        footnote = pitem('.footnote-text')
        refs.append({
            'ids': sup_ids,
            'footnote': {
                'text': footnote.text().strip(),
                'md': to_md(footnote.text().strip(), page_url=resp.url).strip(),
                'raw': footnote.outer_html(),
            }
        })

    related_entries = []
    for eitem in page('#related-entries td').items():
        if not eitem('img[title]'):
            continue
        entry_id = int(re.findall(r'entry_(?P<eid>\d+)', eitem.attr('class') or '')[0])
        related_entries.append({
            'entry_id': entry_id,
            'url': urljoin(resp.url, eitem('h2 a').attr('href')),
            'title': eitem('h2').text().strip(),
        })

    recent_videos = []
    for ritem in page('#videos_list.section_body.recent-videos td').items():
        if not ritem('.video_box'):
            continue
        video_id = int(re.findall(r'video_(?P<eid>\d+)', ritem('.video_box').attr('id') or '')[0])
        info_item = ritem('.info')
        recent_videos.append({
            'video_id': video_id,
            'page_url': urljoin(resp.url, ritem('a.video').attr('href')),
            'alt': ritem('a.video img').attr('alt'),
            'preview_url': urljoin(resp.url, ritem('a.video img').attr('src')),
            'source_url': urljoin(resp.url, ritem('a.video img').attr('data-src'))
            if ritem('a.video img').attr('data-src') else None,
            'info': {
                'text': info_item.text().strip(),
                'md': to_md(info_item.outer_html(), page_url=resp.url).strip(),
                'raw': info_item.outer_html(),
            }
        })

    recent_images = []
    for ritem in page('#photos_list.section_body.recent-images td').items():
        if not ritem('.photo_box'):
            continue
        photo_id = int(re.findall(r'photo_(?P<eid>\d+)', ritem('.photo_box').attr('id') or '')[0])
        info_item = ritem('.info')
        recent_images.append({
            'photo_id': photo_id,
            'page_url': urljoin(resp.url, ritem('a.photo').attr('href')),
            'alt': ritem('a.photo img').attr('alt'),
            'preview_url': urljoin(resp.url, ritem('a.photo img').attr('src')),
            'source_url': urljoin(resp.url, ritem('a.photo img').attr('data-src'))
            if ritem('a.photo img').attr('data-src') else None,
            'info': {
                'text': info_item.text().strip(),
                'md': to_md(info_item.outer_html(), page_url=resp.url).strip(),
                'raw': info_item.outer_html(),
            }
        })

    return {
        'page_url': resp.url,
        'infos': infos,
        'body': {
            'html': main_body_html.outer_html(),
            'md': main_body_md.strip(),
        },
        'references': refs,
        'related_entries': related_entries,
        'recent_videos': recent_videos,
        'recent_images': recent_images,
    }


def sync(src_repo: str, dst_repo: str, upload_time_span: float = 30,
         batch_size: int = 1000, proxy_pool: Optional[str] = None):
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

        def _run(ritem):
            nonlocal has_update
            try:
                vitem = get_page_text(ritem['link'], session=session)
                records.append({
                    **ritem,
                    'page_url': vitem['page_url'],
                    'body_html': vitem['body']['html'],
                    'body_md': vitem['body']['md'],
                    'infos': vitem['infos'],
                    'related_entries': vitem['related_entries'],
                    'recent_images': vitem['recent_images'],
                    'recent_videos': vitem['recent_videos'],
                })
                has_update = True
            except Exception as err:
                logging.info(f'Error occurred when running #{ritem["id"]!r}, url: {ritem["link"]!r} - {err!r}')
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

            hf_upload_limiter.try_acquire('hf upload limit')
            upload_directory_as_directory(
                repo_id=dst_repo,
                repo_type='dataset',
                local_directory=td,
                path_in_repo='.',
                message=f'Add {plural_word(len(df_records) - _total_count, "new record")} into index',
            )
            _total_count = len(df_records)


if __name__ == '__main__':
    logging.try_init_root(level=logging.INFO)
    sync(
        src_repo='datacollection/memes_index',
        dst_repo='datacollection/memes',
        batch_size=1000,
        proxy_pool=os.environ['PP_SITE'],
    )
