import re
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import Tag
from hbutils.string import underscore
from markdownify import MarkdownConverter
from pyquery import PyQuery as pq

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
    main_body_md = to_md(main_body_html.outer_html(), page_url=page_url)

    tags_html = page('#entry_body aside.left')
    infos = {}
    for dt_item in tags_html('dt').items():
        dd_item = dt_item.next('dd')
        key = underscore(dt_item.text().strip())
        infos[key] = {
            'key': dt_item.text().strip(),
            'text': dd_item.text().strip(),
            'md': to_md(dd_item.outer_html(), page_url=page_url).strip(),
            'raw': dd_item.outer_html(),
        }

    refs = []
    for pitem in page('h2#external-references').next('.references')('p[id]').items():
        if not pitem('sup'):
            continue

        sup_id = int(pitem('sup').text().strip().lstrip('[').rstrip(']').strip())
        footnote = pitem('.footnote-text')
        refs.append({
            'sup_id': sup_id,
            'footnote': {
                'text': footnote.text().strip(),
                'md': to_md(footnote.text().strip(), page_url=page_url).strip(),
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
                'md': to_md(info_item.outer_html(), page_url=page_url).strip(),
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
                'md': to_md(info_item.outer_html(), page_url=page_url).strip(),
                'raw': info_item.outer_html(),
            }
        })

    return {
        'page_url': resp.url,
        'infos': infos,
        'body': {
            'html': main_body_html,
            'md': main_body_md.strip(),
        },
        'related_entries': related_entries,
        'recent_videos': recent_videos,
        'recent_images': recent_images,
    }
