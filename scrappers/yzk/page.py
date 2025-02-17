import json
import os.path
import re
from typing import Optional
from urllib.parse import urljoin
from pprint import pprint

import requests
from ditk import logging
from hbutils.system import urlsplit
from pyquery import PyQuery as pq

from scrappers.memes.page import to_md
from scrappers.utils import get_requests_session


def safe_gb2312_decode(content):
    """
    安全地解码 GB2312 编码的内容，跳过无法解码的字符。

    Args:
        content: 字节类型的内容 (bytes)

    Returns:
        str: 解码后的字符串
    """
    # 首先尝试用 gb2312 解码
    try:
        return content.decode('gb2312', errors='replace')
    except UnicodeDecodeError:
        # 如果 gb2312 失败，尝试用 gbk (gb2312 的超集)
        try:
            return content.decode('gbk', errors='replace')
        except UnicodeDecodeError:
            # 最后使用 gb18030 (最大的中文字符集) 并替换错误字符
            return content.decode('gb18030', errors='replace')


def extract_page_info(page_url: str, session: Optional[requests.Session] = None):
    session = session or get_requests_session()
    logging.info(f'Accessing page {page_url!r} ...')
    resp = session.get(page_url)
    resp.raise_for_status()

    page = pq(safe_gb2312_decode(resp.content))
    title = page('#title').text().strip()

    video_source_url = None
    for script_item in page('#_video script').items():
        stext = script_item.text()
        findings = re.findall('var\s+str\s*=\s*(?P<source_url>\"[\s\S]+?\");', stext)
        if findings:
            video_source_url = json.loads(findings[0])
            if video_source_url:
                video_source_url = urljoin(resp.url, video_source_url)
            else:
                video_source_url = None
            break

    content_html = page('#conter').outer_html()
    content_md = to_md(content_html, page_url=resp.url)

    id_ = int(os.path.splitext(os.path.basename(urlsplit(resp.url).filename))[0])

    return {
        'id': id_,
        'title': title,
        'page_url': resp.url,
        'video_source_url': video_source_url,
        'content_md': content_md,
        'content_html': content_html,
    }


def extract_page_urls(url, session: Optional[requests.Session] = None):
    session = session or get_requests_session()
    logging.info(f'Iterating from page {url!r} ...')
    resp = session.get(url)
    resp.raise_for_status()

    page = pq(safe_gb2312_decode(resp.content))
    for li_item in page('#list ul li').items():
        page_url = urljoin(resp.url, li_item('a').attr('href'))
        title = li_item('a').text().strip()
        yield title, page_url


if __name__ == '__main__':
    logging.try_init_root(level=logging.INFO)
    d = extract_page_info('http://www.youzhik.com/xxxinli/xxyzk/2018/0529/52203.html')
    pprint(d)

    # for purl in extract_page_urls('http://youzhik.com/xxxinli/list_10_13.html'):
    #     print(purl)
