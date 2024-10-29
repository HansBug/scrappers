import os
from functools import lru_cache

from openai import OpenAI


@lru_cache()
def get_openai_client():
    return OpenAI(
        api_key=os.environ.get('LLM_API_KEY'),
        base_url=os.environ['LLM_API_URL'],
    )
