import os
from typing import Union

from .openai import get_openai_client

InputTyping = Union[str, os.PathLike, dict, list, tuple]


def infer_with_readerlm(input_: str, model_name: str = 'reader-lm:1.5b'):
    client = get_openai_client()
    response = client.chat.completions.create(
        model=model_name,
        messages=[
            {"role": "user", "content": input_},
        ],
        # max_tokens=4096,
        # temperature=0,
        max_tokens=1024,
        temperature=0,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0.08,  # 这个值近似于原代码中的repetition_penalty
        stream=False
    )
    return response.choices[0].message.content.strip()
