import json
import os
import subprocess

import cv2
from ditk import logging


def get_video_metadata(video_path: str) -> dict:
    metadata = {}
    cap = cv2.VideoCapture(video_path)
    if cap.isOpened():
        metadata.update({
            'width': int(cap.get(cv2.CAP_PROP_FRAME_WIDTH)),
            'height': int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT)),
            'total_frames': int(cap.get(cv2.CAP_PROP_FRAME_COUNT)),
            'fps': cap.get(cv2.CAP_PROP_FPS),
        })
        cap.release()

    ffprobe_cmd = [
        'ffprobe',
        '-v', 'quiet',
        '-print_format', 'json',
        '-show_format',
        '-show_streams',
        video_path
    ]

    result = subprocess.run(ffprobe_cmd, capture_output=True, text=True)
    probe_data = json.loads(result.stdout)

    video_stream = next(
        (stream for stream in probe_data.get('streams', []) if stream.get('codec_type') == 'video'),
        None
    )

    audio_stream = next(
        (stream for stream in probe_data.get('streams', []) if stream.get('codec_type') == 'audio'),
        None
    )

    if video_stream:
        metadata.update({
            'codec': video_stream.get('codec_name'),
            'profile': video_stream.get('profile'),
        })

    format_info = probe_data.get('format', {})
    metadata.update({
        'duration_seconds': float(format_info.get('duration', 0)),
        'bit_rate': int(format_info.get('bit_rate', 0)) if format_info.get('bit_rate') else None,
        'format_name': format_info.get('format_name'),
    })

    if audio_stream:
        metadata.update({
            'audio_codec': audio_stream.get('codec_name'),
            'audio_channels': audio_stream.get('channels'),
            'audio_sample_rate': audio_stream.get('sample_rate'),
        })

    return metadata


def check_video_integrity(video_path, max_frames_to_check=10):
    if not os.path.exists(video_path):
        return False

    try:
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            return False

        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        fps = cap.get(cv2.CAP_PROP_FPS)
        if total_frames <= 0 or fps <= 0:
            cap.release()
            return False

        frames_to_check = min(max_frames_to_check, total_frames)
        for _ in range(frames_to_check):
            ret, frame = cap.read()
            if not ret:
                cap.release()
                return False

        cap.release()
        return True

    except Exception as err:
        logging.warning(f'Error when checking video file {video_path!r} - {err!r}')
        return False
