import json
import logging
from functools import lru_cache
from multiprocessing import JoinableQueue
from subprocess import PIPE, Popen
from typing import Literal, Sequence, TypedDict

from yt_dlpp.interceptors.interceptor import DlInterceptor
from yt_dlpp.workers.worker import Worker


class _VideoSubdict(TypedDict):
    id: str
    original_url: str
    title: str


class _ProgressSubdict(TypedDict):
    downloaded_bytes: int
    total_bytes: int
    total_bytes_estimate: int
    eta: Literal["NA"] | float
    speed: Literal["NA"] | float
    elapsed: float


class ProgressLineDict(TypedDict):
    video: _VideoSubdict
    progress: _ProgressSubdict


class DownloadWorker(Worker):
    """Worker process that downloads from yt-dlp video urls"""

    input_queue: JoinableQueue
    output_queue: JoinableQueue

    _ydl_args: Sequence[str]

    @property
    @lru_cache(maxsize=1)
    def _base_command(self) -> tuple[str]:
        """Generate the base command"""
        interceptor = DlInterceptor()
        _, allowed = interceptor.parse_known_args(self._ydl_args)
        progress_template = (
            "{"
            + '"video": %(info.{id,original_url,title})j,'
            + '"progress": %(progress.{downloaded_bytes,total_bytes,total_bytes_estimate,eta,speed,elapsed})j'
            + "}"
        )
        return (
            "yt-dlp",
            "--quiet",
            "--progress",
            "--newline",
            "--progress-template",
            progress_template,
            *allowed,
        )

    def __init__(
        self,
        ydl_args: Sequence[str],
        input_queue: JoinableQueue,
        output_queue: JoinableQueue,
    ) -> None:
        super().__init__(input_queue, output_queue)
        self._ydl_args = ydl_args

    def _process_item(self, item: str) -> None:
        # Download the video
        logging.debug("Starting download for %s", item)
        process = Popen(
            (*self._base_command, item),
            encoding="utf-8",
            bufsize=1,
            universal_newlines=True,
            stdout=PIPE,
        )
        # Get progress as soon as a line is available
        for line in process.stdout:
            parsed_line: ProgressLineDict = json.loads(line)
            self._send_output(parsed_line)
        logging.debug("Download finished for %s", item)
