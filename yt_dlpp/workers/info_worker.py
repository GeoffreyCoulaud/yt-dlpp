import json
import logging
from argparse import ArgumentParser
from multiprocessing import JoinableQueue
from subprocess import CalledProcessError, run
from typing import Sequence

from yt_dlpp.workers.worker import Worker


class InfoWorker(Worker[str, str]):
    """
    Worker process that treats yt-dlp urls, gets info from them and passes video urls.

    - The input url may refer to a video or playlist.
    """

    input_queue: JoinableQueue
    output_queue: JoinableQueue

    _base_command: Sequence[str]

    def __init__(
        self,
        ydl_args: Sequence[str],
        input_queue: JoinableQueue,
        output_queue: JoinableQueue,
    ) -> None:
        super().__init__(input_queue, output_queue)
        # Define base command args
        # (removes --no-simulate from ydl args at info stage)
        parser = ArgumentParser()
        parser.add_argument("--no-simulate")
        _, allowed_ydl_args = parser.parse_known_args(ydl_args)
        self._base_command = (
            "yt-dlp",
            "--dump-json",
            *allowed_ydl_args,
        )
        logging.debug(
            "InfoWorker initialised with base command: %s",
            " ".join(self._base_command),
        )

    def _process_item(self, item: str) -> None:
        """
        Process an input url to be handled by yt-dlp (may be a video or a playlist)
        and pass video urls to the output queue
        """
        logging.debug("Processing url: %s", item)

        # Call yt-dlp in a subprocess
        try:
            completed_process = run(
                (*self._base_command, item),
                capture_output=True,
                check=True,
                encoding="utf-8",
            )
        except CalledProcessError as e:
            logging.error("Failed to get info from url %s: %s", item, e)
            return

        # Extract video URLs (one video infojson per line)
        for output_line in completed_process.stdout.splitlines():
            stripped_line = output_line.strip()
            if len(stripped_line) == 0:
                logging.debug("Empty line from yt-dlp")
                continue
            try:
                video_info_dict = json.loads(stripped_line)
            except json.JSONDecodeError:
                logging.debug("Invalid JSON line from yt-dlp: %s", stripped_line)
                continue
            if not isinstance(video_info_dict, dict):
                logging.debug("Invalid parsed value: %s", video_info_dict)
                continue
            video_url = video_info_dict.get("original_url")
            if video_url is None:
                logging.debug("No video URL in infojson: %s", video_info_dict)
                continue
            logging.debug("Got video URL from yt-dlp: %s", video_url)
            self._send_output(video_url)
