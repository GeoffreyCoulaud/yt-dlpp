from functools import partial
from multiprocessing import JoinableQueue, Queue
from typing import Any

from yt_dlp import YoutubeDL

from yt_dlpp.workers.progress_info import ProgressInfo
from yt_dlpp.workers.worker import Worker


class DownloadWorker(Worker):
    """Worker process that downloads from yt-dlp video urls"""

    input_queue: JoinableQueue
    output_queue: Queue

    _ydl: YoutubeDL

    def __init__(
        self,
        options: dict[str, Any],
        input_queue: JoinableQueue,
        output_queue: Queue,
    ) -> None:
        """
        Initialize a DownloadWorker object.

        Notes:
        - The options are copied when passed to the constructor
        """
        super().__init__(input_queue, output_queue)

        # Build the ydl object
        options = dict(options)  # Make a copy to not mess with other options users
        hook = partial(self._progress_hook, progress_queue=self.output_queue)
        options["progress_hooks"] = [hook]
        self._ydl = YoutubeDL(options)

    def _progress_hook(progress_info: ProgressInfo, *, progress_queue: Queue) -> Any:
        """yt-dlp progress hook relaying the progress info to the progress queue."""
        progress_queue.put(progress_info)

    def _process_item(self, item: str) -> None:
        # Download the video
        self._ydl.download(item)
