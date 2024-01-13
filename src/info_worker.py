from multiprocessing import JoinableQueue
from typing import Any, Optional

from yt_dlp import YoutubeDL

from worker import BaseWorker


class InfoWorker(BaseWorker[str, str]):
    """
    Worker process that treats yt-dlp urls, gets info from them and passes video urls.

    - The input url may refer to a video or playlist.
    """

    input_queue: JoinableQueue
    output_queue: JoinableQueue

    _ydl: YoutubeDL

    def __init__(
        self,
        options: dict[str, Any],
        input_queue: JoinableQueue,
        output_queue: JoinableQueue,
    ) -> None:
        super().__init__(input_queue, output_queue)
        self._ydl = YoutubeDL(options)

    def _process_item(self, item: str) -> list[str]:
        # Get the information for the url
        info = self._ydl.extract_info(item)

        # Ensure that info is valid
        if not isinstance(info, dict):
            return []
        if (info_type := info.get("_type")) not in ("video", "playlist"):
            return []

        # Extract the video urls
        infos = [info] if info_type == "video" else info["entries"]
        urls = [url for info in infos if (url := info.get("original_url")) is not None]

        return urls
