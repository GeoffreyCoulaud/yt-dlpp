from multiprocessing import JoinableQueue

from tqdm import tqdm

from src.progress_info import (
    DownloadProgressInfo,
    ErrorProgressInfo,
    FinishedProgressInfo,
    ProgressInfo,
)
from worker import Worker


class ProgressWorker(Worker):
    """Worker in charge of displaying progress info"""

    input_queue: JoinableQueue
    output_queue: None = None

    # TODO this all smells bad, refactor.
    # Abandon the "bars" abstraction and re-print the whole state as needed.

    _downloads: dict[str, ProgressInfo]
    _bars: dict[str, tqdm]

    def __init__(self, input_queue: JoinableQueue):
        super().__init__(input_queue, None)
        self._downloads = {}
        self._bars = {}

    def _create_bar(self, item_id: str, title: str, total: int) -> None:
        """Create a tqdm bar"""
        if item_id in self._bars:
            raise KeyError("Bar has already been created")
        self._bars[item_id] = tqdm(
            None,
            desc=title,
            total=total,
            unit="B",
            unit_scale=True,
            leave=True,
        )

    def _get_bar(self, item_id: str) -> None | tqdm:
        """
        Get an internally created bar.\n
        Raises KeyError if no bar exist for that id.\n
        Raises ValueError if a closed bar exists for that id.
        """
        bar = self._bars[item_id]
        if bar is None:
            raise ValueError("Cannot access closed bar")
        return self._bars[item_id]

    def _process_error_item(self, item: ErrorProgressInfo) -> None:
        try:
            bar = self._get_bar(item["info_dict"]["id"])
        except (KeyError, ValueError):
            # Don't create a bar to error it instantly
            return
        bar.write("Error on %s" % item["info_dict"]["title"])
        bar.close()

    def _process_finished_item(self, item: FinishedProgressInfo) -> None:
        try:
            bar = self._get_bar(item["info_dict"]["id"])
        except (KeyError, ValueError):
            # Don't create a bar for it to finish instantly
            return
        bar.close()

    def _process_downloading_item(self, item: DownloadProgressInfo) -> None:
        item_id = item["info_dict"]["id"]
        try:
            bar = self._get_bar(item_id)
        except ValueError:
            # Bar has already been closed
            # (should never happen)
            return
        except KeyError:
            # Don't create a bar until we get a total
            if (total := item.get("total_bytes")) is None:
                return
            # Create a bar
            title = item["info_dict"]["title"]
            self._create_bar(item_id, title, total)
            bar = self._get_bar(item_id)
        # Update the bar
        if (progress := item.get("downloaded_bytes")) is None:
            return
        bar.update(progress)

    def _process_item(self, item: ProgressInfo) -> None:
        match item["status"]:
            case "error":
                self._process_error_item(item)
            case "finished":
                self._process_finished_item(item)
            case "downloading":
                self._process_downloading_item(item)

    def run(self) -> None:
        super().run()
        # Clean all bars up
        for bar in self._bars.values():
            bar.close()
