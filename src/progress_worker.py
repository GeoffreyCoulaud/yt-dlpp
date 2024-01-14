from multiprocessing import JoinableQueue
from worker import Worker

from src.progress_info import ProgressInfo


class ProgressWorker(Worker):
    """Worker in charge of displaying progress info"""

    input_queue: JoinableQueue
    output_queue: None = None

    _downloads: dict[str, ProgressInfo]

    def __init__(self, input_queue: JoinableQueue):
        super().__init__(input_queue, None)
        self._downloads = {}

    def _process_item(self, item: ProgressInfo) -> None:
        # TODO Clear out the screen
        # TODO Write the current state
        pass
