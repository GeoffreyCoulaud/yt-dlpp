from yt_dlpp.workers.worker import Worker


class DedupWorker(Worker[str, str]):
    """Worker in charge of deduplicating inputs"""

    _seen: set[str]

    def __init__(self, input_queue, output_queue) -> None:
        super().__init__(input_queue, output_queue)
        self._seen = set()

    def _process_item(self, item):
        if item in self._seen:
            return []
        self._seen.add(item)
        return [item]
