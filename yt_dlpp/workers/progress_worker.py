from multiprocessing import JoinableQueue

from rich.progress import (
    BarColumn,
    Progress,
    TaskID,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
    TotalFileSizeColumn,
    TransferSpeedColumn,
)

from yt_dlpp.workers.worker import Worker


class ProgressWorker(Worker):
    """Worker in charge of displaying progress info"""

    input_queue: JoinableQueue
    output_queue: None = None

    _progress_bar: Progress
    _tasks: dict[str, TaskID]

    def __init__(self, input_queue: JoinableQueue):
        super().__init__(input_queue, None)

    def run(self) -> None:
        columns = (
            TextColumn('"[progress.description]{task.description}"'),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("of"),
            TotalFileSizeColumn(),
            TextColumn("at"),
            TransferSpeedColumn(),
            TextColumn("ETA"),
            TimeRemainingColumn(),
        )
        with Progress(*columns) as self._progress_bar:
            super().run()

    def _process_item(self, progress_info: dict) -> None:
        # Get current info
        item_id = progress_info["info_dict"]["id"]
        total_bytes = progress_info.get("total_bytes")
        dl_bytes = progress_info.get("downloaded_bytes")

        # Create a task for the video if needed
        if item_id not in self._tasks:
            title = progress_info["info_dict"]["title"]
            task_id = self._progress_bar.add_task(description=title, total=total_bytes)
        task_id = self._tasks[item_id]

        # Update the task
        self._progress_bar.update(task_id, completed=dl_bytes, total=total_bytes)
