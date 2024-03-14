import logging
from multiprocessing import JoinableQueue
from typing import Optional, TypedDict

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

from yt_dlpp.workers.download_worker import ProgressLineDict
from yt_dlpp.workers.worker import Worker


class _VideoTaskInfo(TypedDict):
    """Information about a video task"""

    task_id: TaskID
    started: bool


class ProgressWorker(Worker):
    """Worker in charge of displaying progress info"""

    input_queue: JoinableQueue
    output_queue: None = None

    _progress_bar: Progress
    _tasks: dict[str, _VideoTaskInfo]

    def __init__(self, input_queue: JoinableQueue):
        super().__init__(input_queue, None)

    def run(self) -> None:
        self._tasks = {}
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

    def _get_progress_info_total_bytes(
        self, progress_info: ProgressLineDict
    ) -> Optional[float]:
        try:
            total_bytes = float(progress_info["progress"]["total_bytes"])
        except (ValueError, KeyError):
            try:
                total_bytes = float(progress_info["progress"]["total_bytes_estimate"])
            except (ValueError, KeyError):
                total_bytes = None
        return total_bytes

    def _get_progress_info_downloaded_bytes(
        self, progress_info: ProgressLineDict
    ) -> float:
        try:
            downloaded_bytes = float(progress_info["progress"]["downloaded_bytes"])
        except (ValueError, KeyError):
            downloaded_bytes = 0
        return downloaded_bytes

    def _task_exists(self, video_id: str) -> bool:
        """Check if a progress task exists for a video ID"""
        return video_id in self._tasks

    def _create_task(self, video_id: str, title: str) -> None:
        """Create a new progress task for a video ID"""
        logging.debug("Creating progress task for %s", video_id)
        task_id = self._progress_bar.add_task(description=title, start=False)
        self._tasks[video_id] = _VideoTaskInfo(task_id=task_id, started=False)

    def _task_started(self, video_id: str) -> bool:
        """Check if the progress task has been started for a video ID"""
        return self._tasks[video_id]["started"]

    def _start_task(self, video_id: str) -> None:
        """Start the progress task for a video ID"""
        logging.debug("Starting progress task for %s", video_id)
        task_id = self._tasks[video_id]["task_id"]
        self._progress_bar.start_task(task_id)
        self._tasks[video_id]["started"] = True

    def _update_task(
        self, video_id: str, downloaded_bytes: float, total_bytes: float
    ) -> None:
        """Update the progress task for a video ID"""
        logging.debug("Updating progress task for %s", video_id)
        task_id = self._tasks[video_id]["task_id"]
        self._progress_bar.update(
            task_id,
            completed=downloaded_bytes,
            total=total_bytes,
        )

    def _process_item(self, progress_info: ProgressLineDict) -> None:
        # Get current info
        logging.debug("Processing progress info")
        video_id = progress_info["video"]["id"]
        total_bytes = self._get_progress_info_total_bytes(progress_info)
        downloaded_bytes = self._get_progress_info_downloaded_bytes(progress_info)
        # Create / start / update task
        if not self._task_exists(video_id):
            self._create_task(video_id, progress_info["video"]["title"])
        if not self._task_started(video_id) and total_bytes is not None:
            self._start_task(video_id)
        if self._task_started(video_id):
            self._update_task(video_id, downloaded_bytes, total_bytes)
