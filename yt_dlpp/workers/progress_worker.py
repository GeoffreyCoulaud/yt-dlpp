import logging
from multiprocessing import JoinableQueue
from typing import Optional, TypedDict

from rich.progress import (
    BarColumn,
    Progress,
    TaskID,
    TaskProgressColumn,
    TextColumn,
    filesize,
)

from yt_dlpp.workers.download_worker import ProgressLineDict
from yt_dlpp.workers.worker import Worker


class _VideoTaskInfo(TypedDict):
    """Information about a video task"""

    task_id: TaskID
    started: bool


class _CustomFields(TypedDict):
    """Custom fields for progress tasks"""

    custom_total: str
    custom_speed: str
    custom_eta: str


class ProgressWorker(Worker):
    """Worker in charge of displaying progress info"""

    input_queue: JoinableQueue
    output_queue: None = None

    _progress_bar: Progress
    _tasks: dict[str, _VideoTaskInfo]
    _unknown_value = "?"

    def __init__(self, input_queue: JoinableQueue):
        super().__init__(input_queue, None)

    def run(self) -> None:
        self._tasks = {}
        columns = (
            TextColumn('"[progress.description]{task.description}"'),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("of"),
            TextColumn("{task.fields[custom_total]}", style="progress.filesize"),
            TextColumn("at"),
            TextColumn("{task.fields[custom_speed]}", style="progress.data.speed"),
            TextColumn("ETA"),
            TextColumn("{task.fields[custom_eta]}", style="progress.eta"),
        )
        with Progress(*columns) as self._progress_bar:
            super().run()

    def _get_estimated_total_bytes(
        self, progress_info: ProgressLineDict
    ) -> Optional[float]:
        try:
            return float(progress_info["progress"]["total_bytes_estimate"])
        except (ValueError, KeyError):
            return None

    def _get_real_total_bytes(self, progress_info: ProgressLineDict) -> Optional[float]:
        try:
            return float(progress_info["progress"]["total_bytes"])
        except (ValueError, KeyError):
            return None

    def _get_downloaded_bytes(self, progress_info: ProgressLineDict) -> float:
        try:
            return float(progress_info["progress"]["downloaded_bytes"])
        except (ValueError, KeyError):
            return 0

    def _get_speed(self, progress_info: ProgressLineDict) -> float:
        try:
            return float(progress_info["progress"]["speed"])
        except (ValueError, KeyError):
            return 0

    def _get_eta(self, progress_info: ProgressLineDict) -> Optional[float]:
        try:
            return float(progress_info["progress"]["eta"])
        except (ValueError, KeyError):
            return None

    def _format_custom_total(self, total_bytes: float, is_estimate: bool) -> str:
        # No total bytes available
        if total_bytes is None:
            return self._unknown_value
        # Pretty format total bytes
        prefix = "~ " if is_estimate else ""
        return prefix + filesize.decimal(int(total_bytes))

    def _format_custom_speed(self, speed: float) -> str:
        return filesize.decimal(int(speed)) + "/s"

    def _format_custom_eta(self, eta_seconds: Optional[float]) -> str:
        # No ETA available
        style = "progress.eta"
        if eta_seconds is None:
            return self._unknown_value
        # Pretty format ETA
        minutes, seconds = divmod(int(eta_seconds), 60)
        hours, minutes = divmod(minutes, 60)
        return (
            f"{hours:d}:{minutes:02d}:{seconds:02d}"
            if hours
            else f"{minutes:02d}:{seconds:02d}"
        )

    def _create_empty_custom_fields(self) -> _CustomFields:
        return {
            "custom_total": self._format_custom_total(None, True),
            "custom_speed": self._format_custom_speed(0),
            "custom_eta": self._format_custom_eta(None),
        }

    def _task_exists(self, video_id: str) -> bool:
        """Check if a progress task exists for a video ID"""
        return video_id in self._tasks

    def _create_task(self, video_id: str, title: str) -> None:
        """Create a new progress task for a video ID"""
        logging.debug("Creating progress task for %s", video_id)
        task_id = self._progress_bar.add_task(
            description=title, start=False, **self._create_empty_custom_fields()
        )
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
        self,
        video_id: str,
        downloaded_bytes: float,
        total_bytes: float,
        total_is_estimate: bool,
        speed_bytes_per_second: float,
        eta_seconds: float,
    ) -> None:
        """Update the progress task for a video ID"""
        task_id = self._tasks[video_id]["task_id"]
        fields = {
            "custom_total": self._format_custom_total(total_bytes, total_is_estimate),
            "custom_speed": self._format_custom_speed(speed_bytes_per_second),
            "custom_eta": self._format_custom_eta(eta_seconds),
        }
        self._progress_bar.update(
            task_id, completed=downloaded_bytes, total=total_bytes, **fields
        )

    def _process_item(self, progress_info: ProgressLineDict) -> None:
        # Get current info
        video_id = progress_info["video"]["id"]
        real_total_bytes = self._get_real_total_bytes(progress_info)
        estimated_total_bytes = self._get_estimated_total_bytes(progress_info)
        total_bytes = real_total_bytes or estimated_total_bytes
        downloaded_bytes = self._get_downloaded_bytes(progress_info)
        speed_bytes_per_second = self._get_speed(progress_info)
        eta_seconds = self._get_eta(progress_info)
        # Create / start / update task
        if not self._task_exists(video_id):
            self._create_task(video_id, progress_info["video"]["title"])
        if not self._task_started(video_id) and total_bytes is not None:
            self._start_task(video_id)
        if self._task_started(video_id):
            self._update_task(
                video_id,
                downloaded_bytes,
                total_bytes,
                real_total_bytes is None,
                speed_bytes_per_second,
                eta_seconds,
            )
