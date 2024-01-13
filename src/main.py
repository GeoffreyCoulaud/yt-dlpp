import argparse
from itertools import repeat
import sys
from typing import Callable
from yt_dlp.options import create_parser as create_ydl_parser
from multiprocessing import JoinableQueue, cpu_count
from worker import BaseWorker, TaskConsumer, WorkerGroup
from src.download_worker import DownloadWorker
from src.info_worker import InfoWorker
from src.progress_worker import ProgressWorker


class _MainArgs(argparse.Namespace):
    n_info_workers: int
    n_dl_workers: int


def main():
    """App entry point"""

    # Parse the main arguments
    parser = argparse.ArgumentParser(
        description="A wrapper to download content using yt-dlp in parallel",
        epilog="See `yt-dlp --help` for more CLI options",
    )
    parser.add_argument("--n-info-workers", type=int, default=cpu_count())
    parser.add_argument("--n-dl-workers", type=int, default=cpu_count())
    main_args: _MainArgs
    main_args, other_args = parser.parse_known_args()

    # Parse the yt-dlp arguments
    ydl_parser = create_ydl_parser()
    options, cli_urls = ydl_parser.parse_args(args=other_args)
    if not isinstance(options, dict):
        raise ValueError("Options cannot be parsed to a dict")

    # Create the queues
    info_queue = JoinableQueue()
    dl_queue = JoinableQueue()
    progress_queue = JoinableQueue()

    # Create the workers
    task_consumers: tuple[TaskConsumer] = (
        WorkerGroup(
            info_queue,
            dl_queue,
            *repeat(
                InfoWorker(options, None, None),
                main_args.n_info_workers,
            ),
        ),
        WorkerGroup(
            dl_queue,
            progress_queue,
            *repeat(
                DownloadWorker(options, None, None),
                main_args.n_dl_workers,
            ),
        ),
        ProgressWorker(progress_queue),
    )

    # Start the workers
    for task_consumer in task_consumers:
        task_consumer.start()

    # Send the initial URLs to the info queue
    for cli_url in cli_urls:
        info_queue.put(cli_url)

    # Wait for every step to finish, one after the other
    for task_consumer in task_consumers:
        task_consumer.close()
        task_consumer.join()

    # If all went well, all of our workers finished
    # The remaining ones will be killed at exit since they're daemon processes
    sys.exit(0)


if __name__ == "__main__":
    main()
