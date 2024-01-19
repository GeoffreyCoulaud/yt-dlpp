import argparse
import sys
from itertools import repeat
from multiprocessing import JoinableQueue, cpu_count

from yt_dlp.options import create_parser as create_ydl_parser

from dedup_worker import DedupWorker
from src.download_worker import DownloadWorker
from src.info_worker import InfoWorker
from src.progress_worker import ProgressWorker
from worker import WorkerInterface, WorkerPool


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
    parser.add_argument(
        "--n-info-workers",
        type=int,
        default=cpu_count(),
        help="Number of info workers to use",
    )
    parser.add_argument(
        "--n-dl-workers",
        type=int,
        default=cpu_count(),
        help="Number of download workers to use",
    )
    args: _MainArgs
    args, ydl_args = parser.parse_known_args()

    # Parse the yt-dlp arguments
    ydl_parser = create_ydl_parser()
    options, cli_urls = ydl_parser.parse_args(args=ydl_args)
    if not isinstance(options, dict):
        raise ValueError("Options cannot be parsed to a dict")
    options["quiet"] = True

    # Create the queues
    generic_url_queue = JoinableQueue()
    video_url_queue = JoinableQueue()
    unique_video_url_queue = JoinableQueue()
    progress_queue = JoinableQueue()

    # Create the workers
    workers: tuple[WorkerInterface] = (
        WorkerPool.from_class(
            args.n_info_workers,
            InfoWorker,
            (options, generic_url_queue, video_url_queue),
        ),
        DedupWorker(
            video_url_queue,
            unique_video_url_queue,
        ),
        WorkerPool.from_class(
            args.n_dl_workers,
            DownloadWorker,
            (options, unique_video_url_queue, progress_queue),
        ),
        ProgressWorker(
            progress_queue,
        ),
    )

    # Start the workers
    for worker in workers:
        worker.start()

    # Send the initial URLs to the queue
    for cli_url in cli_urls:
        generic_url_queue.put(cli_url)

    # Wait for every step to finish, one after the other
    for worker in workers:
        worker.dismiss()
        worker_input_queue = worker.get_input_queue()
        worker_input_queue.close()
        worker_input_queue.join()

    # If all went well, all of our workers finished
    # The remaining ones will be killed at exit since they're daemon processes
    sys.exit(0)


if __name__ == "__main__":
    main()
