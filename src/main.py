import argparse
from itertools import repeat
import sys
from typing import Callable
from yt_dlp.options import create_parser as create_ydl_parser
from multiprocessing import JoinableQueue, cpu_count
from base_worker import BaseWorker
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

    # Create the queues and workers
    info_queue = JoinableQueue()
    dl_queue = JoinableQueue()
    progress_queue = JoinableQueue()

    # Start the workers
    n_info_workers = main_args.n_info_workers
    n_dl_workers = main_args.n_dl_workers
    workers: list[BaseWorker] = [
        *repeat(InfoWorker(options, info_queue, dl_queue), n_info_workers),
        *repeat(DownloadWorker(options, dl_queue, progress_queue), n_dl_workers),
        ProgressWorker(progress_queue),
    ]
    for worker in workers:
        worker.start()

    # Send the initial URLs to the info queue
    for cli_url in cli_urls:
        info_queue.put(cli_url)

    # Info workers won't get more tasks, inform them and wait until they finish
    for _ in range(n_info_workers):
        info_queue.put(None)
    info_queue.close()
    info_queue.join()

    # DL workers won't get more tasks, inform them and wait until they finish
    for _ in range(n_dl_workers):
        dl_queue.put(None)
    dl_queue.close()
    dl_queue.join()

    # Progress worker won't get more tasks, inform it and wait until it finishes
    progress_queue.put(None)
    progress_queue.close()
    progress_queue.join()

    # If all went well, all of our workers finished
    # The remaining ones will be killed at exit since they're daemon processes
    sys.exit(0)


if __name__ == "__main__":
    main()
