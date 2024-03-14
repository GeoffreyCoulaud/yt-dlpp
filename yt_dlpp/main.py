import argparse
import sys
from multiprocessing import JoinableQueue, cpu_count
from typing import Any

from yt_dlp import parse_options

from yt_dlpp.workers.dedup_worker import DedupWorker
from yt_dlpp.workers.download_worker import DownloadWorker
from yt_dlpp.workers.info_worker import InfoWorker
from yt_dlpp.workers.progress_worker import ProgressWorker
from yt_dlpp.workers.worker import WorkerInterface, WorkerPool


def _create_main_parser() -> argparse.ArgumentParser:
    """Create the main yt-dlpp parser"""

    parser = argparse.ArgumentParser(
        description="A wrapper to download content using yt-dlp in parallel",
        epilog="See `yt-dlp --help` for more CLI options",
        allow_abbrev=False,
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

    return parser


def _create_ydl_interceptor_parser() -> argparse.ArgumentParser:
    """
    Create a dummy yt-dlp parser to intercept some arguments

    In charge of neutralizing arguments that change the output and would prevent
    yt-dlpp from working as intended.

    Also catches the URLs positionned at the end of the args.
    """

    parser = argparse.ArgumentParser(allow_abbrev=False)

    # stdout format - We want to read from it programmatically
    parser.add_argument("--quiet", "-q")
    parser.add_argument("--no-quiet")
    parser.add_argument("--print", "-O")
    parser.add_argument("--verbose", "-v")
    parser.add_argument("--dump-pages")
    parser.add_argument("--print-traffic")

    # Progress options - We disable it to handle progress manually
    parser.add_argument("--newline")
    parser.add_argument("--no-progress")
    parser.add_argument("--progress")
    parser.add_argument("--console-title")
    parser.add_argument("--progress-template")

    # JSON dump - We set it manually
    parser.add_argument("--dump-json")
    parser.add_argument("--dump-single-json")

    # URLs at the end - We want to use those directly
    parser.add_argument("urls", nargs="+")

    return parser


def main():
    """App entry point"""

    # Parse the main arguments
    parser = _create_main_parser()
    args, raw_ydl_args = parser.parse_known_args()

    # Intercept some yt-dlp args
    interceptor_parser = _create_ydl_interceptor_parser()
    intercepted_ydl_args, kept_ydl_args = interceptor_parser.parse_known_args(
        raw_ydl_args
    )
    cli_urls = intercepted_ydl_args.urls

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
            kept_ydl_args,
            generic_url_queue,
            video_url_queue,
        ),
        DedupWorker(
            video_url_queue,
            unique_video_url_queue,
        ),
        WorkerPool.from_class(
            args.n_dl_workers,
            DownloadWorker,
            kept_ydl_args,
            unique_video_url_queue,
            progress_queue,
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
