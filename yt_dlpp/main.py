import argparse
import logging
import sys
from multiprocessing import JoinableQueue, cpu_count
from os import getenv

from yt_dlpp.interceptors import UrlInterceptor
from yt_dlpp.workers.dedup_worker import DedupWorker
from yt_dlpp.workers.download_worker import DownloadWorker
from yt_dlpp.workers.info_worker import InfoWorker
from yt_dlpp.workers.progress_worker import ProgressWorker
from yt_dlpp.workers.worker import WorkerInterface, WorkerPool


def _setup_logging() -> None:
    """Setup the logging"""
    log_levels = logging.getLevelNamesMapping()
    log_level = log_levels[getenv("LOG_LEVEL", "ERROR")]
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - [%(processName)s - %(levelname)s] %(message)s",
    )


def _create_ytdlpp_parser() -> argparse.ArgumentParser:
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


def main():
    """App entry point"""

    # Enable logging to be able to debug if needed
    _setup_logging()

    # Parse the main arguments
    logging.debug("Parsing yt-dlpp args")
    parser = _create_ytdlpp_parser()
    args, raw_ydl_args = parser.parse_known_args()

    # Intercept some yt-dlp args
    logging.debug("Intercepting yt-dlp arguments")
    interceptor = UrlInterceptor()
    intercepted_args, other_args = interceptor.parse_known_args(raw_ydl_args)
    cli_urls = intercepted_args.urls

    # Create the queues
    logging.debug("Creating queues")
    generic_url_queue = JoinableQueue()
    video_url_queue = JoinableQueue()
    unique_video_url_queue = JoinableQueue()
    progress_queue = JoinableQueue()

    # Create the workers
    logging.debug("Creating workers")
    workers: tuple[WorkerInterface] = (
        WorkerPool.from_class(
            args.n_info_workers,
            InfoWorker,
            other_args,
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
            other_args,
            unique_video_url_queue,
            progress_queue,
        ),
        ProgressWorker(
            progress_queue,
        ),
    )

    # Start the workers
    logging.debug("Starting workers")
    for worker in workers:
        worker.start()

    # Send the initial URLs to the queue
    print("Getting video info...")
    logging.debug("Sending initial URLs to the queue")
    for cli_url in cli_urls:
        logging.debug(f"\t {cli_url}")
        generic_url_queue.put(cli_url)

    # Wait for every step to finish, one after the other
    for i, worker in enumerate(workers):
        logging.debug(f"Waiting for worker {i} to finish")
        worker.dismiss()
        logging.debug(f"\tDismissed worker {i}")
        worker_input_queue = worker.get_input_queue()
        worker_input_queue.close()
        worker_input_queue.join()
        logging.debug(f"\tWorker {i} finished")
    logging.debug("All workers finished")

    # If all went well, all of our workers finished
    # The remaining ones will be killed at exit since they're daemon processes
    sys.exit(0)


if __name__ == "__main__":
    main()
