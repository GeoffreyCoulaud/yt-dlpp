import logging
import sys
from argparse import ArgumentParser, Namespace
from multiprocessing import JoinableQueue, cpu_count
from os import getenv
from typing import Optional, Sequence

from yt_dlpp.interceptors.interceptor import InputUrlsInterceptor
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


class YtdlppParserNamespace(Namespace):
    """Namespace for yt-dlpp parser args"""

    n_info_workers: int
    n_dl_workers: int


class YtdlppParser(ArgumentParser):
    """Parser for yt-dlpp"""

    def __init__(self) -> None:
        super().__init__(
            description="A wrapper to download content using yt-dlp in parallel",
            epilog="See `yt-dlp --help` for more CLI options",
            allow_abbrev=False,
        )
        self.add_argument(
            "--n-info-workers",
            type=int,
            default=cpu_count(),
            help="Number of info workers to use",
        )
        self.add_argument(
            "--n-dl-workers",
            type=int,
            default=cpu_count(),
            help="Number of download workers to use",
        )

    def parse_known_args(
        self,
        args: Optional[Sequence[str]] = None,
        namespace: Optional[Namespace] = None,
    ) -> tuple[YtdlppParserNamespace, list[str]]:
        return super().parse_known_args(args, namespace)


def main():
    """App entry point"""

    # Enable logging to be able to debug if needed
    _setup_logging()

    # Parse the main arguments
    logging.debug("Parsing yt-dlpp args")
    args, raw_ydtdlp_args = YtdlppParser().parse_known_args()

    # Intercept some yt-dlp args
    logging.debug("Intercepting yt-dlp arguments")
    input_urls_args, ytdlp_args = InputUrlsInterceptor().parse_known_args(
        raw_ydtdlp_args
    )

    # Get the input URLs
    urls = []
    if input_urls_args.urls:
        urls.extend(input_urls_args.urls)
    if input_urls_args.batch_file:
        logging.debug("Reading URLs from batch file: %s", input_urls_args.batch_file)
        try:
            with open(input_urls_args.batch_file, "r") as file:
                lines = file.readlines()
            urls.extend({url for line in lines if (url := line.strip())})
        except OSError as e:
            logging.error("Error reading batch file: %s", e)
    if len(urls) == 0:
        logging.error("No URLs to process")
        sys.exit(1)

    # Create the queues
    logging.debug("Creating queues")
    input_url_queue = JoinableQueue()
    video_url_queue = JoinableQueue()
    unique_video_url_queue = JoinableQueue()
    progress_queue = JoinableQueue()

    # Create the workers
    logging.debug("Creating workers")
    workers: tuple[WorkerInterface] = (
        WorkerPool.from_class(
            args.n_info_workers,
            InfoWorker,
            ytdlp_args,
            input_url_queue,
            video_url_queue,
        ),
        DedupWorker(
            video_url_queue,
            unique_video_url_queue,
        ),
        WorkerPool.from_class(
            args.n_dl_workers,
            DownloadWorker,
            ytdlp_args,
            unique_video_url_queue,
            progress_queue,
        ),
        ProgressWorker(
            progress_queue,
        ),
    )

    # Wrap everything in a try/except to handle KeyboardInterrupt gracefully
    try:

        # Start the workers
        logging.debug("Starting workers")
        for worker in workers:
            worker.start()

        # Send the initial URLs to the queue
        print("Getting video info...")
        logging.debug("Sending initial URLs to the queue")
        for url in urls:
            logging.debug("\t %s", url)
            input_url_queue.put(url)

        # Wait for every step to finish, one after the other
        for i, worker in enumerate(workers):
            kind = "WorkerPool" if isinstance(worker, WorkerPool) else "Worker"
            logging.debug("Waiting for %s %d to finish", kind, i)
            worker.dismiss()
            logging.debug("Dismissed %s %d", kind, i)
            worker_input_queue = worker.get_input_queue()
            worker_input_queue.close()
            worker_input_queue.join()
            logging.debug("%s %d finished", kind, i)
        logging.debug("All workers finished")

    except KeyboardInterrupt:
        print("Interrupted by user")

    # If all went well, all of our workers finished
    # The remaining ones will be killed at exit since they're daemon processes
    sys.exit(0)


if __name__ == "__main__":
    main()
