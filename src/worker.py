import sys
from abc import abstractmethod
from contextlib import redirect_stderr, redirect_stdout
from multiprocessing import JoinableQueue, Process
from os import devnull
from typing import Any, Generic, Sequence, TypeVar

TaskValueT = TypeVar("TaskValueT")


class WorkerInterface(Generic[TaskValueT]):
    """Base class for Worker and WorkerPool"""

    @abstractmethod
    def start(self) -> None:
        """Start the worker"""

    @abstractmethod
    def get_input_queue(self) -> JoinableQueue[None | TaskValueT]:
        """Get the worker's input queue"""

    @abstractmethod
    def dismiss(self) -> None:
        """Signal to the worker to exit"""


class Worker(
    Process,
    WorkerInterface[TaskValueT],
    Generic[TaskValueT],
):
    """Worker process with input and output queues"""

    # HACK: type keyword is python 3.12 only
    # type WorkerQueue = JoinableQueue[None, TaskValueT]
    # type OptionalWorkerQueue = None | WorkerQueue
    _WorkerQueue = JoinableQueue[None | Any]
    _OptionalWorkerQueue = None | _WorkerQueue

    # --- Protected methods

    input_queue: _WorkerQueue
    output_queue: _OptionalWorkerQueue

    def _send_output(self, value: TaskValueT) -> None:
        """Send an item to the output queue if it exists, else do nothing"""
        if self.output_queue is None:
            return
        self.output_queue.put(value)

    @abstractmethod
    def _process_item(self, item: TaskValueT) -> Sequence[TaskValueT]:
        """Process an item from the queue and return results to the output queue."""

    # --- Init

    def __init__(
        self,
        input_queue: _WorkerQueue,
        output_queue: _OptionalWorkerQueue,
    ) -> None:
        super(Process, self).__init__(daemon=True)
        self.input_queue = input_queue
        self.output_queue = output_queue

    # --- Public methods

    def run(self) -> None:
        """Subprocess' main function"""

        while True:
            # Process the next item
            item: TaskValueT = self.input_queue.get()
            results = []
            if item is not None:
                results.extend(self._process_item(item))
            for result in results:
                self._send_output(result)
            self.input_queue.task_done()

            # Stop if requested to
            if item is None:
                break

        # Exit gracefuly
        sys.exit(0)

    def start(self) -> None:
        super(Process, self).start()

    def dismiss(self) -> None:
        self.input_queue.put(None)

    def get_input_queue(self) -> JoinableQueue[None | TaskValueT]:
        return self.input_queue


class SilentWorker(Worker):
    """Worker that cannot print to stdout and stderr"""

    def run(self) -> None:
        with (
            open(devnull, "w") as shadow_realm,  # Yup.
            redirect_stdout(shadow_realm),
            redirect_stderr(shadow_realm),
        ):
            super().run()


class WorkerPool(WorkerInterface[TaskValueT]):
    """Pool of workers sharing an input queue"""

    # --- Protected methods

    __workers: tuple[Worker]

    # --- Init

    def __init__(self, *workers: Worker) -> None:
        """
        Intitialize a Pool\n
        It is critical that workers share their input queue.
        """
        assert len(workers) > 0, "Cannot create pool with no workers"
        assert (
            len({worker.get_input_queue() for worker in workers}) == 1
        ), "All Workers in a pool must have the same input queue"
        self.__workers = workers

    @classmethod
    def from_class(cls, n: int, klass: type[Worker], *args) -> "WorkerPool":
        """
        Create a worker pool containing n workers of the given class
        with all the same constructor args
        """
        workers = (klass(*args) for _ in n)
        return WorkerPool(*workers)

    # --- Public methods

    def start(self) -> None:
        for worker in self.__workers:
            worker.start()

    def dismiss(self) -> None:
        for worker in self.__workers:
            worker.dismiss()

    def get_input_queue(self) -> JoinableQueue[None | TaskValueT]:
        return self.__workers[0].get_input_queue()
