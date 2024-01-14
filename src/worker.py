from abc import abstractmethod
from multiprocessing import JoinableQueue, Process
import sys
from typing import Any, Generic, Sequence, TypeVar

TaskValueT = TypeVar("TaskValueT")


class WorkerInterface(Generic[TaskValueT]):
    """Base class for `Worker`s and `WorkerGroup`s"""

    @abstractmethod
    def start(self) -> None:
        """Start the worker"""

    @abstractmethod
    def end_of_tasks(self) -> None:
        """Signal the end of the tasks to the worker"""

    @abstractmethod
    def close(self) -> None:
        """Signal to the worker to close its queue"""

    @abstractmethod
    def join(self) -> None:
        """Wait until all tasks are done"""


class Worker(
    Process,
    WorkerInterface[TaskValueT],
    Generic[TaskValueT],
):
    """Worker process with input and output queues"""

    # HACK: type keyword is python 3.12 only
    # type WorkerQueue = JoinableQueue[None, TaskValueT]
    # type OptionalWorkerQueue = None | WorkerQueue
    WorkerQueue = JoinableQueue[None | Any]
    OptionalWorkerQueue = None | WorkerQueue

    # --- Protected methods

    __input_queue: WorkerQueue
    __output_queue: OptionalWorkerQueue

    def _send_output(self, value: TaskValueT) -> None:
        """Send an item to the output queue if it exists, else do nothing"""
        if self.__output_queue is None:
            return
        self.__output_queue.put(value)

    @abstractmethod
    def _process_item(self, item: TaskValueT) -> Sequence[TaskValueT]:
        """Process an item from the queue and return results to the output queue."""

    # --- Init

    def __init__(
        self,
        input_queue: WorkerQueue,
        output_queue: OptionalWorkerQueue,
    ) -> None:
        super(Process, self).__init__(daemon=True)
        self.__input_queue = input_queue
        self.__output_queue = output_queue

    # --- Public methods

    def run(self) -> None:
        """Subprocess' main function"""

        while True:
            # Process the next item
            item: TaskValueT = self.__input_queue.get()
            results = []
            if item is not None:
                results.extend(self._process_item(item))
            for result in results:
                self._send_output(result)
            self.__input_queue.task_done()

            # Stop if requested to
            if item is None:
                break

        # Exit gracefuly
        sys.exit(0)

    def start(self) -> None:
        super(Process, self).start()

    def end_of_tasks(self) -> None:
        self.__input_queue.put(None)

    def close(self) -> None:
        self.__input_queue.close()

    def join(self) -> None:
        self.__input_queue.join()


class WorkerGroup(WorkerInterface[TaskValueT]):
    """Group containing workers"""

    # --- Protected methods

    __workers: tuple[Worker]

    # --- Init

    def __init__(self, *workers: Worker) -> None:
        self.__workers = workers

    # --- Public methods

    def start(self) -> None:
        """Start all the workers in the group"""
        for worker in self.__workers:
            worker.start()

    def end_of_tasks(self) -> None:
        for worker in self.__workers:
            worker.end_of_tasks()

    def close(self) -> None:
        # TODO won't work when workers share a queue AAAAAAA
        for worker in self.__workers:
            worker.close()

    def join(self) -> None:
        for worker in self.__workers:
            worker.join()
