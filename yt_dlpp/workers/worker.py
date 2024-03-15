import sys
from abc import abstractmethod
from multiprocessing import Process
from signal import SIGINT, signal
from typing import Any, Generic, TypeVar

# HACK: Type hints are bad, but it's not my fault.
# mutiprocessing queues don't support type hints, for some god-forsaken reason.
# See https://github.com/python/cpython/issues/99509

TaskInputValueT = TypeVar("TaskInputValueT")
TaskOutputValueT = TypeVar("TaskOutputValueT")


class WorkerInterface(Generic[TaskInputValueT, TaskOutputValueT]):
    """Base class for Worker and WorkerPool"""

    @abstractmethod
    def start(self) -> None:
        """Start the worker"""

    @abstractmethod
    def get_input_queue(self) -> Any:
        """Get the worker's input queue"""

    @abstractmethod
    def dismiss(self) -> None:
        """Signal to the worker to exit"""


class Worker(Process, WorkerInterface[TaskInputValueT, TaskOutputValueT]):
    """Worker process with input and output queues"""

    # --- Protected methods

    def _send_output(self, value: TaskOutputValueT) -> None:
        """Send an item to the output queue if it exists, else do nothing"""
        if self.output_queue is None:
            return
        self.output_queue.put(value)

    @abstractmethod
    def _process_item(self, item: TaskInputValueT) -> None:
        """Process an item and pass results to the output queue"""

    # --- Init

    def __init__(
        self,
        input_queue: Any,
        output_queue: None | Any,
    ) -> None:
        super(Process, self).__init__(daemon=True)
        self.input_queue = input_queue
        self.output_queue = output_queue

    # --- Public methods

    def run(self):
        """Subprocess' main function"""

        # Set up signal handling
        signal(SIGINT, lambda _signum, _frame: sys.exit(0))

        while True:
            # Process the next item
            item: TaskInputValueT = self.input_queue.get()
            if item is not None:
                self._process_item(item)
            self.input_queue.task_done()

            # Stop if requested to
            if item is None:
                break

        # Exit gracefuly
        sys.exit(0)

    def start(self):
        super(Process, self).start()

    def dismiss(self):
        self.input_queue.put(None)

    def get_input_queue(self):
        return self.input_queue


class WorkerPool(WorkerInterface[TaskInputValueT, TaskOutputValueT]):
    """Pool of workers sharing an input queue"""

    # --- Protected methods

    __workers: tuple[Worker[TaskInputValueT, TaskOutputValueT]]

    # --- Init

    def __init__(self, *workers: Worker):
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
        workers = (klass(*args) for _ in range(n))
        return WorkerPool(*workers)

    # --- Public methods

    def start(self):
        for worker in self.__workers:
            worker.start()

    def dismiss(self):
        for worker in self.__workers:
            worker.dismiss()

    def get_input_queue(self):
        return self.__workers[0].get_input_queue()
