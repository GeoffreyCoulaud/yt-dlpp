from abc import abstractmethod
from multiprocessing import JoinableQueue, Process
import sys
from typing import Generic, Sequence, TypeVar

TaskValueT = TypeVar("TaskValueT")


# TODO better name aaaaaaaaa
class TaskConsumer(Generic[TaskValueT]):
    """Interface for objects that consume tasks with an input JoinableQueue"""

    @abstractmethod
    def put(self, task: None | TaskValueT) -> None:
        """Put a task in the input queue of the worker"""

    @abstractmethod
    def close(self) -> None:
        """Signal to the worker to end and close its queue"""

    @abstractmethod
    def join(self) -> None:
        """Join the worker after all tasks have been done"""


class BaseWorker(
    Process,
    TaskConsumer[TaskValueT],
    Generic[TaskValueT],
):
    """Worker process with input and output queues"""

    _input_queue: None | JoinableQueue[None | TaskValueT]
    _output_queue: None | JoinableQueue[None | TaskValueT]

    # --- Task consumer methods

    def put(self, task) -> None:
        self._input_queue.put(task)

    def close(self) -> None:
        self.put(None)
        self._input_queue.close()

    def join(self) -> None:
        self._input_queue.join()

    # --- Worker methods

    def __init__(
        self,
        input_queue: None | JoinableQueue[None | TaskValueT],
        output_queue: None | JoinableQueue[None | TaskValueT],
    ) -> None:
        super().__init__(daemon=True)
        self._input_queue = input_queue
        self._output_queue = output_queue

    def start(self) -> None:
        assert self._input_queue is not None, "Cannot run without an input queue"
        super().start()

    def run(self) -> None:
        """Subprocess' main function"""

        while True:
            # Break if there is no queue
            if not self._input_queue:
                break

            # Get input item
            item: None | TaskValueT = self._input_queue.get()

            # Process it
            if item is not None:
                results = self._process_item(item)
                if self._output_queue is not None:
                    for result in results:
                        self._output_queue.put(result)

            # Signal that we finished processing
            self._input_queue.task_done()

            # Stop if requested to
            if item is None:
                break

        # Exit gracefuly
        sys.exit(0)

    @abstractmethod
    def _process_item(self, item: TaskValueT) -> Sequence[TaskValueT]:
        """Process an item from the queue and return results to the output queue."""

    def set_input_queue(self, queue: None | JoinableQueue[None | TaskValueT]) -> None:
        """Replace the worker's input queue in place"""
        self._input_queue = queue

    def set_output_queue(self, queue: None | JoinableQueue[None | TaskValueT]) -> None:
        """Replace the worker's output queue in place"""
        self._output_queue = queue


class WorkerGroup(TaskConsumer[TaskValueT]):
    """Group containing workers"""

    _workers: tuple[BaseWorker]
    _input_queue: None | JoinableQueue[None | TaskValueT]
    _output_queue: None | JoinableQueue[None | TaskValueT]

    def __init__(
        self,
        input_queue: None | JoinableQueue[None | TaskValueT],
        output_queue: None | JoinableQueue[None | TaskValueT],
        *workers: BaseWorker
    ) -> None:
        self._input_queue = input_queue
        self._output_queue = output_queue
        self._workers = workers

        # Set the worker queues to the group's queues
        for worker in self._workers:
            worker.set_input_queue(input_queue)
            worker.set_output_queue(output_queue)

    # --- Task consumer methods

    def put(self, task: None | TaskValueT) -> None:
        self._input_queue.put(task)

    def close(self) -> None:
        for _ in self._workers:
            self.put(None)
        self._input_queue.close()

    def join(self) -> None:
        self._input_queue.join()

    # --- Worker group method

    def start(self) -> None:
        """Start all the workers in the group"""
        for worker in self._workers:
            worker.start()
